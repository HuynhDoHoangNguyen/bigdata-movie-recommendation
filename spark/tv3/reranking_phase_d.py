"""Phase D: bounded validation selection, immutable lock, one test confirmation.

Selection uses one authorized temporary ALS fit on train_core only.
Confirmation/demo use the unchanged Phase B model fit on all sample train.
TV2 test was observed in B/C; Phase D test results are confirmatory.
"""

import argparse
import hashlib
import json
import time
from datetime import datetime, timezone

from pyspark import StorageLevel
from pyspark.ml.recommendation import ALSModel
from pyspark.sql import SparkSession, Window, functions as F

from config import (DEMO_USERS_PATH, EXPERIMENTS_PATH, FINAL_METRICS_PATH, MANIFEST_PATH,
                    MODEL_PATH, MOVIES_PATH, RECOMMENDATIONS_PATH, SAMPLE_USERS_PATH,
                    SEED, TEST_PATH, TOPK_METRICS_PATH, TRAIN_PATH, TUNING_MANIFEST_PATH,
                    VALIDATION_RATIO)
from evaluate import split_train_validation
from output_contract import check_factors, path_exists, read_manifest, require_new_outputs, write_manifest
from support_reranking import (K, METHODS, POOL_SIZE, choose_config, item_support,
                               rank_candidates, ranking_metrics, regression_checks,
                               verify_recommendations)
from topn import deterministic_user_sample
from train_als import fit_als

BASE = "hdfs://namenode:9000/project/movielens/output/tv3"
DIAGNOSTICS = f"{BASE}/diagnostics/reranking_phase_d"
SELECTION_MODEL = f"{DIAGNOSTICS}/selection_model"
FIT_ATTEMPT = f"{DIAGNOSTICS}/selection_fit_attempt"
FIT_REPORT = f"{DIAGNOSTICS}/selection_model_report"
CHECKS_REPORT = f"{DIAGNOSTICS}/regression_checks"
SELECTION_DATASET = "SAMPLE_VALIDATION_INDEPENDENT_OF_SELECTION_MODEL"
PARAMS = {"rank": 10, "maxIter": 8, "regParam": 0.05}
LOCK = f"{DIAGNOSTICS}/config_lock"
VALIDATION_REPORT = f"{DIAGNOSTICS}/validation_report"
TEST_REPORT = f"{DIAGNOSTICS}/test_report"
VERIFY_REPORT = f"{DIAGNOSTICS}/verification_report"
VALIDATION_USERS = f"{DIAGNOSTICS}/validation_users"
TEST_USERS = f"{DIAGNOSTICS}/test_users"
ATTEMPT = f"{DIAGNOSTICS}/confirmation_attempt"
VALIDATION_METRICS = f"{BASE}/metrics/reranking_validation"
TEST_METRICS = f"{BASE}/metrics/reranking_test"
DEMO = f"{BASE}/recommendations/topn_reranked"
PHASE_C = f"{BASE}/diagnostics/ranking_phase_c"
PROTECTED = (MODEL_PATH, FINAL_METRICS_PATH, TOPK_METRICS_PATH, RECOMMENDATIONS_PATH,
             EXPERIMENTS_PATH, MANIFEST_PATH, SAMPLE_USERS_PATH, DEMO_USERS_PATH,
             TUNING_MANIFEST_PATH, PHASE_C)


def now():
    return datetime.now(timezone.utc).isoformat()


def fingerprint(spark, paths=PROTECTED):
    records = []
    for path in paths:
        hpath = spark._jvm.org.apache.hadoop.fs.Path(path)
        fs = hpath.getFileSystem(spark._jsc.hadoopConfiguration())
        if not fs.exists(hpath):
            raise ValueError(f"Required protected artifact missing: {path}")
        files = fs.listFiles(hpath, True)
        while files.hasNext():
            item = files.next()
            records.append((item.getPath().toString(), item.getLen(), item.getModificationTime(),
                            str(fs.getFileChecksum(item.getPath()))))
    records.sort()
    return {"files": len(records), "sha256": hashlib.sha256(json.dumps(records).encode()).hexdigest()}


def digest(payload):
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def read_lock(spark):
    envelope = read_manifest(spark, LOCK)
    if envelope["sha256"] != digest(envelope["config"]):
        raise ValueError("Locked config digest differs")
    config = envelope["config"]
    if (config["selection_dataset"] != SELECTION_DATASET or config["seed"] != SEED
            or config["candidate_pool_size"] != POOL_SIZE or config["k"] != K
            or config["support_source"] != "SAMPLE_TRAIN_CORE_ONLY"
            or not config["validation_is_unseen_by_selection_model"]
            or config["selection_model_path"] != SELECTION_MODEL
            or config["confirmation_model_path"] != MODEL_PATH):
        raise ValueError("Unexpected lock protocol")
    return envelope


def context(spark, model_path=MODEL_PATH):
    cached = []

    def cache(frame):
        frame = frame.persist(StorageLevel.DISK_ONLY)
        cached.append(frame)
        return frame

    manifest = read_manifest(spark, MANIFEST_PATH)
    tuning = read_manifest(spark, TUNING_MANIFEST_PATH)
    if manifest["status"] != "SAMPLE_COMPLETE_WARNING" or manifest["model_scope"] != "SAMPLE":
        raise ValueError("Requires the protected Phase B SAMPLE model")
    sample_users = cache(spark.read.parquet(SAMPLE_USERS_PATH))
    all_train = spark.read.parquet(TRAIN_PATH)
    expected_users = deterministic_user_sample(all_train, 10000, SEED)
    if sample_users.count() != 10000 or sample_users.exceptAll(expected_users).count() or expected_users.exceptAll(sample_users).count():
        raise ValueError("Saved users differ from original deterministic Phase A sample")
    train = cache(all_train.join(F.broadcast(sample_users), "userId"))
    core, validation = split_train_validation(train, VALIDATION_RATIO, SEED)
    core, validation = cache(core), cache(validation)
    counts = {"sample_users": 10000, "sample_train_rows": train.count(),
              "train_core_rows": core.count(), "validation_rows": validation.count()}
    expected = tuning["validation_split"]
    if (counts["sample_train_rows"] != expected["tuning_rows"]
            or counts["sample_train_rows"] != manifest["sample"]["train_rows"]
            or counts["train_core_rows"] != 1123778 or counts["validation_rows"] != 119934):
        raise ValueError(f"Original Phase A sample/split did not reproduce: {counts}")
    pair_overlap = core.join(validation, ["userId", "movieId"]).count()
    partition_difference = core.unionByName(validation).exceptAll(train).count() + train.exceptAll(
        core.unionByName(validation)).count()
    if pair_overlap or partition_difference:
        raise ValueError("Train-core/validation leakage or partition mismatch")
    support = cache(item_support(core))
    support_stats = support.agg(F.min("train_support").alias("min"),
        F.percentile_approx("train_support", [0.5, 0.75, 0.9, 0.95], 10000).alias("percentiles"),
        F.max("train_support").alias("max"), F.sum("train_support").alias("sum"),
        F.count("*").alias("supported_items")).first().asDict()
    if support_stats["sum"] != counts["train_core_rows"]:
        raise ValueError("Support includes rows beyond train-core")
    quantiles = support_stats.pop("percentiles")
    support_stats.update(dict(zip(("median", "p75", "p90", "p95"), quantiles)))
    mean = float(core.agg(F.avg("rating")).first()[0])
    movies = cache(spark.read.parquet(MOVIES_PATH))
    model = ALSModel.load(model_path) if model_path is not None else None
    eligible_items = None
    if model is not None:
        item_ids = model.itemFactors.select(F.col("id").alias("movieId"))
        eligible_items = cache(item_ids.join(movies.select("movieId"), "movieId").join(
            support, "movieId", "left").fillna(0, ["train_support"]))
    # Zero-support items remain eligible for RAW/shrinkage; missing support is not a label count.
    # All support/global mean features use core at validation, test and demo time.
    leakage = {"feature_source": "SAMPLE_TRAIN_CORE_ONLY", "core_validation_overlap": pair_overlap,
        "partition_difference": partition_difference, "support_sum_matches_core": True,
        "test_read_during_selection": False, "feature_leakage_status": "PASS",
        "validation_rows_seen_by_phase_b_model": counts["validation_rows"],
        "validation_is_unseen_by_selection_model": model_path == SELECTION_MODEL,
        "selection_model_train_source": "SAMPLE_TRAIN_CORE_ONLY",
        "model_validation_independence": "PASS" if model_path == SELECTION_MODEL else "NOT_SELECTION_STAGE"}
    return {"spark": spark, "cache": cache, "cached": cached, "manifest": manifest, "model": model,
            "sample_users": sample_users, "train": train, "core": core, "validation": validation,
            "support": support, "support_stats": support_stats, "global_mean": mean,
            "movies": movies, "eligible_items": eligible_items, "counts": counts, "leakage": leakage}


def fit_selection(spark):
    """One authorized fit, durably guarded; never retried or substituted for Phase B."""
    checks = read_manifest(spark, CHECKS_REPORT)
    if checks["status"] != "PASS":
        raise ValueError("Regression checks must PASS before fitting")
    require_new_outputs(spark, [SELECTION_MODEL, FIT_ATTEMPT, FIT_REPORT, LOCK,
                               VALIDATION_METRICS, TEST_METRICS, DEMO])
    started, before = time.perf_counter(), fingerprint(spark)
    ctx = context(spark, model_path=None)
    try:
        if ctx["counts"]["train_core_rows"] != 1123778 or ctx["leakage"]["core_validation_overlap"]:
            raise ValueError("Selection training source does not reproduce original train_core")
        write_manifest(spark, {"status": "FIT_STARTED", "created_at_utc": now(), "parameters": PARAMS,
            "seed": SEED, "fit_source": "SAMPLE_TRAIN_CORE_ONLY", "counts": ctx["counts"],
            "protected_fingerprint": before}, FIT_ATTEMPT)
        print("Fitting exactly ONE temporary train_core ALS model (1,123,778 rows)", flush=True)
        model, fit_seconds = fit_als(ctx["core"], PARAMS, SEED)
        model.write().save(SELECTION_MODEL)
        del model
        model = ALSModel.load(SELECTION_MODEL)
        factors = check_factors(model)
        for factor_frame, expected, name in (
                (model.userFactors.select(F.col("id").alias("userId")), ctx["core"].select("userId").distinct(), "users"),
                (model.itemFactors.select(F.col("id").alias("movieId")), ctx["core"].select("movieId").distinct(), "items")):
            if factor_frame.exceptAll(expected).count() or expected.exceptAll(factor_frame).count():
                raise ValueError(f"Temporary {name} factors differ from train_core IDs")
        after = fingerprint(spark)
        if before != after:
            raise ValueError("Protected outputs changed during temporary fit")
        report = {"status": "PASS", "model_scope": "TEMPORARY_SELECTION_ONLY", "model_path": SELECTION_MODEL,
            "parameters": PARAMS, "seed": SEED, "implicitPrefs": False, "coldStartStrategy": "drop",
            "fit_calls": 1, "fit_source": "SAMPLE_TRAIN_CORE_ONLY", "counts": ctx["counts"],
            "validation_rows_in_fit": 0, "core_validation_overlap": 0, "factors": factors,
            "fit_time_sec": fit_seconds, "runtime_sec": time.perf_counter() - started,
            "protected_artifacts": {"before": before, "after": after, "unchanged": True},
            "selection_model_fingerprint": fingerprint(spark, (SELECTION_MODEL,)), "generated_at_utc": now()}
        write_manifest(spark, report, FIT_REPORT)
        print(json.dumps(report, indent=2), flush=True)
    finally:
        for frame in reversed(ctx["cached"]):
            frame.unpersist()


def evaluation_users(ctx, labels):
    relevant = ctx["cache"](labels.where("rating >= 4.0").select("userId", "movieId").distinct())
    users = ctx["cache"](deterministic_user_sample(relevant, 500, SEED))
    if users.count() != 500 or users.join(ctx["model"].userFactors.select(
            F.col("id").alias("userId")), "userId", "left_anti").count():
        raise ValueError("Evaluation user factors/count mismatch")
    return users, ctx["cache"](relevant.join(F.broadcast(users), "userId"))


def pools(ctx, users, history):
    history = ctx["cache"](history.join(F.broadcast(users), "userId").select("userId", "movieId").distinct())
    print(f"Bounded candidate pool: {users.count()} users x {POOL_SIZE} items", flush=True)
    raw = ctx["model"].recommendForUserSubset(users, POOL_SIZE).select(
        "userId", F.posexplode("recommendations").alias("_pos", "_rec")).select(
        "userId", (F.col("_pos") + 1).alias("raw_rank"),
        F.col("_rec.movieId").cast("int").alias("movieId"),
        F.col("_rec.rating").cast("double").alias("prediction"))
    unseen = ctx["cache"](raw.join(ctx["eligible_items"], "movieId").join(
        history, ["userId", "movieId"], "left_anti").join(F.broadcast(ctx["movies"]), "movieId"))
    popular = ctx["eligible_items"].orderBy(F.desc("train_support"), "movieId").limit(100)
    # For baseline use Phase C's 100-item Top-10 popularity budget, with core-only counts.
    popular_input = users.crossJoin(F.broadcast(popular)).join(history, ["userId", "movieId"], "left_anti")
    # Detach ALS factor lineage before transform; at most 500*100 distributed rows.
    detached = ctx["spark"].createDataFrame(popular_input.select("userId", "movieId", "train_support").rdd,
                                            "userId int,movieId int,train_support long")
    popularity = ctx["cache"](ctx["model"].transform(detached).withColumn("raw_rank", F.lit(0)).join(
        F.broadcast(ctx["movies"]), "movieId"))
    return unseen, popularity, history


def evaluate(ctx, pool, popular, history, relevant, users, configs):
    rows = []
    for config in configs:
        print(f"Ranking {config['method']} parameter={config['parameter']}", flush=True)
        source = popular if config["method"] == "POPULARITY" else pool
        recs = rank_candidates(source, config["method"], config["parameter"], ctx["global_mean"]).persist(StorageLevel.DISK_ONLY)
        try:
            audit = verify_recommendations(recs, history, ctx["movies"], users)
            metrics = ranking_metrics(recs, relevant, users)
            rows.append({**config, **metrics, "contract_verification": "PASS", "audit": audit})
        finally:
            recs.unpersist()
    return rows


def write_metrics(spark, rows, path):
    # Uniform scalar schema avoids nested dict inference and permits independent Parquet readers.
    flat = [{name: row[name] for name in ("method", "parameter", "k", "users", "hits", "precision",
        "recall", "hit_rate", "recommendations", "users_with_10", "mean_item_support")}
            for row in rows]
    spark.createDataFrame(flat).coalesce(1).write.mode("errorifexists").parquet(path)


def validate(spark):
    checks = read_manifest(spark, CHECKS_REPORT)
    fit_report = read_manifest(spark, FIT_REPORT)
    if (checks["status"] != "PASS" or fit_report["status"] != "PASS"
            or fit_report["fit_calls"] != 1 or fit_report["parameters"] != PARAMS
            or fit_report["validation_rows_in_fit"] != 0
            or fit_report["selection_model_fingerprint"] != fingerprint(spark, (SELECTION_MODEL,))):
        raise ValueError("Independent temporary selection model checks failed")
    require_new_outputs(spark, [LOCK, VALIDATION_REPORT, VALIDATION_USERS, VALIDATION_METRICS, TEST_METRICS, DEMO])
    started, before = time.perf_counter(), fingerprint(spark)
    ctx = context(spark, SELECTION_MODEL)
    try:
        users, relevant = evaluation_users(ctx, ctx["validation"])
        pool, popular, history = pools(ctx, users, ctx["core"])
        results = evaluate(ctx, pool, popular, history, relevant, users, METHODS)
        selected = choose_config(results)
        raw = next(row for row in results if row["method"] == "RAW_ALS")
        # A concrete, prespecified demo gate; small validation-only changes are not called clear gains.
        clear_gain = (selected["recall"] - raw["recall"] >= 0.01
                      and selected["precision"] > raw["precision"]
                      and selected["users_with_10"] >= raw["users_with_10"])
        config = {"method": selected["method"], "parameter": selected["parameter"],
            "selection_dataset": SELECTION_DATASET, "selection_metric": "Recall@10",
            "tie_break": ["Precision@10", "HitRate@10", "full_user_coverage", "recommendation_count", "simplicity", "parameter"],
            "support_source": "SAMPLE_TRAIN_CORE_ONLY", "candidate_pool_size": POOL_SIZE,
            "raw_als_candidate_pool_size": 100, "popularity_candidate_pool_size": 100,
            "seed": SEED, "k": K, "global_train_core_mean": ctx["global_mean"],
            "validation_is_unseen_by_selection_model": True,
            "selection_model_path": SELECTION_MODEL, "confirmation_model_path": MODEL_PATH,
            "selection_model_fingerprint": fit_report["selection_model_fingerprint"],
            "clear_validation_gain": clear_gain, "clear_gain_rule": "recall gain >= 0.01; precision improves; full-user coverage not lower",
            "model_path": MODEL_PATH, "protected_fingerprint": before, "locked_at_utc": now()}
        after = fingerprint(spark)
        if before != after:
            raise ValueError("Protected artifacts changed during validation")
        report = {"status": "VALIDATION_SELECTED_INDEPENDENT", "generated_at_utc": now(),
            "scope": "500_DETERMINISTIC_SAMPLE_VALIDATION_USERS", "counts": ctx["counts"],
            "selection_dataset": SELECTION_DATASET, "leakage_audit": ctx["leakage"],
            "support_distribution": ctx["support_stats"], "global_train_core_mean": ctx["global_mean"],
            "support_percentiles": "percentile_approx accuracy=10000 over supported train-core items",
            "results": results, "selected_config": config, "regression_checks": checks,
            "protected_artifacts": {"before": before, "after": after, "unchanged": True},
            "runtime_sec": time.perf_counter() - started,
            "hybrid": "RUN_3_LAMBDAS_WITHIN_SAME_BOUNDED_ALS_POOL",
            "temporary_selection_model": fit_report,
            "limitation": "Selection on 500 sample validation users; confirmation transfers the locked config to the unchanged Phase B model."}
        write_metrics(spark, results, VALIDATION_METRICS)
        users.coalesce(1).write.mode("errorifexists").parquet(VALIDATION_USERS)
        write_manifest(spark, report, VALIDATION_REPORT)
        # Lock written last. Confirmation requires a complete selection report and digest.
        write_manifest(spark, {"config": config, "sha256": digest(config)}, LOCK)
        print(json.dumps(report, indent=2), flush=True)
    finally:
        for frame in reversed(ctx["cached"]):
            frame.unpersist()


def confirm(spark):
    require_new_outputs(spark, [ATTEMPT, TEST_REPORT, TEST_METRICS, TEST_USERS, DEMO])
    lock = read_lock(spark)
    validation_report = read_manifest(spark, VALIDATION_REPORT)
    if lock["config"] != validation_report["selected_config"]:
        raise ValueError("Selection report differs from immutable lock")
    before = fingerprint(spark)
    if before != lock["config"]["protected_fingerprint"]:
        raise ValueError("Protected artifacts changed since validation lock")
    if fingerprint(spark, (SELECTION_MODEL,)) != lock["config"]["selection_model_fingerprint"]:
        raise ValueError("Temporary model changed after selection")
    # Durable guard BEFORE the first test read; retries require an explicit separate recovery decision.
    write_manifest(spark, {"status": "CONFIRMATION_STARTED", "started_at_utc": now(),
                           "lock_sha256": lock["sha256"]}, ATTEMPT)
    started, ctx = time.perf_counter(), context(spark)
    try:
        if not abs(ctx["global_mean"] - lock["config"]["global_train_core_mean"]) < 1e-12:
            raise ValueError("Train-core mean differs from locked selection")
        # This is the ONLY stage reading TV2 test labels; no parameter search occurs here.
        test = ctx["cache"](spark.read.parquet(TEST_PATH).join(F.broadcast(ctx["sample_users"]), "userId"))
        if test.count() != ctx["manifest"]["sample"]["test_rows"] or ctx["train"].join(test, ["userId", "movieId"]).count():
            raise ValueError("Sample test count or train/test leakage check failed")
        users, relevant = evaluation_users(ctx, test)
        pool, popular, history = pools(ctx, users, ctx["train"])
        selected = {name: lock["config"][name] for name in ("method", "parameter")}
        configs = [{"method": "RAW_ALS", "parameter": 0.0}, selected,
                   {"method": "POPULARITY", "parameter": 0.0}]
        # If RAW_ALS won selection, evaluate that identical config once and copy its result for presentation.
        unique = list({(c["method"], c["parameter"]): c for c in configs}.values())
        measured = evaluate(ctx, pool, popular, history, relevant, users, unique)
        selected_result = next(row for row in measured if row["method"] == selected["method"]
                               and row["parameter"] == selected["parameter"])
        results = [{**row, "role": "RAW_ALS" if row["method"] == "RAW_ALS" else "POPULARITY"}
                   for row in measured if row["method"] in ("RAW_ALS", "POPULARITY")]
        results.insert(1, {**selected_result, "role": "RERANKED_ALS"})
        demo_status = {"status": "NOT_EXPORTED_NO_CLEAR_VALIDATION_GAIN", "path": DEMO}
        # Export policy fixed by validation; do not change it after viewing confirmation metrics.
        if lock["config"]["clear_validation_gain"]:
            demo_users = spark.read.parquet(DEMO_USERS_PATH)
            expected_demo = deterministic_user_sample(ctx["sample_users"], 20, SEED)
            if demo_users.count() != 20 or demo_users.exceptAll(expected_demo).count() or expected_demo.exceptAll(demo_users).count():
                raise ValueError("Demo users differ from Phase B deterministic protocol")
            demo_pool, _popular, demo_history = pools(ctx, demo_users, ctx["train"])
            recs = rank_candidates(demo_pool, selected["method"], selected["parameter"], ctx["global_mean"]).persist(StorageLevel.DISK_ONLY)
            try:
                audit = verify_recommendations(recs, demo_history, ctx["movies"], demo_users)
                if audit["users"] != 20 or audit["rows"] != 20 * K:
                    demo_status = {"status": "NOT_EXPORTED_INSUFFICIENT_DEMO_COVERAGE", "path": DEMO,
                                   "candidate_audit": audit, "expected_users": 20, "expected_rows": 20 * K}
                else:
                    recs.coalesce(1).write.mode("errorifexists").parquet(DEMO)
                    reload = spark.read.parquet(DEMO)
                    if reload.exceptAll(recs).count() or recs.exceptAll(reload).count():
                        raise ValueError("Reranked demo read-back mismatch")
                    verify_recommendations(reload, demo_history, ctx["movies"], demo_users, require_full=True)
                    demo_status = {"status": "VERIFIED", "path": DEMO, **audit}
            finally:
                recs.unpersist()
        after = fingerprint(spark)
        if before != after or read_lock(spark) != lock:
            raise ValueError("Protected artifacts or locked config changed")
        write_metrics(spark, results, TEST_METRICS)
        users.coalesce(1).write.mode("errorifexists").parquet(TEST_USERS)
        report = {"status": "CONFIRMATORY_COMPLETE_WARNING", "generated_at_utc": now(),
            "locked_config": lock, "results": results, "demo": demo_status,
            "support_source": "SAMPLE_TRAIN_CORE_ONLY", "sample_test_rows": test.count(),
            "sample_train_test_overlap": 0, "lock_unchanged": True,
            "protected_artifacts": {"before": before, "after": after, "unchanged": True},
            "runtime_sec": time.perf_counter() - started,
            "confirmation_model_path": MODEL_PATH,
            "limitation": "TV2 test was observed in Phases B/C. This is confirmation, not an untouched holdout; selection used an independent temporary train_core model."}
        write_manifest(spark, report, TEST_REPORT)
        print(json.dumps(report, indent=2), flush=True)
    finally:
        for frame in reversed(ctx["cached"]):
            frame.unpersist()


def verify(spark):
    """Read-back verifier; never repeat test evaluation or tune configurations."""
    require_new_outputs(spark, [VERIFY_REPORT])
    lock = read_lock(spark)
    selection = read_manifest(spark, VALIDATION_REPORT)
    confirmation = read_manifest(spark, TEST_REPORT)
    if (lock["config"] != selection["selected_config"] or lock != confirmation["locked_config"]
            or choose_config(selection["results"])["method"] != lock["config"]["method"]
            or choose_config(selection["results"])["parameter"] != lock["config"]["parameter"]):
        raise ValueError("Selection/lock/confirmation mismatch")
    if read_manifest(spark, ATTEMPT)["lock_sha256"] != lock["sha256"]:
        raise ValueError("Confirmation attempt not tied to lock")
    for path, rows in ((VALIDATION_METRICS, selection["results"]), (TEST_METRICS, confirmation["results"])):
        saved = spark.read.parquet(path).take(15)
        if len(saved) != len(rows):
            raise ValueError(f"Metric row count mismatch: {path}")
        expected = sorted(rows, key=lambda row: (row["method"], row["parameter"]))
        actual = sorted([row.asDict() for row in saved], key=lambda row: (row["method"], row["parameter"]))
        for left, right in zip(actual, expected):
            if any(left[key] != right[key] for key in left):
                raise ValueError(f"Metric read-back mismatch: {path}")
    for path in (VALIDATION_USERS, TEST_USERS):
        users = spark.read.parquet(path)
        if users.count() != 500 or users.select("userId").distinct().count() != 500:
            raise ValueError("Saved evaluation user count mismatch")
    if fingerprint(spark) != lock["config"]["protected_fingerprint"]:
        raise ValueError("Original outputs were modified")
    if fingerprint(spark, (SELECTION_MODEL,)) != lock["config"]["selection_model_fingerprint"]:
        raise ValueError("Temporary model changed after lock")
    fit_report = read_manifest(spark, FIT_REPORT)
    if fit_report["validation_rows_in_fit"] != 0 or fit_report["fit_calls"] != 1:
        raise ValueError("Selection model independence/provenance failed")
    if confirmation["demo"]["status"] == "VERIFIED":
        ctx = context(spark)
        try:
            demo = spark.read.parquet(DEMO)
            users = spark.read.parquet(DEMO_USERS_PATH)
            history = ctx["train"].join(F.broadcast(users), "userId").select("userId", "movieId").distinct()
            verify_recommendations(demo, history, ctx["movies"], users, require_full=True)
            annotated = demo.alias("r").join(ctx["support"].alias("s"), "movieId", "left")
            if annotated.where(~F.col("r.train_support").eqNullSafe(F.coalesce(F.col("s.train_support"), F.lit(0)))).count():
                raise ValueError("Demo support differs from train-core counts")
            config = lock["config"]
            if config["method"] == "HARD_GATE" and demo.where(F.col("train_support") < config["parameter"]).count():
                raise ValueError("Demo violates locked support gate")
            if config["method"] == "SHRINKAGE":
                expected = ctx["global_mean"] + F.col("train_support") / (
                    F.col("train_support") + config["parameter"]) * (F.col("prediction") - ctx["global_mean"])
                if demo.where(F.abs(F.col("adjusted_score") - expected) > 1e-10).count():
                    raise ValueError("Demo adjusted score differs from locked shrinkage")
        finally:
            for frame in reversed(ctx["cached"]):
                frame.unpersist()
    elif path_exists(spark, DEMO):
        raise ValueError("Unexpected demo output despite non-export policy")
    result = {"verification_status": "PASS", "status": "PASS_CONFIRMATORY_TEST_LIMITATION",
              "selection_model_independence": "PASS", "selection_model_fit_calls": 1,
              "lock_sha256": lock["sha256"], "protected_outputs_unchanged": True,
              "generated_at_utc": now(), "test_evaluation_repeated": False}
    write_manifest(spark, result, VERIFY_REPORT)
    print(json.dumps(result, indent=2), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", required=True, choices=("checks", "fit-selection", "validation", "confirm", "verify"))
    args = parser.parse_args()
    spark = SparkSession.builder.appName(f"TV3RerankingPhaseD_{args.stage}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        if args.stage == "checks":
            require_new_outputs(spark, [DIAGNOSTICS, VALIDATION_METRICS, TEST_METRICS, DEMO])
            checks = regression_checks(spark)
            write_manifest(spark, checks, CHECKS_REPORT)
            print(json.dumps(checks, indent=2), flush=True)
        elif args.stage == "fit-selection":
            fit_selection(spark)
        elif args.stage == "validation":
            validate(spark)
        elif args.stage == "confirm":
            confirm(spark)
        else:
            verify(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
