"""Independent read-only verification of the SAMPLE handoff contract."""

import math

from pyspark import StorageLevel
from pyspark.ml.recommendation import ALSModel
from pyspark.sql import functions as F

from config import (BEST_SAMPLE_PARAMS, DEMO_USERS_PATH, EXPERIMENTS_PATH,
                    FINAL_METRICS_PATH, MODEL_PATH, MOVIES_PATH, RECOMMENDATIONS_PATH,
                    SAMPLE_USERS_PATH, SEED, TEST_PATH, TOPK_METRICS_PATH,
                    TRAIN_PATH, TUNING_MANIFEST_PATH)
from evaluate import evaluate_ratings
from output_contract import (check_factors, check_recommendations, cold_start_stats,
                             read_manifest, validate_output_path)
from topn import deterministic_user_sample, evaluate_top_k


def verify_sample(spark, manifest):
    expected_scopes = {"status": "SAMPLE_COMPLETE_WARNING", "model_scope": "SAMPLE",
                       "evaluation_scope": "SAMPLE", "recommendation_scope": "SAMPLE_DEMO"}
    for key, value in expected_scopes.items():
        if manifest.get(key) != value:
            raise ValueError(f"Incorrect {key}: {manifest.get(key)}")
    expected_outputs = {"model": MODEL_PATH, "final_metrics": FINAL_METRICS_PATH,
                        "sample_users": SAMPLE_USERS_PATH, "demo_users": DEMO_USERS_PATH,
                        "top_k_metrics": TOPK_METRICS_PATH, "recommendations": RECOMMENDATIONS_PATH}
    for name, path in expected_outputs.items():
        validate_output_path(manifest["outputs"][name])
        if manifest["outputs"][name] != path:
            raise ValueError(f"Unexpected {name} path")
    if (manifest["model"]["path"] != MODEL_PATH or manifest["sample"]["users_path"] != SAMPLE_USERS_PATH
            or manifest["recommendations"]["path"] != RECOMMENDATIONS_PATH
            or manifest["recommendations"]["demo_users_path"] != DEMO_USERS_PATH
            or manifest["tuning_evidence"]["manifest_path"] != TUNING_MANIFEST_PATH
            or manifest["tuning_evidence"]["experiments_path"] != EXPERIMENTS_PATH):
        raise ValueError("Nested manifest paths differ from output contract")
    tuning = read_manifest(spark, TUNING_MANIFEST_PATH)
    if tuning != manifest["tuning_evidence"]["manifest"] or tuning["status"] != "TUNING_COMPLETE":
        raise ValueError("Tuning evidence changed")
    experiments = [row.asDict() for row in spark.read.parquet(EXPERIMENTS_PATH).take(4)]
    if len(experiments) != 3 or sorted(experiments, key=lambda r: r["experiment_id"]) != sorted(
        tuning["experiments"], key=lambda r: r["experiment_id"]
    ):
        raise ValueError("Persisted A/B/C evidence differs from preserved tuning manifest")
    users = spark.read.parquet(SAMPLE_USERS_PATH)
    full_train = spark.read.parquet(TRAIN_PATH)
    expected_users = deterministic_user_sample(full_train, 10000, SEED)
    if (users.count() != 10000 or users.distinct().count() != 10000
            or users.join(expected_users, "userId", "left_anti").count()
            or expected_users.join(users, "userId", "left_anti").count()):
        raise ValueError("Persisted sample users do not match deterministic tuning users")
    train = full_train.join(F.broadcast(users), "userId", "inner").persist(StorageLevel.DISK_ONLY)
    test = spark.read.parquet(TEST_PATH).join(F.broadcast(users), "userId", "inner").persist(StorageLevel.DISK_ONLY)
    try:
        train_rows, test_rows = train.count(), test.count()
        if (train_rows != manifest["sample"]["train_rows"] or test_rows != manifest["sample"]["test_rows"]
                or train_rows != tuning["validation_split"]["tuning_rows"]):
            raise ValueError("Sample row counts differ from manifest/tuning")
        model = ALSModel.load(MODEL_PATH)
        factors = check_factors(model)
        # ALSModel persists rank/prediction settings, not estimator maxIter/regParam/seed.
        if (model.rank != BEST_SAMPLE_PARAMS["rank"] or manifest["parameters"] != BEST_SAMPLE_PARAMS
                or manifest["seed"] != SEED or model.getColdStartStrategy() != "drop"):
            raise ValueError("Model params/seed/cold-start policy differ")
        if factors != manifest["model"]["factors"]:
            raise ValueError("Factor counts/quality differ from manifest")
        for column, factors_frame in (("userId", model.userFactors), ("movieId", model.itemFactors)):
            known = factors_frame.select(F.col("id").alias(column))
            ids = train.select(column).distinct()
            if ids.join(known, column, "left_anti").count() or known.join(ids, column, "left_anti").count():
                raise ValueError(f"Model {column} factors do not match sampled train")
        metric_rows = spark.read.parquet(FINAL_METRICS_PATH).take(2)
        if len(metric_rows) != 1:
            raise ValueError("Expected exactly one final metric row")
        metrics = metric_rows[0].asDict()
        if any(metrics[name] != value for name, value in BEST_SAMPLE_PARAMS.items()) or metrics["seed"] != SEED:
            raise ValueError("Final metric parameter provenance differs")
        if metrics != manifest["evaluation"] or metrics["evaluation_scope"] != "SAMPLE":
            raise ValueError("Final metrics differ from manifest or scope")
        if (metrics["sample_train_rows"] != train_rows or metrics["sample_test_rows"] != test_rows
                or metrics["evaluation_rows"] != test_rows or metrics["evaluation_dataset"] != "TV2 test deterministic user sample"
                or metrics["valid_prediction_rows"] <= 0 or metrics["dropped_rows"] < 0
                or metrics["valid_prediction_rows"] + metrics["dropped_rows"] != test_rows):
            raise ValueError("Invalid prediction coverage/sample counts")
        for name in ("rmse", "mae", "training_time_sec", "evaluation_time_sec"):
            if not math.isfinite(metrics[name]) or metrics[name] < 0:
                raise ValueError(f"Invalid metric {name}")
        if not math.isclose(metrics["dropped_percentage"], 100.0 * metrics["dropped_rows"] / test_rows):
            raise ValueError("Invalid dropped percentage")
        cold_start = cold_start_stats(model, test)
        if cold_start != manifest["cold_start"] or cold_start["affected_test_rows"] != metrics["dropped_rows"]:
            raise ValueError("Cold-start statistics differ")
        # Re-evaluate the loaded model on actual TV2 sample test; no fit/tuning.
        measured = evaluate_ratings(model, test, test_rows)
        for name in ("rmse", "mae"):
            if not math.isclose(measured[name], metrics[name], rel_tol=1e-8, abs_tol=1e-10):
                raise ValueError(f"Read-back {name} mismatch")
        if measured["valid_prediction_rows"] != metrics["valid_prediction_rows"]:
            raise ValueError("Read-back prediction count mismatch")
        demo = spark.read.parquet(DEMO_USERS_PATH)
        ids = [r.userId for r in demo.orderBy("userId").limit(21).collect()]
        if ids != manifest["recommendations"]["demo_user_ids"] or demo.count() != len(ids):
            raise ValueError("Demo users differ from manifest")
        if demo.join(users, "userId", "left_anti").count():
            raise ValueError("Demo user outside model sample")
        rec = manifest["recommendations"]
        if rec["scope"] != "SAMPLE_DEMO" or rec["top_n"] != 10:
            raise ValueError("Invalid recommendation scope/Top-N")
        checks = check_recommendations(spark.read.parquet(RECOMMENDATIONS_PATH), train,
                                      spark.read.parquet(MOVIES_PATH), demo, 10)
        if checks != rec["checks"] or checks["rows"] != rec["rows"] or checks["users"] != rec["actual_users"]:
            raise ValueError("Recommendation counts/checks differ from manifest")
        top_k = manifest["top_k"]
        top_k_audit = None
        if top_k["status"] == "PASS":
            top_rows = spark.read.parquet(TOPK_METRICS_PATH).take(2)
            if len(top_rows) != 1 or top_rows[0].asDict() != top_k:
                raise ValueError("Top-K metrics differ from manifest")
            if (top_k["evaluation_scope"] != "SAMPLE" or top_k["k"] != 10
                    or top_k["relevance_threshold"] != 4.0 or not 0 < top_k["evaluated_users"] <= 500):
                raise ValueError("Invalid Top-K scope")
            for name in ("precision_at_k", "recall_at_k"):
                if not math.isfinite(top_k[name]) or not 0 <= top_k[name] <= 1:
                    raise ValueError(f"Invalid {name}")
            top_k_audit = evaluate_top_k(model, train, test, spark.read.parquet(MOVIES_PATH),
                                       10, 4.0, 500, 10, SEED)
            for name in ("precision_at_k", "recall_at_k", "evaluated_users", "selected_users"):
                if not math.isclose(top_k_audit[name], top_k[name], rel_tol=1e-8, abs_tol=1e-10):
                    raise ValueError(f"Read-back Top-K {name} mismatch")
        elif top_k["status"] != "NOT_RUN_WITH_REASON" or not top_k.get("reason"):
            raise ValueError("Top-K omitted without reason")
        return {"status": "WARNING", "verification_status": "PASS", "errors": [],
                "warnings": [manifest["limitation"]], "manifest_status": manifest["status"],
                "model_scope": "SAMPLE", "train_rows": train_rows, "test_rows": test_rows,
                "factors": factors, "final_metrics": metrics, "cold_start": cold_start,
                "recommendation_checks": checks, "top_k": top_k,
                "top_k_read_back_audit": top_k_audit,
                "tuning_evidence_preserved": True}
    finally:
        train.unpersist()
        test.unpersist()
