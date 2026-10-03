"""End-to-end TV3 ALS tuning, final evaluation, and recommendation pipeline."""

import argparse
import json
from datetime import datetime, timezone

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F, types as T

from config import (
    EXPERIMENTS,
    EXPERIMENTS_PATH,
    FINAL_MAX_USERS,
    FINAL_METRICS_PATH,
    MANIFEST_PATH,
    MODEL_PATH,
    MOVIES_PATH,
    OUTPUT_BASE,
    RECOMMENDATION_CANDIDATE_MULTIPLIER,
    RECOMMENDATIONS_PATH,
    RELEVANCE_THRESHOLD,
    SEED,
    TEST_PATH,
    TUNING_MAX_USERS,
    TOP_K,
    TOP_K_MAX_USERS,
    TOP_N,
    TOP_N_SAMPLE_USERS,
    TOPK_METRICS_PATH,
    TRAIN_PATH,
    VALIDATION_RATIO,
)
from evaluate import evaluate_ratings, split_train_validation
from load_inputs import load_and_validate_inputs
from topn import deterministic_user_sample, evaluate_top_k, recommend_unseen
from train_als import fit_als
from tune_als import run_experiments


def parse_args():
    parser = argparse.ArgumentParser(description="MovieLens TV3 ALS pipeline")
    parser.add_argument("--top-k-max-users", type=int, default=TOP_K_MAX_USERS,
                        help="Deterministic Top-K sample size; 0 means all eligible users")
    parser.add_argument("--tuning-max-users", type=int, default=TUNING_MAX_USERS,
                        help="Deterministic tuning user sample; 0 means full TV2 train")
    parser.add_argument("--final-max-users", type=int, default=FINAL_MAX_USERS,
                        help="Deterministic final-model users; 0 means full TV2 train")
    parser.add_argument("--recommendation-users", type=int, default=TOP_N_SAMPLE_USERS,
                        help="Deterministic Top-N export user count; 0 means all train users")
    parser.add_argument("--top-n", type=int, default=TOP_N)
    parser.add_argument("--skip-historical-count-check", action="store_true")
    parser.add_argument(
        "--tuning-only",
        action="store_true",
        help="Stop after sampled validation tuning; do not fit or evaluate a final model",
    )
    return parser.parse_args()


def write_json_text(spark, payload, path):
    value = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    spark.createDataFrame([(value,)], "json STRING").coalesce(1).write.mode("overwrite").text(path)


def rows_to_frame(spark, rows):
    """Use an explicit schema so nullable failure fields remain serializable."""
    schema = T.StructType([
        T.StructField("experiment_id", T.StringType(), False),
        T.StructField("evaluation_scope", T.StringType(), False),
        T.StructField("rank", T.IntegerType(), False),
        T.StructField("maxIter", T.IntegerType(), False),
        T.StructField("regParam", T.DoubleType(), False),
        T.StructField("train_rows", T.LongType(), False),
        T.StructField("evaluation_dataset", T.StringType(), False),
        T.StructField("evaluation_rows", T.LongType(), False),
        T.StructField("prediction_rows_before_filter", T.LongType(), True),
        T.StructField("valid_prediction_rows", T.LongType(), True),
        T.StructField("dropped_rows", T.LongType(), True),
        T.StructField("dropped_percentage", T.DoubleType(), True),
        T.StructField("rmse", T.DoubleType(), True),
        T.StructField("mae", T.DoubleType(), True),
        T.StructField("training_time_sec", T.DoubleType(), True),
        T.StructField("evaluation_time_sec", T.DoubleType(), True),
        T.StructField("cold_start_policy", T.StringType(), True),
        T.StructField("status", T.StringType(), False),
        T.StructField("notes", T.StringType(), False),
    ])
    normalized = [{field.name: row.get(field.name) for field in schema.fields} for row in rows]
    return spark.createDataFrame(normalized, schema)


def main():
    args = parse_args()
    if (args.top_n <= 0 or args.top_k_max_users < 0 or args.recommendation_users < 0
            or args.tuning_max_users < 0 or args.final_max_users < 0):
        raise ValueError("top-n must be positive and user limits cannot be negative")

    spark = SparkSession.builder.appName("MovieLensTV3ALS").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
    cached = []
    manifest = {
        "project": "Big Data Movie Recommendation System",
        "module": "TV3 - Recommendation Model & Evaluation",
        "status": "RUNNING",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "spark_version": spark.version,
        "input": {"train_path": TRAIN_PATH, "test_path": TEST_PATH, "movies_path": MOVIES_PATH},
        "output_base": OUTPUT_BASE,
        "seed": SEED,
    }
    try:
        print("[1/8] Validating TV2 -> TV3 inputs", flush=True)
        train, test, movies, input_validation = load_and_validate_inputs(
            spark, not args.skip_historical_count_check
        )
        train = train.persist(StorageLevel.DISK_ONLY)
        test = test.persist(StorageLevel.DISK_ONLY)
        movies = movies.persist(StorageLevel.MEMORY_AND_DISK)
        cached.extend([train, test, movies])
        manifest["input_validation"] = input_validation
        train_rows = input_validation["train"]["row_count"]
        test_rows = input_validation["test"]["row_count"]
        manifest["input_counts"] = {"train": train_rows, "test": test_rows,
                                    "movies": input_validation["movies"]["row_count"]}

        print("[2/8] Creating deterministic tuning sample and train_core/validation", flush=True)
        if args.tuning_max_users:
            tuning_users = deterministic_user_sample(train, args.tuning_max_users, SEED)
            tuning_input = train.join(F.broadcast(tuning_users), "userId", "inner")
            tuning_scope = "DETERMINISTIC_USER_SAMPLE"
        else:
            tuning_input = train
            tuning_scope = "FULL_TV2_TRAIN"
        tuning_input = tuning_input.persist(StorageLevel.DISK_ONLY)
        cached.append(tuning_input)
        tuning_rows = tuning_input.count()
        tuning_users_count = tuning_input.select("userId").distinct().count()
        train_core, validation = split_train_validation(tuning_input, VALIDATION_RATIO, SEED)
        train_core = train_core.persist(StorageLevel.DISK_ONLY)
        validation = validation.persist(StorageLevel.DISK_ONLY)
        cached.extend([train_core, validation])
        train_core_rows, validation_rows = train_core.count(), validation.count()
        manifest["validation_split"] = {
            "method": "per-user deterministic seeded xxhash64",
            "seed": SEED,
            "tuning_scope": tuning_scope,
            "tuning_max_users": args.tuning_max_users or None,
            "tuning_users": tuning_users_count,
            "tuning_rows": tuning_rows,
            "requested_ratio": VALIDATION_RATIO,
            "train_core_rows": train_core_rows,
            "validation_rows": validation_rows,
        }

        print("[3/8] Running three controlled ALS experiments", flush=True)
        def persist_experiment(row, first):
            mode = "overwrite" if first else "append"
            rows_to_frame(spark, [row]).write.mode(mode).parquet(EXPERIMENTS_PATH)

        experiment_rows, best = run_experiments(
            train_core, validation, EXPERIMENTS, SEED, train_core_rows, validation_rows,
            on_result=persist_experiment,
        )
        manifest["experiments"] = experiment_rows
        best_params = {key: best[key] for key in ("rank", "maxIter", "regParam")}
        manifest["selection"] = {
            "metric": "minimum validation RMSE, then MAE and rank",
            "experiment_id": best["experiment_id"],
            **best_params,
        }

        if args.tuning_only:
            manifest["outputs"] = {
                "experiments": EXPERIMENTS_PATH,
                "manifest": MANIFEST_PATH,
            }
            manifest["evaluation_scope"] = "SAMPLE"
            manifest["status"] = "TUNING_COMPLETE"
            manifest["limitation"] = (
                "Only deterministic sampled validation tuning was run; no final model, "
                "TV2 test evaluation, Top-K evaluation, or recommendation export was run."
            )
            write_json_text(spark, manifest, MANIFEST_PATH)
            print(json.dumps(manifest, ensure_ascii=False, indent=2), flush=True)
            print("TV3 TUNING-ONLY PASSED", flush=True)
            return

        print(f"[4/8] Preparing final model scope: {best_params}", flush=True)
        if args.final_max_users:
            final_users = deterministic_user_sample(train, args.final_max_users, SEED)
            final_train = train.join(F.broadcast(final_users), "userId", "inner")
            final_test = test.join(F.broadcast(final_users), "userId", "inner")
            final_scope = "DETERMINISTIC_USER_SAMPLE"
        else:
            final_train, final_test = train, test
            final_scope = "FULL_TV2_DATA"
        final_train = final_train.persist(StorageLevel.DISK_ONLY)
        final_test = final_test.persist(StorageLevel.DISK_ONLY)
        cached.extend([final_train, final_test])
        final_train_rows, final_test_rows = final_train.count(), final_test.count()
        print(f"Fitting final model scope={final_scope}, train_rows={final_train_rows}", flush=True)
        final_model, final_training_time = fit_als(final_train, best_params, SEED)
        final_model.write().overwrite().save(MODEL_PATH)

        print("[5/8] Evaluating final model on the matching untouched TV2 test scope", flush=True)
        final_metrics = evaluate_ratings(final_model, final_test, final_test_rows)
        final_metrics.update({
            **best_params,
            "experiment_id": best["experiment_id"],
            "evaluation_scope": final_scope,
            "train_rows": final_train_rows,
            "evaluation_dataset": "TV2 test" if not args.final_max_users else "TV2 test deterministic user sample",
            "training_time_sec": final_training_time,
            "status": "PASS",
        })
        rows_to_frame(spark, [{**final_metrics, "notes": "Final unbiased test evaluation"}]).write.mode(
            "overwrite"
        ).parquet(FINAL_METRICS_PATH)
        manifest["best_model"] = {
            **best_params,
            "path": MODEL_PATH,
            "training_time_sec": final_training_time,
            "scope": final_scope,
            "max_users": args.final_max_users or None,
            "train_rows": final_train_rows,
            "test_rows": final_test_rows,
        }
        manifest["evaluation"] = final_metrics

        print("[6/8] Running Top-K ranking evaluation", flush=True)
        top_k_user_limit = None if args.top_k_max_users == 0 else args.top_k_max_users
        top_k = evaluate_top_k(
            final_model, final_train, final_test, movies, TOP_K, RELEVANCE_THRESHOLD,
            top_k_user_limit, RECOMMENDATION_CANDIDATE_MULTIPLIER, SEED,
        )
        spark.createDataFrame([top_k]).write.mode("overwrite").parquet(TOPK_METRICS_PATH)
        manifest["top_k"] = top_k

        print("[7/8] Exporting unseen-item Top-N recommendations", flush=True)
        recommendation_limit = None if args.recommendation_users == 0 else args.recommendation_users
        users = deterministic_user_sample(final_train, recommendation_limit, SEED)
        recommendations = recommend_unseen(
            final_model, final_train, movies, users, args.top_n,
            RECOMMENDATION_CANDIDATE_MULTIPLIER,
        )
        recommendations.write.mode("overwrite").parquet(RECOMMENDATIONS_PATH)
        recommendation_rows = spark.read.parquet(RECOMMENDATIONS_PATH).count()
        recommendation_user_count = spark.read.parquet(RECOMMENDATIONS_PATH).select("userId").distinct().count()
        manifest["recommendations"] = {
            "path": RECOMMENDATIONS_PATH,
            "scope": "ALL_MODEL_USERS" if recommendation_limit is None else "DETERMINISTIC_SAMPLE",
            "requested_users": recommendation_limit,
            "actual_users": recommendation_user_count,
            "rows": recommendation_rows,
            "top_n": args.top_n,
            "excludes_train_history": True,
            "schema": "userId int, movieId int, title string, genres string, prediction double, rank int",
        }

        print("[8/8] Writing TV3 manifest", flush=True)
        manifest["outputs"] = {
            "model": MODEL_PATH,
            "experiments": EXPERIMENTS_PATH,
            "final_metrics": FINAL_METRICS_PATH,
            "top_k_metrics": TOPK_METRICS_PATH,
            "recommendations": RECOMMENDATIONS_PATH,
            "manifest": MANIFEST_PATH,
        }
        manifest["status"] = "WARNING" if args.final_max_users else "PASS"
        if args.final_max_users:
            manifest["limitation"] = (
                "Final ALS model/evaluation use a deterministic user sample because the "
                "local 4-core/4-GiB Docker cluster lost both workers on full-data ALS. "
                "Run with --tuning-max-users 0 --final-max-users 0 on a larger cluster."
            )
        write_json_text(spark, manifest, MANIFEST_PATH)
        print(json.dumps(manifest, ensure_ascii=False, indent=2), flush=True)
        print("TV3 PIPELINE PASSED", flush=True)
    finally:
        for frame in reversed(cached):
            frame.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
