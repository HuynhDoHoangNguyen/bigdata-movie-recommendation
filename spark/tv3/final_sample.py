"""Fit exactly one SAMPLE model from existing tuning evidence; never tune/full-fit."""

import json
from datetime import datetime, timezone

from pyspark import StorageLevel
from pyspark.ml.recommendation import ALSModel
from pyspark.sql import SparkSession, functions as F

from config import (BEST_SAMPLE_PARAMS, EXPERIMENTS_PATH, FINAL_METRICS_PATH,
                    MANIFEST_PATH, MODEL_PATH, SAMPLE_USERS_PATH, SEED,
                    TUNING_MANIFEST_PATH, TRAIN_PATH, TEST_PATH, MOVIES_PATH)
from evaluate import evaluate_ratings
from load_inputs import load_and_validate_inputs
from output_contract import (check_factors, cold_start_stats, read_manifest,
                             require_new_outputs, write_manifest)
from topn import deterministic_user_sample
from train_als import fit_als


def main():
    spark = SparkSession.builder.appName("TV3FinalSampleALS").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    cached = []
    try:
        tuning = read_manifest(spark, MANIFEST_PATH)
        if (tuning["status"] != "TUNING_COMPLETE" or tuning["evaluation_scope"] != "SAMPLE"
                or tuning["seed"] != SEED or tuning["validation_split"]["tuning_users"] != 10000
                or tuning["validation_split"]["tuning_max_users"] != 10000):
            raise ValueError("Expected completed Phase A 10,000-user tuning")
        experiments = [row.asDict() for row in spark.read.parquet(EXPERIMENTS_PATH).take(4)]
        if len(experiments) != 3 or any(
            row["status"] != "PASS" or row["evaluation_scope"] != "SAMPLE"
            or row["evaluation_dataset"] != "validation" for row in experiments
        ):
            raise ValueError("Expected three successful sample validation experiments")
        best = min(experiments, key=lambda row: (row["rmse"], row["mae"], row["rank"]))
        params = {key: best[key] for key in ("rank", "maxIter", "regParam")}
        if params != BEST_SAMPLE_PARAMS or best["experiment_id"] != tuning["selection"]["experiment_id"]:
            raise ValueError("Candidate differs from persisted sample selection")
        require_new_outputs(spark, [MODEL_PATH, FINAL_METRICS_PATH, SAMPLE_USERS_PATH, TUNING_MANIFEST_PATH])
        print("[1/4] Loading inputs and reconstructing the original tuning users", flush=True)
        train, test, _movies, input_validation = load_and_validate_inputs(spark)
        users = deterministic_user_sample(train, 10000, SEED).persist(StorageLevel.DISK_ONLY)
        cached.append(users)
        sample_train = train.join(F.broadcast(users), "userId", "inner").persist(StorageLevel.DISK_ONLY)
        sample_test = test.join(F.broadcast(users), "userId", "inner").persist(StorageLevel.DISK_ONLY)
        cached.extend([sample_train, sample_test])
        user_count, train_rows, test_rows = users.count(), sample_train.count(), sample_test.count()
        if user_count != 10000 or train_rows != tuning["validation_split"]["tuning_rows"]:
            raise ValueError("Sample does not reproduce the tuning input")
        write_manifest(spark, tuning, TUNING_MANIFEST_PATH)
        users.coalesce(1).write.mode("errorifexists").parquet(SAMPLE_USERS_PATH)
        print(f"[2/4] Fitting one SAMPLE model on {train_rows} rows: {params}", flush=True)
        model, training_time = fit_als(sample_train, params, SEED)
        model.write().save(MODEL_PATH)
        del model
        print("[3/4] Reloading persisted model and validating factors", flush=True)
        model = ALSModel.load(MODEL_PATH)
        factors = check_factors(model)
        if factors["users"]["rows"] != user_count:
            raise ValueError("Reloaded model user count mismatch")
        print("[4/4] Evaluating matching SAMPLE TV2 test (not tuning validation)", flush=True)
        metrics = evaluate_ratings(model, sample_test, test_rows)
        cold_start = cold_start_stats(model, sample_test)
        if metrics["dropped_rows"] != cold_start["affected_test_rows"]:
            raise ValueError("Cold-start rows do not match dropped predictions")
        metrics.update({**params, "seed": SEED, "evaluation_scope": "SAMPLE",
                        "model_scope": "SAMPLE", "evaluation_dataset": "TV2 test deterministic user sample",
                        "sample_train_rows": train_rows, "sample_test_rows": test_rows,
                        "training_time_sec": float(training_time), "status": "PASS"})
        spark.createDataFrame([metrics]).coalesce(1).write.mode("errorifexists").parquet(FINAL_METRICS_PATH)
        manifest = {"status": "SAMPLE_MODEL_READY_WARNING", "model_scope": "SAMPLE",
                    "evaluation_scope": "SAMPLE", "recommendation_scope": "NOT_RUN",
                    "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                    "spark_version": spark.version, "seed": SEED, "parameters": params,
                    "parameter_provenance": "Best configuration on deterministic tuning SAMPLE",
                    "input": {"train_path": TRAIN_PATH, "test_path": TEST_PATH, "movies_path": MOVIES_PATH},
                    "input_validation": input_validation, "sample": {"users_path": SAMPLE_USERS_PATH,
                    "users": user_count, "train_rows": train_rows, "test_rows": test_rows,
                    "sampling": "orderBy xxhash64(userId, lit(seed)), userId; limit 10000"},
                    "model": {"path": MODEL_PATH, "reload_status": "PASS", "factors": factors},
                    "evaluation": metrics, "cold_start": cold_start,
                    "tuning_evidence": {"manifest_path": TUNING_MANIFEST_PATH,
                    "experiments_path": EXPERIMENTS_PATH, "manifest": tuning},
                    "outputs": {"model": MODEL_PATH, "final_metrics": FINAL_METRICS_PATH,
                    "sample_users": SAMPLE_USERS_PATH, "manifest": MANIFEST_PATH},
                    "limitation": "SAMPLE model only; full MovieLens ALS was not trained."}
        write_manifest(spark, manifest, MANIFEST_PATH, overwrite=True)
        print(json.dumps(manifest, indent=2), flush=True)
        print("TV3 SAMPLE MODEL/EVALUATION PASSED", flush=True)
    finally:
        for frame in reversed(cached):
            frame.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
