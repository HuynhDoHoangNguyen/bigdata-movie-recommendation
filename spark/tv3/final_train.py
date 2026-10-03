"""One-shot full TV2 train -> save -> test evaluation job.

This entrypoint deliberately does not tune parameters or generate Top-N output.
Run it only after the resource-readiness gate has been approved.
"""

import json
from datetime import datetime, timezone

from pyspark.sql import SparkSession

from config import (
    BEST_SAMPLE_PARAMS,
    FINAL_METRICS_PATH,
    MANIFEST_PATH,
    MODEL_PATH,
    MOVIES_PATH,
    OUTPUT_BASE,
    SEED,
    TEST_PATH,
    TRAIN_PATH,
)
from evaluate import evaluate_ratings
from load_inputs import load_and_validate_inputs
from train_als import fit_als


def write_json_text(spark, payload, path):
    value = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    spark.createDataFrame([(value,)], "json STRING").coalesce(1).write.mode(
        "overwrite"
    ).text(path)


def main():
    spark = SparkSession.builder.appName("MovieLensTV3FinalALS").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
    manifest = {
        "project": "Big Data Movie Recommendation System",
        "module": "TV3 - Full-data ALS train and evaluation",
        "status": "RUNNING",
        "evaluation_scope": "FULL",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "spark_version": spark.version,
        "seed": SEED,
        "input": {
            "train_path": TRAIN_PATH,
            "test_path": TEST_PATH,
            "movies_path": MOVIES_PATH,
        },
        "output_base": OUTPUT_BASE,
        "parameter_provenance": "Best configuration on deterministic tuning sample",
        "parameters": BEST_SAMPLE_PARAMS,
    }
    try:
        print("[1/4] Validating full TV2 train/test inputs", flush=True)
        train, test, _movies, validation = load_and_validate_inputs(spark, True)
        train_rows = int(validation["train"]["row_count"])
        test_rows = int(validation["test"]["row_count"])
        manifest["input_validation"] = validation

        print(f"[2/4] Training one full ALS model: {BEST_SAMPLE_PARAMS}", flush=True)
        model, training_time = fit_als(train, BEST_SAMPLE_PARAMS, SEED)

        print(f"[3/4] Saving model to {MODEL_PATH}", flush=True)
        model.write().overwrite().save(MODEL_PATH)

        print("[4/4] Evaluating once on the untouched TV2 test", flush=True)
        metrics = evaluate_ratings(model, test, test_rows)
        metrics.update({
            **BEST_SAMPLE_PARAMS,
            "evaluation_scope": "FULL",
            "evaluation_dataset": "TV2 test",
            "train_rows": train_rows,
            "training_time_sec": float(training_time),
            "status": "PASS",
        })
        spark.createDataFrame([metrics]).coalesce(1).write.mode("overwrite").parquet(
            FINAL_METRICS_PATH
        )
        manifest.update({
            "status": "PASS",
            "model": {"path": MODEL_PATH, "train_rows": train_rows},
            "evaluation": metrics,
            "outputs": {
                "model": MODEL_PATH,
                "final_metrics": FINAL_METRICS_PATH,
                "manifest": MANIFEST_PATH,
            },
            "top_n_status": "NOT_RUN",
        })
        write_json_text(spark, manifest, MANIFEST_PATH)
        print(json.dumps(manifest, ensure_ascii=False, indent=2), flush=True)
        print("TV3 FULL-DATA TRAIN/EVALUATION PASSED", flush=True)
    except Exception as exc:
        manifest.update({
            "status": "FAILED",
            "failure": f"{type(exc).__name__}: {str(exc)[:1000]}",
        })
        try:
            write_json_text(spark, manifest, MANIFEST_PATH)
        except Exception:
            pass
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
