"""Controlled ALS experiments selected only on a validation split."""

from train_als import fit_als
from evaluate import evaluate_ratings


def run_experiments(
    train_core, validation, experiments, seed, train_rows, validation_rows,
    on_result=None,
):
    results = []
    for params in experiments:
        experiment_id = params["experiment_id"]
        print(f"Training controlled experiment {experiment_id}: {params}", flush=True)
        base = {
            "experiment_id": experiment_id,
            "evaluation_scope": "SAMPLE",
            "rank": int(params["rank"]),
            "maxIter": int(params["maxIter"]),
            "regParam": float(params["regParam"]),
            "train_rows": int(train_rows),
            "evaluation_dataset": "validation",
            "evaluation_rows": int(validation_rows),
        }
        try:
            model, training_time = fit_als(train_core, params, seed)
            metrics = evaluate_ratings(model, validation, validation_rows)
            base.update(metrics)
            base.update({
                "training_time_sec": float(training_time),
                "status": "PASS",
                "notes": "Hyperparameter selection metric; TV2 test was not used",
            })
        except Exception as exc:  # Preserve failed experiment evidence without inventing metrics.
            base.update({
                "training_time_sec": None,
                "valid_prediction_rows": None,
                "dropped_rows": None,
                "dropped_percentage": None,
                "rmse": None,
                "mae": None,
                "evaluation_time_sec": None,
                "status": "FAILED",
                "notes": f"{type(exc).__name__}: {str(exc)[:500]}",
            })
        results.append(base)
        if on_result is not None:
            on_result(base, len(results) == 1)
    passed = [row for row in results if row["status"] == "PASS"]
    if not passed:
        raise RuntimeError("All controlled ALS experiments failed")
    best = min(passed, key=lambda row: (row["rmse"], row["mae"], row["rank"]))
    return results, best
