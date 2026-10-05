# TV3 – Recommendation Model & Evaluation

## 1. Mục tiêu

TV3 trains and evaluates Spark MLlib ALS on the TV2 handoff, selects parameters on a deterministic validation set, evaluates the selected model once on TV2 test, and exports model/metrics/recommendations for TV4.

Phase B on the new machine is complete with a **SAMPLE** model. See
`docs/TV3_PHASE_B_COMPLETION_REPORT.md` and `docs/TV3_HANDOFF_TO_TV4.md`.
Current manifest is `SAMPLE_COMPLETE_WARNING`; full-data ALS has not run.
Final sample test RMSE=0.8146154253, MAE=0.6200873149; 200 Top-10 rows for
20 demo users are materialized. Precision/Recall@10 on 500 sample users are
both zero and are a documented ranking-quality limitation.

Phase C diagnostic is complete. See
[ranking report](../../docs/TV3_RANKING_DIAGNOSTIC_REPORT.md) and its JSON evidence.
Metric/ID/filter audits passed; weak ALS score ordering favors items with little
sample-train support. On the same 500 users, train-only popularity achieves
Precision@10=0.1252 versus ALS=0. The saved model and handoff remain unchanged.
`ranking_diagnostic.py` reads the saved model without fitting and refuses an
existing diagnostic output path; its completed results should be read directly.

`final_sample.py` fits one model from preserved tuning evidence and evaluates
matching sample TV2 test; `sample_handoff.py` reloads it in a separate application
and exports demo Top-N/Top-K. They refuse existing outputs and never tune/full-fit.
Use `verify_outputs.py` to inspect the current artifacts. Do not rerun `main.py`
or training to serve the TV4 dashboard. `recommend.py` writes only new paths
inside TV3/recommendations and rejects users outside the saved model scope.

## 2. Input từ TV2

| Dataset | HDFS path | Schema |
|---|---|---|
| Train | `/project/movielens/output/tv2/als/train` | `userId int, movieId int, rating double` |
| Test | `/project/movielens/output/tv2/als/test` | Same |
| Movies | `/project/movielens/standard/movies` | `movieId int, title string, genres string` |

Do not train from RAW CSV and do not repeat TV2 preprocessing.

New-machine smoke/tuning results and the full ALS readiness checkpoint are in
`docs/TV3_PHASE_A_COMPLETION_REPORT.md`. Controlled tuning records a failed
experiment and stops immediately rather than starting the next configuration.

For new-machine recovery, run `input_gate.py` after the original TV1/TV2
pipelines finish, with the same Spark resource options as below. It only reads
inputs and measures runtime counts, nulls, overlap and cold-start; it never fits
ALS or writes HDFS outputs. Historical train/test counts are references.
The overall gate also requires `hdfs fsck /project/movielens -files -blocks`
and `hdfs dfsadmin -report` to pass. New-machine evidence is recorded in
`docs/TV3_NEW_MACHINE_INPUT_RECOVERY.md`.

## 3. Cách chạy toàn bộ pipeline

Run from repository root in PowerShell:

```powershell
docker compose -f docker-compose.yml -f docker-compose.tv3.yml build spark-master
docker compose -f docker-compose.yml -f docker-compose.tv3.yml up -d --force-recreate spark-master spark-worker1 spark-worker2

docker exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --deploy-mode client `
  --conf spark.driver.host=spark-master `
  --conf spark.driver.bindAddress=0.0.0.0 `
  --conf spark.executor.memory=1536m `
  --conf spark.cores.max=4 `
  --conf spark.sql.shuffle.partitions=64 `
  /opt/spark-apps/tv3/main.py
```

The controlled tuning set is exactly A=`rank=10,maxIter=5,regParam=0.10`, B=`rank=10,maxIter=8,regParam=0.10`, and C=`rank=10,maxIter=8,regParam=0.05`. Tuning defaults to a deterministic 10,000-user sample, and every experiment metric is labelled `evaluation_scope=SAMPLE`.

For the safe pre-final tuning stage, append `--tuning-only`. This persists every experiment immediately, selects the best configuration using validation metrics only, writes a `TUNING_COMPLETE` manifest, and exits before final fitting, TV2 test evaluation, Top-K, or recommendation export.

`final_train.py` is the separately gated full-data entrypoint. It uses the best
configuration on the deterministic tuning sample, trains exactly one model, saves
it, evaluates once on TV2 test, and exits. It does not tune or materialize Top-N.
Do not run it until the resource-readiness report is approved.

Use `--top-k-max-users 0 --recommendation-users 0` for full eligible-user Top-K evaluation and all-user recommendation export when sufficient resources are available.

## 4. Tuning và final model

Experiments are centrally configured in `config.py`. TV2 train is split deterministically per user into `train_core/validation` with seed 42. Hyperparameters are selected only from validation RMSE (then MAE/rank as tie-breakers). TV2 test is untouched until final evaluation.

ALS uses explicit feedback and exposes `rank`, `maxIter`, `regParam`, and `seed`. Do not add a large grid on the local cluster.

## 5. Cold-start và metrics

ALS uses `coldStartStrategy="drop"`. Reports include total evaluation rows, valid predictions, dropped rows/percentage, RMSE and MAE. Expected item cold-start from TV2 is 4,729 test rows, but the current runtime output is always the source of truth.

Top-K defaults to K=10. A relevant item means `test.rating >= 4.0`. Recommendations exclude `(userId, movieId)` pairs present in TV2 train.

## 6. Top-N on demand/batch

One user:

```powershell
docker exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --conf spark.driver.host=spark-master `
  --conf spark.driver.bindAddress=0.0.0.0 `
  /opt/spark-apps/tv3/recommend.py --user-id 1 --top-n 10 `
  --output hdfs://namenode:9000/project/movielens/output/tv3/recommendations/user_1
```

All users (resource intensive):

```powershell
docker exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --conf spark.driver.host=spark-master `
  --conf spark.driver.bindAddress=0.0.0.0 `
  /opt/spark-apps/tv3/recommend.py --all-users --top-n 10
```

`--users-file <parquet-path>` supports a supplied user subset. Unknown single users fail with a clear error. Missing metadata is excluded by the metadata join, and output may contain fewer than N rows if the candidate buffer is exhausted.

## 7. Output cho TV4

```text
/project/movielens/output/tv3/model/als_best
/project/movielens/output/tv3/metrics/experiments
/project/movielens/output/tv3/metrics/final
/project/movielens/output/tv3/metrics/top_k
/project/movielens/output/tv3/recommendations/topn
/project/movielens/output/tv3/manifest
```

Recommendation schema:

```text
userId int, movieId int, title string, genres string, prediction double, rank int
```

## 8. Verify output

Run the non-production smoke test first:

```powershell
docker exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --conf spark.driver.host=spark-master `
  --conf spark.driver.bindAddress=0.0.0.0 `
  /opt/spark-apps/tv3/smoke_test.py
```

Then verify production outputs:

```powershell
docker exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --deploy-mode client `
  --conf spark.driver.host=spark-master `
  --conf spark.driver.bindAddress=0.0.0.0 `
  /opt/spark-apps/tv3/verify_outputs.py
```

## 9. Lỗi thường gặp

- `Path does not exist ... output/tv2/als`: run and verify TV2 first.
- Executor lost/OOM: keep the default sampled Top-K/Top-N, lower `spark.sql.shuffle.partitions` only after inspecting the Spark UI, or increase Docker memory.
- Fewer predictions than test rows: expected item cold-start; inspect dropped coverage in final metrics.
- Unknown user: the saved ALS model has no user factor; select an ID present in TV2 train.
- Existing outputs: TV3 deliberately overwrites only `/output/tv3`, never TV1/TV2 paths.
