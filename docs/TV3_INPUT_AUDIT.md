# TV3 Input Audit

Audit date: 2026-10-03 (Asia/Bangkok). Evidence labels used below: **EXPECTED**, **SOURCE VERIFIED**, and **RUNTIME VERIFIED**.

## 1. Project structure liên quan

```text
docker-compose.yml              Hadoop 3.5.0 + Spark 3.5.9 cluster
config/                         Hadoop core/HDFS configuration
scripts/                        MovieLens download and HDFS upload
spark/tv1/                      RAW validation and STANDARD creation
spark/tv2/                      Processing, analytics, ALS split and verification
spark/tv3/                      Recommendation model and evaluation
```

The Docker runtime contains one NameNode, three DataNodes, one Spark master and two Spark workers. All seven containers were running during this audit.

## 2. TV1 – Source đã tìm thấy

| Path | Mục đích | Input | Output | Trạng thái |
|---|---|---|---|---|
| `scripts/download_movielens.ps1` | Download and checksum MovieLens 32M | GroupLens ZIP | `data/ml-32m` | SOURCE VERIFIED |
| `scripts/upload_to_hdfs.ps1` | Upload four CSV files | Local MovieLens CSV | HDFS RAW | SOURCE VERIFIED |
| `spark/tv1/inspect_movielens.py` | Explicit-schema inspection | HDFS RAW | Console counts/schema/sample | SOURCE VERIFIED |
| `spark/tv1/data_quality.py` | Null, ID, rating and duplicate checks | RAW ratings | Console report | SOURCE VERIFIED |
| `spark/tv1/referential_integrity.py` | Movie foreign-key and rating-step checks | RAW datasets | Console report | SOURCE VERIFIED |
| `spark/tv1/profile_movielens.py` | Distributed dataset profiling | RAW ratings/movies | Console report | SOURCE VERIFIED |
| `spark/tv1/build_standard_layer.py` | Convert CSV to Parquet/Snappy | HDFS RAW | HDFS STANDARD | SOURCE VERIFIED |
| `spark/tv1/verify_standard_layer.py` | Read-back schema/count/sample | HDFS STANDARD | Console report | SOURCE VERIFIED |

## 3. TV1 – Runtime/output đã xác minh

HDFS runtime contains readable `_SUCCESS` Parquet outputs at:

- `/project/movielens/standard/ratings`
- `/project/movielens/standard/movies`
- `/project/movielens/standard/tags`
- `/project/movielens/standard/links`

The TV2 runtime manifest independently records Standard validation PASS with counts 32,000,204 ratings, 87,585 movies, 2,000,072 tags and 87,585 links. Status: **RUNTIME VERIFIED**.

## 4. TV2 – Source đã tìm thấy

| Path | Mục đích | Input | Output | Trạng thái |
|---|---|---|---|---|
| `spark/tv2/config.py` | Central paths/counts/seed | Project contract | Shared constants | SOURCE VERIFIED |
| `spark/tv2/load_standard.py` | Read and validate Standard datasets | TV1 Parquet | Validated DataFrames | SOURCE VERIFIED |
| `spark/tv2/preprocess.py` | Type enforcement, metadata join, ALS projection | Ratings/movies | Processed/ALS frames | SOURCE VERIFIED |
| `spark/tv2/analytics.py` | Distributed EDA tables | Processed datasets | Seven analytics tables | SOURCE VERIFIED |
| `spark/tv2/split_data.py` | Per-user deterministic split and leakage/cold-start stats | ALS frame | Train/test | SOURCE VERIFIED |
| `spark/tv2/main.py` | End-to-end orchestration and manifest | TV1 Standard | `/output/tv2` | SOURCE VERIFIED |
| `spark/tv2/verify_outputs.py` | Independent HDFS read-back | TV2 outputs | PASS/exception | SOURCE + RUNTIME VERIFIED |

Split logic sorts each user's interactions by `xxhash64(userId, movieId, seed)` and `movieId`, then places the first `floor(0.8 * count)` rows (minimum one) in train. Seed is 42.

## 5. TV2 – Runtime/output đã xác minh

The independent verifier ran successfully against Spark 3.5.9 on 2026-10-03 and returned `status=PASS`. Runtime counts:

- processed ratings: 32,000,204
- processed ratings + movies: 32,000,204
- ALS train: 25,520,897
- ALS test: 6,479,307
- all seven analytics outputs were readable and matched the manifest

The HDFS manifest is readable, has `status=PASS`, and contains read-back counts, split rules, schemas and cold-start statistics.

## 6. TV2 → TV3 Input Contract

| Item | Expected | Actual | Evidence | Status |
|---|---|---|---|---|
| Train HDFS path | `/project/movielens/output/tv2/als/train` | Present/readable | TV2 verifier + HDFS listing | PASS |
| Test HDFS path | `/project/movielens/output/tv2/als/test` | Present/readable | TV2 verifier + HDFS listing | PASS |
| Train schema | `userId int, movieId int, rating double` | Exact match | Runtime manifest/read-back | PASS |
| Test schema | Same | Exact match | Runtime manifest/read-back | PASS |
| Train count | 25,520,897 | 25,520,897 | Runtime verifier | PASS |
| Test count | 6,479,307 | 6,479,307 | Runtime verifier | PASS |
| Null/invalid ratings | 0 | 0 inherited from validated Standard and projection-only TV2 logic | Source + runtime manifest | PASS |
| Seed/split rule | Seed 42, deterministic per-user 80/20 | Seed 42, seeded `xxhash64`, actual 79.7523/20.2477 | Source + runtime manifest | PASS |
| Train/test overlap | 0 | 0 | Runtime manifest | PASS |
| Unknown test users | 0 | 0 | Runtime manifest | PASS |
| Unknown test movies | 4,178 | 4,178 | Runtime manifest | WARNING |
| Rows affected by unknown movie | 4,729 | 4,729 | Runtime manifest | WARNING |
| Movies metadata path | `/project/movielens/standard/movies` | Present/readable | HDFS listing + manifest | PASS |
| Movies schema | `movieId int, title string, genres string` | Exact match, 87,585 valid rows | Runtime manifest | PASS |

Cold-start warnings are expected real split characteristics. TV3 uses `coldStartStrategy="drop"` and explicitly reports dropped coverage.

## 7. Sai khác giữa tài liệu và project thực tế

- README's older Spark UI examples use ports 8080–8082; actual Compose host ports are 18080–18082.
- README's older TV1 commands omit the `/tv1/` path segment; actual mounted script paths are `/opt/spark-apps/tv1/...`.
- No runtime count discrepancy was found for the TV2 → TV3 contract.
- The upstream Apache Spark Python image lacked `numpy`, so importing `pyspark.ml` failed during the TV3 smoke test. A minimal derived image (`docker/spark/Dockerfile`) pins `numpy==1.26.4`; Compose uses it for Spark master/workers. No TV1/TV2 data contract or output path changed.
- Full-data hyperparameter tuning saturated the local 4-core/4-GiB Docker runtime, and a later full-data final fit disconnected both workers. TV3 therefore defaults to a deterministic 10,000-user scope for tuning and final fit, recording `WARNING` plus exact scope/counts in the manifest. Full-data mode remains available through CLI on a larger cluster. Interrupted attempts produced no reported metrics.
- Rank 15 caused sustained resource thrashing on the sample. Runtime-safe controlled experiments therefore keep rank 10; larger ranks remain a documented larger-cluster extension.
- The requested controlled grid is A=`rank=10,maxIter=5,regParam=0.10`, B=`rank=10,maxIter=8,regParam=0.10`, and C=`rank=10,maxIter=8,regParam=0.05`. It must run only on the deterministic 10,000-user sample before a separate full-data readiness decision.
- Working-tree-only changes in `data/ml-32m/README.txt` and `data/ml-32m/checksums.txt` were line-ending/stat artifacts with no content diff; both were restored to HEAD without changing any dataset CSV.

## 8. Command tái tạo input TV3 nếu cần

```powershell
docker exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --deploy-mode client `
  --conf spark.driver.host=spark-master `
  --conf spark.driver.bindAddress=0.0.0.0 `
  --conf spark.executor.memory=1536m `
  --conf spark.cores.max=4 `
  --conf spark.sql.shuffle.partitions=32 `
  /opt/spark-apps/tv2/main.py

docker exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --deploy-mode client `
  --conf spark.driver.host=spark-master `
  --conf spark.driver.bindAddress=0.0.0.0 `
  /opt/spark-apps/tv2/verify_outputs.py
```

## 9. Kết luận TV3 có thể bắt đầu hay chưa

**PASS — TV3 can start.** Train, test and movie metadata are runtime verified. The only warning is expected item cold-start, which is part of the TV3 evaluation policy rather than an input failure.
