# TV3 NEW MACHINE INPUT RECOVERY REPORT

Audit: 2026-10-05, Asia/Saigon. All counts below were measured on the new
machine, not copied from the historical audit. Recovery stopped at the input
gate; no ALS smoke test, tuning, training or recommendation export was run.

## 1. Environment

- Docker Desktop Linux engine accessible; all seven containers running.
- Compose checked with both `docker-compose.yml` and `docker-compose.tv3.yml`.
- Spark runtime: 3.5.9; image: `movie-recommendation-spark:3.5.9`.
- NumPy: 1.26.4. Docker memory: 8,184,647,680 bytes (about 7.62 GiB).
- Two Spark workers ALIVE, each 2 cores / 2 GiB; applications ran sequentially.
- Hadoop image: 3.5.0. HDFS HEALTHY, live DataNodes 3/3, safe mode OFF.

## 2. TV1 STANDARD

Status: PASS. Original entrypoints executed unchanged:

1. `spark/tv1/data_quality.py`: 32,000,204 RAW ratings; nulls in all four
   columns 0; exact duplicates 0; duplicate user/movie pairs 0; invalid IDs 0;
   invalid ratings 0.
2. `spark/tv1/referential_integrity.py`: duplicate movie IDs 0; missing movie
   references from ratings/tags/links 0; movies without links 0; invalid rating
   steps 0.
3. `spark/tv1/build_standard_layer.py`: created four STANDARD datasets and
   counted them after reading Parquet back.
4. `spark/tv1/verify_standard_layer.py`: independently read all four datasets,
   printed counts, schemas and bounded samples; exit code 0.

STANDARD directory was empty before execution. RAW was not changed or uploaded
again. Outputs contain `_SUCCESS` and Snappy Parquet.

| Dataset | Runtime rows | Schema | HDFS output path |
|---|---:|---|---|
| ratings | 32,000,204 | userId int, movieId int, rating double, timestamp bigint | `/project/movielens/standard/ratings` |
| movies | 87,585 | movieId int, title string, genres string | `/project/movielens/standard/movies` |
| tags | 2,000,072 | userId int, movieId int, tag string, timestamp bigint | `/project/movielens/standard/tags` |
| links | 87,585 | movieId int, imdbId string, tmdbId int | `/project/movielens/standard/links` |

TV1 conversion does not filter/deduplicate; it applies explicit CSV schemas.
TV2 boundary validation subsequently passed for all four STANDARD datasets.
Optional links.tmdbId may be null under the original contract.

## 3. TV2 ALS INPUT

Status: PASS, with a recovered task memory warning described below.
Original entrypoint `spark/tv2/main.py` executed once, unchanged, exit code 0.
`spark/tv2/verify_outputs.py` independently passed with exit code 0.
The entire output directory was checked empty before starting TV2.

| Input | Runtime rows | HDFS output path |
|---|---:|---|
| train | 25,520,897 | `/project/movielens/output/tv2/als/train` |
| test | 6,479,307 | `/project/movielens/output/tv2/als/test` |

Both schemas: `userId int, movieId int, rating double`.
Both datasets have 0 null rows and 0 invalid rating rows; rating range 0.5–5.0.
Counts match historical references without hardcoding or modifying data.

Seed 42, requested train ratio 0.8. Each user's interactions are ordered by
`xxhash64(userId, movieId, seed)`, then movieId. The first
`max(1, floor(0.8 * count))` interactions enter train; the rest enter test.
Actual train ratio: 0.7975229470412126. TV2 performs projection/type enforcement
and metadata join without filtering or deduplication; removed rows: 0.

Runtime checks measured again by `spark/tv3/input_gate.py`:

- Train/test overlap on `(userId, movieId, rating)`: 0.
- Train/test overlap on `(userId, movieId)`: 0.
- Cold-start users: 0; cold-start movies: 4,178.
- Unknown-movie test rows: 4,729; rows affected by unknown user OR movie: 4,729.
- Movies metadata: 87,585 readable rows, 87,585 distinct IDs, 0 critical nulls.

TV2 manifest `/project/movielens/output/tv2/manifest` has status PASS and records
schemas, boundary checks, processing counts, split statistics and read-back.
Independent verifier also confirmed processed ratings and ratings_movies:
32,000,204 rows each. Analytics counts: dataset_summary 1;
rating_distribution 10; user_activity 200,948; movie_popularity 84,432;
genre_statistics 21; rating_trend 29; tag_statistics 131,664.

## 4. HDFS INTEGRITY

Before recovery, RAW fsck: HEALTHY, 10 blocks, ratings 7/7, replication 3.
After recovery, `hdfs fsck /project/movielens`: HEALTHY, 191 files,
181 validated blocks, missing blocks 0, corrupt blocks 0, missing replicas 0,
under-replicated blocks 0, average replication 3.0. `dfsadmin -report`
independently confirmed 3 live DataNodes and no missing/corrupt blocks.
Safe mode OFF. Six STANDARD/ALS dataset listings confirmed successful outputs.

## 5. TV3 INPUT GATE

**PASS**. Spark gate exit code 0 plus external HDFS integrity PASS.
Train/test and metadata are readable, nonempty and valid. Gate performs only
distributed read-only checks and small aggregate reads; no full-frame collect
or toPandas. Historical count enforcement is disabled only for this audit via
the existing `verify_historical_counts=False` parameter.

## 6. SOURCE CHANGES

- Added `spark/tv3/input_gate.py`: repeatable read-only input audit, no ALS fit.
- Updated `spark/tv3/README.md`: documented gate and two-file Compose commands.
- Updated `docs/TV3_INPUT_AUDIT.md`: marked previous-machine results historical.
- Added this report.

No TV1/TV2 source, schema, seed, split, algorithm or contract changed.
Neither Compose file, config nor scripts changed. No compatibility workaround
was needed. No output was deleted, no dataset downloaded, no container restarted.

## 7. GIT STATUS

Branch: `recommendation-model-evaluation`. Pre-existing changes preserved:

```text
 M data/ml-32m/README.txt
 M data/ml-32m/checksums.txt
?? data/ml-32m-before-download-20261004-233138/
?? docker-compose.tv3.yml
```

Session changes are the four files in section 6. No commit, push or merge.
Final mandatory `git diff -- spark/tv1 spark/tv2`: empty.

## 8. ERRORS / WARNINGS

TV2 stage 140, task 2.0, executor 1 reported
`SparkOutOfMemoryError [UNABLE_TO_ACQUIRE_MEMORY]`: unable to acquire 65,536
bytes for UnsafeExternalSorter during sort/merge join. On inspection to stop
the job, it had already completed successfully after Spark's internal task
retry. There was no manual pipeline rerun, memory adjustment or source fix.
Both independent output verification and the subsequent input gate passed.
This warning means the recovery profile is not evidence that full ALS is safe.

Direct `python3 -c 'import pyspark'` failed because PySpark is supplied through
Spark's launcher environment. All actual jobs via spark-submit ran Spark 3.5.9;
no installation or workaround was applied. NumPy imported directly.
Native Hadoop library warning used built-in Java classes; jobs completed.
Git reports existing dataset line-ending warnings; those files were preserved.

## 9. NEXT STEP

Safe to begin TV3 input work / small smoke test: YES.
Full-data ALS resource readiness: NOT ESTABLISHED; do not start without the
user's confirmation. Recovery deliberately stops here.

Recommended smoke-test command (not executed this session):

```powershell
docker exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 --deploy-mode client `
  --driver-memory 1g --executor-memory 1536m `
  --conf spark.driver.host=spark-master `
  --conf spark.driver.bindAddress=0.0.0.0 `
  --conf spark.cores.max=4 --conf spark.executor.cores=2 `
  --conf spark.sql.shuffle.partitions=64 `
  /opt/spark-apps/tv3/smoke_test.py
```

Recommended initial configuration is the profile above, one Spark application
at a time. It is suitable for the bounded 200-user smoke test, not an approval
for full-data ALS. For a future Compose operation always pass both files:
`docker compose -f docker-compose.yml -f docker-compose.tv3.yml ...`.
