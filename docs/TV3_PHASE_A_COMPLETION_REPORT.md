# TV3 PHASE A COMPLETION REPORT

Ngày: 05/10/2026 (Asia/Saigon). Checkpoint Phase A hoàn tất.
**Smoke test PASS; sample tuning TUNING_COMPLETE; SAFE TO TRY FULL TRAIN: NO
(chưa đủ bằng chứng an toàn với cấu hình hiện tại).** Không chạy final ALS.

## 1. SOURCE AUDIT

Đã đọc tài liệu recovery, input audit, issues report, README và toàn bộ 13
module Python trong `spark/tv3/`, gồm `__init__.py` và `input_gate.py`.

| Thành phần | Kết quả audit |
|---|---|
| config.py | Đúng paths TV2/TV3; seed 42; A/B/C đúng yêu cầu; candidate C đúng |
| load_inputs.py | Kiểm tra schema/type, counts, critical null và rating range; runtime PASS |
| train_als.py | Explicit-feedback ALS, coldStartStrategy=drop, fit có đo thời gian |
| evaluate.py | Validation split xác định trên TV2 train; predictions DISK_ONLY; có RMSE/MAE/coverage và unpersist |
| tune_als.py | Chỉ dùng validation để lựa chọn; đã bổ sung dừng sau experiment lỗi |
| main.py | `--tuning-only` return trước final fit/test evaluation/Top-K/export |
| final_train.py | FULL train → fit một model → save → đánh giá FULL test → save metrics/manifest; không import/call tuning hoặc Top-N |
| smoke_test.py | Mẫu 200 user, rank 4/maxIter 1/regParam 0.1; runtime PASS |
| topn.py / recommend.py | Lọc train history bằng left-anti join, join metadata, rank theo prediction; CLI riêng |
| verify_outputs.py | Verifier production yêu cầu model + final metrics + Top-K + recommendations + experiments + manifest |
| input_gate.py | Chỉ đọc input, kiểm tra overlap/cold-start; không fit ALS |

Không thiếu thành phần cốt lõi. Những deliverable chưa có: model final, final
test metrics, Top-K, production recommendations và handoff TV4. Verifier
production chưa chạy vì các output này chưa tồn tại.

Lưu ý cho phase tiếp theo: manifest của `final_train.py` dùng keys `model` và
`evaluation`, không có `recommendations` như verifier production yêu cầu.
Chạy final-only thành công chưa đủ để verifier production PASS; cần bước
Top-N/Top-K riêng và manifest bàn giao phù hợp. Final entrypoint cũng chưa
reload model để kiểm tra độc lập. Không sửa/chạy các bước này trong Phase A.
`recommend.py --output` nhận path do người gọi nhập; lần export sau phải giới
hạn path trong TV3. Không chạy recommendation CLI trong phiên này.

Không có `collect()` hoặc `toPandas()` trên full ratings trong source TV3.
Các `.first()` đọc aggregate nhỏ; `collect_set/collect_list` trong Top-K là
aggregation phân tán theo user. Top-K full scope chưa được chứng minh về RAM.
Tuning được chạy với 10.000 user; không dùng chế độ `--tuning-max-users 0`
(source hiện gắn nhãn experiment SAMPLE, không phù hợp cho full tuning).

Thay đổi trong Phase A:

- `spark/tv3/smoke_test.py`: thêm runtime, coverage, kiểm tra phim/user không có
  factor, predictions hợp lệ, metadata và loại train history. Giữ nguyên mẫu,
  seed và tham số ALS.
- `spark/tv3/tune_als.py`: ghi evidence experiment FAILED rồi raise, không chạy
  tiếp cấu hình sau lỗi. Giữ nguyên tham số/sampling/split/selection.
- Tạo báo cáo này; cập nhật README và chú thích issues report cũ.

Static check: `py_compile` và import cả 13 module PASS trong Spark container.
Compiled cache nằm riêng trong `/tmp`; runtime dùng PYTHONDONTWRITEBYTECODE=1.
Kiểm tra mock failure PASS: callback ghi đúng một FAILED result, fit được gọi
một lần, experiment thứ hai không khởi động. `git diff --check` PASS.
Không có lỗi source import; lần compile đầu vướng quoting của PowerShell,
đã chạy lại thành công qua stdin, không đổi môi trường hay cài dependency.

## 2. ENVIRONMENT

- Docker: cả 7 containers running, giữ custom Spark image.
- Compose luôn kiểm tra bằng `-f docker-compose.yml -f docker-compose.tv3.yml`.
- Spark: 3.5.9; NumPy: 1.26.4; không có lỗi import pyspark.ml/ALS.
- HDFS: HEALTHY, 3/3 DataNodes, safe mode OFF.
- Sau tuning: 188 validated blocks, missing/corrupt/under-replicated blocks 0,
  missing replicas 0, average replication 3.0.
- Cuối phiên: 2 Spark workers ALIVE, active applications 0.
- Không chạy lại TV1/TV2, không restart container, Docker Desktop hay WSL.

## 3. SMOKE TEST

Entry point: `spark/tv3/smoke_test.py`, exit code 0, status PASS.
Application: `app-20261004172032-0007`, FINISHED.

| Chỉ số runtime | Giá trị |
|---|---:|
| Selected users | 200 (200 userId nhỏ nhất trong train) |
| Train sample rows | 24.206 |
| Test sample rows | 6.143 |
| Valid predictions | 5.562 |
| Dropped rows | 581 |
| Unknown-user test rows | 0 |
| Unknown-movie test rows | 581 |
| Invalid predictions | 0 |
| Recommendations | 15 (5 user × Top-3) |
| Invalid metadata rows | 0 |
| Recommendations trùng train history | 0 |
| Runtime trong script | 47,756541 giây |
| Spark master application duration | 46,514 giây |

Load train/test/movies, ALS.fit, prediction, cold-start drop, recommendation và
metadata join đều đã được thực thi. 581 dropped rows khớp số dòng phim không
có factor; không có user cold-start. Không ghi output production.
Không ghi nhận memory error hoặc executor loss trong smoke test.

Command đã chạy:

```powershell
docker exec -e PYTHONDONTWRITEBYTECODE=1 spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 --deploy-mode client `
  --driver-memory 1g --executor-memory 1536m `
  --conf spark.driver.host=spark-master `
  --conf spark.driver.bindAddress=0.0.0.0 `
  --conf spark.cores.max=4 --conf spark.executor.cores=2 `
  --conf spark.sql.shuffle.partitions=64 `
  /opt/spark-apps/tv3/smoke_test.py
```

## 4. SAMPLE TUNING

HDFS TV3 output chưa tồn tại trước Phase A. Đã chạy entrypoint hiện có:
`main.py --tuning-only --tuning-max-users 10000`, với cùng Spark resource
options như smoke test. Exit code 0, `TV3 TUNING-ONLY PASSED`.
Application `app-20261004172230-0008`, FINISHED, duration 61,686 giây.

- Status: TUNING_COMPLETE; evaluation_scope: SAMPLE.
- Sampling: distinct userId, sort `xxhash64(userId, lit(42))`, tie-break userId,
  limit 10.000; strategy gốc không thay đổi.
- Sample users: 10.000; sampled TV2 train rows: 1.243.712.
- Train-core: 1.123.778; validation: 119.934; seed 42, requested ratio 0.1.
- Validation chỉ tách từ sampled TV2 train. TV2 test được kiểm tra contract,
  không được dùng cho prediction/metrics/lựa chọn hyperparameter trong tuning.

| Config | rank | maxIter | regParam | Validation RMSE | Validation MAE | Fit sec | Eval sec |
|---|---:|---:|---:|---:|---:|---:|---:|
| A | 10 | 5 | 0.10 | 0.8272565507 | 0.6426270238 | 8.668591 | 4.626357 |
| B | 10 | 8 | 0.10 | 0.8172661386 | 0.6316896571 | 5.946098 | 2.358351 |
| C | 10 | 8 | 0.05 | 0.8146315545 | 0.6199775921 | 5.036182 | 1.901597 |

Cả ba PASS, mỗi experiment có 118.852 valid predictions / 119.934 validation
rows; dropped 1.082 (0,902163%). Kết quả khớp máy cũ ở độ chính xác được cung
cấp. Best ON SAMPLE: **C, rank=10, maxIter=8, regParam=0.05**.
Không gọi đây là cấu hình tốt nhất trên full MovieLens.

Persisted outputs:

```text
/project/movielens/output/tv3/metrics/experiments
/project/movielens/output/tv3/manifest
```

Đọc lại manifest bằng HDFS: TUNING_COMPLETE, selection als_c, SAMPLE.
Đọc lại Parquet độc lập bằng Spark SQL: đúng 3 rows, 3/3 PASS/SAMPLE/validation,
coverage sums đúng, metrics và params khớp manifest. Không chỉ dựa vào path
tồn tại. Không có model/final metrics/Top-K/production recommendation output.
Không ghi nhận memory error hoặc executor loss trong sample tuning.

## 5. RESOURCE

Docker engine memory: 8.184.647.680 bytes = khoảng 7,62 GiB. Các giới hạn
7,623 GiB xuất hiện trên từng dòng docker stats là cùng budget Docker/WSL,
không phải RAM độc lập cấp cho mỗi container.

| Container | Sau tất cả jobs (MiB) | Snapshot khi tuning |
|---|---:|---:|
| namenode | 388.7 | 386.4 MiB |
| datanode1 | 293.5 | 290.4 MiB |
| datanode2 | 316.7 | 317.0 MiB |
| datanode3 | 288.0 | 286.9 MiB |
| spark-master | 262.2 | 1.155 GiB (gồm driver) |
| spark-worker1 | 92.66 | 1.089 GiB (gồm executor) |
| spark-worker2 | 92.40 | 1.407 GiB (gồm executor) |

Hadoop cuối phiên khoảng 1,26 GiB; Spark idle khoảng 0,44 GiB.
Trong snapshot tuning, Spark khoảng 3,65 GiB, Hadoop khoảng 1,25 GiB.
Đây là snapshot, không phải peak được đo liên tục. Docker/WSL MemAvailable
khi tuning khoảng 2,43 GiB; sau jobs khoảng 5,49 GiB. Swap dùng khoảng 0,73 GiB,
đã có swap usage trước smoke test; không quy toàn bộ cho ALS.

- Workers: 2 × (2 cores, advertised memory 2.048 MiB).
- Smoke/tuning: 2 executors × 1.536 MiB heap × 2 cores; driver heap 1 GiB.
- Host RAM: khoảng 15,73 GiB; available khoảng 3,65 GiB trong audit sau tuning.
- Disk trong Docker: overlay khoảng 945 GiB available.
- Ổ D thực: 159.083.409.408 bytes free, khoảng 148,16 GiB.
  Không coi dung lượng virtual filesystem Docker là dung lượng vật lý của D.

## 6. FULL ALS READINESS

**SAFE TO TRY FULL TRAIN: NO — chưa đủ bằng chứng an toàn.**

Smoke fit chỉ 24.206 rows; sample tuning fit 1.123.778 rows. Full train có
25.520.897 rows, khoảng 22,7 lần train-core. Không thể ngoại suy RAM/thời gian
theo hệ số đơn giản. Mỗi executor chỉ có 1.536 MiB heap; còn JVM/Python/native
overhead, Hadoop, driver và OS. TV2 trước đây đã thiếu execution memory, và
full ALS trên máy cũ từng mất workers. Thành công của sample không loại bỏ
những rủi ro này.

Nếu người dùng sau đó chấp thuận một lần thử có kiểm soát, cấu hình khởi đầu
thận trọng là giữ executor 1.536 MiB/driver 1 GiB, hạ concurrency xuống một
core/executor, tổng 2 cores, shuffle/default parallelism 128 và không coalesce
shuffle partitions. Các option này giảm số task đồng thời và kích thước một
số SQL partitions; chúng không thay đổi rank/seed/TV2 split, và không bảo đảm
ALS internals vừa RAM. Không tăng executor vượt worker budget hiện tại.
Cần kiểm tra lại RAM trước lần thử và dừng khi memory error/executor loss.
Không tự áp dụng thay đổi Compose hay Docker memory trong Phase A.

## 7. GIT

Branch `recommendation-model-evaluation`; không commit/push/merge.
Git status cuối Phase A:

```text
 M data/ml-32m/README.txt
 M data/ml-32m/checksums.txt
 M docs/TV3_CURRENT_ISSUES_REPORT.md
 M docs/TV3_INPUT_AUDIT.md
 M spark/tv3/README.md
 M spark/tv3/smoke_test.py
 M spark/tv3/tune_als.py
?? data/ml-32m-before-download-20261004-233138/
?? docker-compose.tv3.yml
?? docs/TV3_NEW_MACHINE_INPUT_RECOVERY.md
?? docs/TV3_PHASE_A_COMPLETION_REPORT.md
?? spark/tv3/input_gate.py
```

Các thay đổi dataset, backup directory, Compose TV3, input audit/recovery và
input_gate có từ trước Phase A, được giữ nguyên. TV1/TV2 không chạy lại và
không sửa source. Hai Compose files giữ nguyên trong phiên.

`git diff -- spark/tv1 spark/tv2`: **empty**.
`git diff --check`: **PASS**, chỉ có warning line endings ở hai dataset docs
đã modified từ trước. Không xóa/hoàn nguyên thay đổi của người dùng.

## 8. NEXT STEP

Checkpoint: DỪNG. Candidate final đã được tái xác nhận trên sample mới.
Full final fit chưa được thực hiện; không có final test RMSE/MAE.
Lệnh dưới chỉ là đề xuất cho lần thử sau khi người dùng xác nhận và resource
được xem xét lại. **DO NOT EXECUTE trong Phase A.**

```powershell
docker exec -e PYTHONDONTWRITEBYTECODE=1 spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 --deploy-mode client `
  --driver-memory 1g --executor-memory 1536m `
  --conf spark.driver.host=spark-master `
  --conf spark.driver.bindAddress=0.0.0.0 `
  --conf spark.cores.max=2 --conf spark.executor.cores=1 `
  --conf spark.sql.shuffle.partitions=128 `
  --conf spark.default.parallelism=128 `
  --conf spark.sql.adaptive.coalescePartitions.enabled=false `
  /opt/spark-apps/tv3/final_train.py
```

Command gọi `final_train.py`, không gọi `main.py`; chỉ fit một candidate
rank=10/maxIter=8/regParam=0.05, seed 42, FULL TV2 train/test. Entrypoint ghi
model/final metrics và overwrite manifest trong TV3. Tuning metrics vẫn nằm
riêng tại experiments; kết quả tuning hiện cũng được ghi trong báo cáo này.
