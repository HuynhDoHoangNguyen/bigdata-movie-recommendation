# Báo cáo hiện trạng và các vấn đề còn lại của thành viên 3 (TV3)

> Báo cáo lịch sử của máy cũ. Trạng thái mới ngày 05/10/2026 (Asia/Saigon):
> [Phase B completion report](TV3_PHASE_B_COMPLETION_REPORT.md) và
> [TV4 handoff](TV3_HANDOFF_TO_TV4.md): SAMPLE model/output đã verify PASS với
> status SAMPLE_COMPLETE_WARNING; Top-K bằng 0 là hạn chế chất lượng. Full ALS
> chưa chạy. Các blocker SAMPLE cũ dưới đây không còn là trạng thái mới nhất.
> [Phase A completion report](TV3_PHASE_A_COMPLETION_REPORT.md). Smoke test và
> tuning sample trên máy mới đã PASS; full ALS chưa chạy. Resource và Git status
> trong tài liệu này không đại diện cho máy mới.

Ngày kiểm tra: 03/10/2026 (Asia/Bangkok)

## 1. Kết luận ngắn

TV3 **chưa hoàn thành toàn bộ**. Phần đọc dữ liệu, kiểm tra input, smoke test,
tuning ALS trên mẫu xác định và lựa chọn tham số đã hoàn thành. Trạng thái runtime
hiện tại là `TUNING_COMPLETE`, với `evaluation_scope=SAMPLE`.

Phần còn thiếu để bàn giao TV3 hoàn chỉnh là model final, đánh giá final trên TV2
test, Top-N recommendation, kiểm tra output cuối và tài liệu handoff cho TV4.
Nguyên nhân chặn chính là giới hạn RAM của Docker/WSL và việc full-data ALS trước
đây từng làm Spark worker mất kết nối.

Không được trình bày kết quả tuning mẫu như kết quả của toàn bộ MovieLens 32M.

## 2. Những phần đã hoàn thành

| Hạng mục | Trạng thái | Bằng chứng |
|---|---|---|
| Docker và HDFS | PASS | 7 container đang chạy, 3/3 DataNode, safe mode OFF |
| Input TV2 | PASS | Train 25.520.897 rows, test 6.479.307 rows |
| Spark ML environment | PASS | Spark/PySpark 3.5.9, NumPy 1.26.4, ALS import được |
| Static check | PASS | Toàn bộ Python TV3 compile được; `git diff --check` sạch |
| Smoke test | PASS | 24.206 train rows, 6.143 test rows, 5.562 valid predictions, 15 recommendations |
| Tuning sample | PASS | 10.000 deterministic users, 1.243.712 ratings |
| Persist experiment metrics | PASS | Ba kết quả A/B/C đã được ghi riêng vào HDFS |
| Memory-safe final entrypoint | READY, NOT RUN | `spark/tv3/final_train.py` |

Best configuration **trên deterministic tuning sample**:

```text
rank = 10
maxIter = 8
regParam = 0.05
validation RMSE = 0.8146315545
validation MAE = 0.6199775921
```

Đây không phải là tuyên bố tham số tốt nhất cho toàn bộ MovieLens 32M.

## 3. Trạng thái HDFS TV3 hiện tại

Hiện chỉ có:

```text
/project/movielens/output/tv3/metrics/experiments
/project/movielens/output/tv3/manifest
```

Manifest hiện tại:

```text
status = TUNING_COMPLETE
evaluation_scope = SAMPLE
```

Các output final chưa tồn tại:

```text
/project/movielens/output/tv3/model/als_best
/project/movielens/output/tv3/metrics/final
/project/movielens/output/tv3/metrics/top_k
/project/movielens/output/tv3/recommendations/topn
```

Vì vậy, chạy `verify_outputs.py` ở thời điểm này sẽ thất bại đúng thiết kế do chưa
có model và output final.

## 4. Các vấn đề đang ngăn TV3 hoàn thành

### P1 — Chưa có full-data model

Mức độ: **BLOCKER**

ALS chưa được fit thành công trên toàn bộ 25.520.897 train rows. Lần chạy cũ đã
làm worker mất kết nối. Phiên hiện tại cố ý không chạy lại full-data để bảo vệ
Docker/HDFS.

### P2 — Resource hiện tại không đủ an toàn

Mức độ: **BLOCKER**

```text
Host physical RAM       ≈ 11,73 GiB
Host available RAM      ≈ 2,44 GiB tại thời điểm audit
Docker/WSL memory       ≈ 5,65 GiB tổng
Spark workers           = 2
Worker memory           = 2 GiB/worker
Worker cores            = 2/worker
Executor memory         = 1536 MiB/executor
Total Spark cores       = 4
```

Docker/WSL 5,65 GiB còn phải chứa NameNode, ba DataNode, Spark Master, driver và
hai worker. Profile mong muốn 2 worker × 4 GiB không thể áp dụng trong giới hạn
hiện tại.

### P3 — Nguyên nhân worker loss chưa được chứng minh hoàn toàn

Mức độ: **HIGH RISK**

Không tìm thấy `OutOfMemoryError`, `OOMKilled` hoặc `ExecutorLostFailure` rõ ràng.
Các executor log cũ kết thúc đột ngột giữa task/shuffle và có đoạn bị truncate.

Phân loại:

```text
VERIFIED ROOT CAUSE = UNKNOWN
LIKELY ROOT CAUSE   = áp lực tổng hợp RAM/I/O của Docker/WSL và ALS shuffle
CONFIDENCE          = MEDIUM
```

Do không có bằng chứng OOM trực tiếp, không nên khẳng định lỗi chắc chắn là OOM.

### P4 — Chưa có final test metrics

Mức độ: **BLOCKER CHO BÁO CÁO MÔ HÌNH**

TV2 test chưa được dùng cho final evaluation. Các số RMSE/MAE hiện có chỉ là
validation metrics trên sample. Final evaluation còn phải báo cáo:

```text
test_rows_total
valid_prediction_rows
dropped_rows
dropped_percentage
RMSE
MAE
training_time_sec
evaluation_time_sec
```

Cold-start đã biết từ TV2: unknown users = 0, unknown movies = 4.178 và 4.729
test rows bị ảnh hưởng. Runtime final vẫn phải đo lại và ghi nhận thực tế.

### P5 — Chưa có Top-N và output cho TV4

Mức độ: **CHƯA HOÀN THÀNH CHỨC NĂNG**

Chưa materialize recommendation output gồm:

```text
userId, movieId, title, genres, prediction, rank
```

Top-N phải loại phim user đã rating trong train, join metadata, xử lý user không
tồn tại/thiếu metadata và ghi rõ phạm vi user demo hoặc full.

### P6 — Chưa chạy verifier cuối

Mức độ: **BLOCKER CHO NGHIỆM THU**

`spark/tv3/verify_outputs.py` đã tồn tại nhưng chỉ nên chạy sau khi model, final
metrics và recommendations đã được tạo. Hiện verifier chưa thể PASS.

### P7 — Git worktree chưa được đóng gói/bàn giao

Mức độ: **MEDIUM**

`docker-compose.yml` đang modified và toàn bộ `docker/spark`, `spark/tv3`, tài
liệu TV3 đang là file mới chưa được commit. Không có lỗi `diff --check`, nhưng cần
review và commit trước khi bàn giao nhóm.

### P8 — Chưa có tài liệu handoff TV4

Mức độ: **MEDIUM**

Chưa có `docs/TV3_HANDOFF_TO_TV4.md`. Tài liệu này phải ghi schema/path output,
scope thực tế (`FULL` hoặc `SAMPLE`), metrics runtime và cách đọc recommendation.

## 5. Cải tiến source đã chuẩn bị

`spark/tv3/final_train.py` đã được tạo để tách final job khỏi tuning và Top-N:

```text
validate full train/test
→ train đúng một ALS model
→ save model
→ evaluate đúng một lần trên TV2 test
→ save final metrics và manifest
```

Job này cố định cấu hình tốt nhất trên tuning sample và không tự chạy Top-N.
`evaluate.py` persist predictions bằng `DISK_ONLY` trong lúc tính count/RMSE/MAE,
sau đó unpersist, tránh transform lại nhiều lần.

Source hiện không dùng `collect()` trên full train, `toPandas()`, user×movie cross
join hoặc materialize all-user recommendations trong final training job.

## 6. Hai phương án để hoàn thành TV3

### Phương án A — Full-data chính thức

Đây là phương án cho kết quả đầy đủ nhất, nhưng chưa an toàn trên resource hiện tại.

1. Tăng RAM khả dụng của host và giới hạn Docker/WSL.
2. Mục tiêu tối thiểu đề xuất: Docker khoảng 9–10 GiB; hai worker khoảng 4 GiB
   mỗi worker; vẫn phải chừa RAM cho Hadoop, master và driver.
3. Chỉ restart ba container Spark, không restart Hadoop.
4. Chạy smoke test và một sample validation trước.
5. Chạy đúng một lần `final_train.py` với rank 10, maxIter 8, regParam 0.05.
6. Nếu worker mất kết nối, dừng; không lặp lại liên tục.
7. Sau khi model/evaluation PASS, chạy Top-N demo riêng.
8. Chạy `verify_outputs.py` và lập handoff TV4.

Máy hiện chỉ có 11,73 GiB RAM vật lý và khoảng 2,44 GiB còn trống tại lúc audit,
nên mục tiêu Docker 9–10 GiB có thể vẫn không thực tế nếu không giải phóng hoặc
nâng cấp RAM host.

### Phương án B — Sample/demo model

Nếu giảng viên/nhóm chấp nhận giới hạn máy cá nhân, có thể hoàn thành deliverable
theo phạm vi sample:

1. Train final demo model trên deterministic 10.000-user sample.
2. Evaluate trên phần TV2 test tương ứng cùng tập user.
3. Export Top-N cho tập user demo xác định.
4. Manifest phải ghi `evaluation_scope=SAMPLE` và status `WARNING`.
5. Tài liệu phải ghi rõ full MovieLens 32M training chưa hoàn thành.
6. Điều chỉnh verifier/handoff để chấp nhận scope SAMPLE nhưng không đánh tráo với FULL.

Phương án B phù hợp để demo chức năng, nhưng không được gọi là full MovieLens model.

## 7. Thứ tự công việc còn lại được khuyến nghị

| Thứ tự | Công việc | Điều kiện hoàn thành |
|---:|---|---|
| 1 | Nhóm chọn FULL hay SAMPLE/demo | Có quyết định scope chính thức |
| 2 | Nếu FULL: xử lý giới hạn RAM trước | Spark resource smoke test PASS |
| 3 | Train một final model | Model load lại được từ HDFS |
| 4 | Evaluate final | Có đủ coverage, RMSE, MAE và runtime |
| 5 | Sinh Top-N demo riêng | Không chứa phim đã rating; metadata hợp lệ |
| 6 | Chạy verifier | Status PASS hoặc WARNING đúng scope |
| 7 | Viết handoff TV4 | Path, schema, counts, metrics và limitation đầy đủ |
| 8 | Review/commit Git | Không còn thay đổi ngoài phạm vi hoặc file sinh tự động |

## 8. Tiêu chí để tuyên bố TV3 hoàn thành

TV3 chỉ nên được đánh dấu hoàn thành khi tất cả mục sau đều đạt:

- Có model ALS đã save và load lại thành công.
- Có final metrics với scope minh bạch.
- Có report cold-start coverage.
- Có Top-N recommendation đã loại train history và join metadata.
- Manifest khớp output thực tế.
- `verify_outputs.py` PASS, hoặc WARNING có lý do SAMPLE rõ ràng.
- Có `docs/TV3_HANDOFF_TO_TV4.md`.
- Source và tài liệu đã được review/commit.
- Không làm thay đổi hoặc ghi đè output TV1/TV2.

## 9. Đánh giá tiến độ hiện tại

```text
Implementation nền tảng       : Hoàn thành
Input validation              : Hoàn thành
Smoke test                    : Hoàn thành
Sample tuning                 : Hoàn thành
Full/sample final model       : Chưa hoàn thành
Final test evaluation         : Chưa hoàn thành
Top-N materialization         : Chưa hoàn thành
Final verification            : Chưa hoàn thành
TV4 handoff                   : Chưa hoàn thành
```

Đánh giá tổng thể: **TV3 đã hoàn thành phần phát triển và tuning mẫu, nhưng chưa
hoàn thành phần output/model cuối để nghiệm thu và bàn giao TV4.**

## 10. Khuyến nghị quyết định tiếp theo

Với máy hiện tại, không nên chạy full-data ngay. Quyết định cần thiết từ nhóm là:

- cấp máy/cluster có nhiều RAM hơn để thực hiện phương án A; hoặc
- chính thức chấp nhận phương án B và ghi rõ đây là sample/demo model.

Sau khi có quyết định scope, các bước còn lại có thể triển khai mà không chạy lại
TV1, TV2 hoặc download MovieLens.
