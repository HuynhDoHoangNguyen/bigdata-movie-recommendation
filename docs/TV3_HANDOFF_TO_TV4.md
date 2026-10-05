# Bàn giao công việc TV3 cho TV4 — Dashboard / Web Demo SAMPLE

Ngày bàn giao: 05/10/2026 (Asia/Saigon).
Bên bàn giao: **Thành viên 3**. Bên tiếp nhận: **Thành viên 4**.
**SAMPLE_COMPLETE_WARNING**. Model/sample evaluation và output contract đã
verify PASS; full MovieLens ALS chưa được train. Top-K trên 500 user có 0 hit,
vì vậy raw output dùng để demo chức năng. Phase D đã verified output reranked
riêng: cải thiện so với raw ALS, nhưng vẫn thấp hơn popularity baseline.
Giới hạn SAMPLE và confirmatory test được giữ rõ trong phần Phase D bên dưới.

## Nội dung bàn giao và trách nhiệm tiếp nhận

**TV3 đã hoàn tất phạm vi SAMPLE được duyệt; TV4 có thể tiếp nhận để xây dựng
Dashboard/Web Demo. Full ALS trên toàn bộ MovieLens 32M chưa được chạy.**

| TV3 đã bàn giao, verified | TV4 tiếp nhận triển khai |
|---|---|
| Saved final SAMPLE ALS model và rating metrics | Hiển thị thông tin model, sample scope, RMSE/MAE/coverage |
| Raw Top-10 demo cho 20 users | Hiển thị kết quả raw để đối chiếu |
| Reranked Top-10 demo cho cùng 20 users | Tích hợp danh sách reranked đã materialize, sort theo rank |
| Ranking metrics raw/reranked/popularity | Biểu đồ hoặc bảng so sánh, giữ nguyên số liệu thực tế |
| User scopes, output schemas, config lock và evidence | Backend đọc đúng schema, selector đúng demo users |
| Verifier PASS và HDFS integrity tại checkpoint Phase D | Kiểm tra dữ liệu đầu vào trên môi trường TV4 trước demo |

Tài liệu này là đầu mối bàn giao chính. Các reports/JSON evidence liên kết bên
dưới cung cấp chi tiết kiểm chứng; không cần chạy lại training để nhận kết quả.
TV4 chưa được triển khai hoặc nghiệm thu bởi TV3 trong tài liệu này.

## Thứ tự tiếp nhận nhanh cho TV4

1. Kiểm tra môi trường Docker/HDFS và xác nhận các artifact paths có đủ dữ liệu.
2. Backend đọc `recommendations/topn_reranked` và `recommendations/topn`;
   mỗi dataset có 200 rows cho cùng 20 users. Cache dữ liệu demo khi khởi động.
3. Tạo selector từ demo user IDs; mặc định có thể chọn user `806`.
4. Với mỗi user, hiển thị rank, movieId, title, genres; thêm raw ALS score và
   adjusted score nếu cần giải thích kết quả. Luôn sort theo `rank`.
5. Đọc rating/ranking metrics và hiển thị so sánh ba phương pháp. Popularity
   hiện chỉ có metrics baseline, chưa có Top-N popularity artifact để phục vụ UI.
6. Ghi rõ trên dashboard/báo cáo: SAMPLE, 20 demo users, full ALS chưa chạy,
   test Phase D là confirmatory và popularity vẫn tốt hơn reranked ALS.
7. Hoàn thành checklist tiếp nhận ở cuối tài liệu và kiểm tra demo đầu cuối.

Output reranked là kết quả TV3 bổ sung sau cải thiện ranking; raw output được
giữ để đối chiếu. Không lựa chọn hoặc điều chỉnh lại config từ kết quả test.

## Môi trường và dữ liệu trên máy TV4

Môi trường đã chạy/verify: Spark 3.5.9, Hadoop 3.5.0, Docker Desktop;
1 Spark Master, 2 Workers, 1 NameNode, 3 DataNodes. Kiểm tra Compose luôn dùng
cả cấu hình nhóm và override TV3:

```powershell
docker info
docker compose -f docker-compose.yml -f docker-compose.tv3.yml ps
docker exec namenode hdfs dfsadmin -report
docker exec namenode hdfs dfsadmin -safemode get
docker exec namenode hdfs dfs -ls /project/movielens/output/tv3/recommendations/topn
docker exec namenode hdfs dfs -ls /project/movielens/output/tv3/recommendations/topn_reranked
```

Backend cần truy cập Docker network chứa `namenode` và DataNodes để đọc URI
`hdfs://namenode:9000`; kiểm tra tên network thực tế trên môi trường TV4.
Browser gọi backend của TV4, không trực tiếp đọc HDFS RPC. Ví dụ PySpark ở dưới
giả định backend đã có Spark session kết nối đúng môi trường.

**Clone/pull Git chỉ mang source/config/docs; không mang theo HDFS volumes,
saved ALS model hoặc recommendation Parquet.** Nếu TV4 dùng máy khác, cần nhận
artifacts thực tế/backup HDFS từ TV3 hoặc truy cập HDFS chung. JSON trong docs
là evidence metrics, không thay thế model hoặc 200 recommendation rows.
Khi artifact thiếu, phối hợp TV3 để bàn giao dữ liệu; không tự chạy lại TV1/TV2,
training hoặc Phase D để dựng dashboard. Không thay Compose nhóm, reset HDFS
hoặc xóa volumes trong quá trình tiếp nhận.

## Scope và kết quả thực tế

| Hạng mục | Runtime mới |
|---|---:|
| Model scope / evaluation scope | SAMPLE / SAMPLE |
| Recommendation scope | SAMPLE_DEMO |
| Sample users | 10.000 |
| Sample train ratings | 1.243.712 |
| Sample TV2 test ratings | 315.869 |
| Valid predictions | 313.351 |
| Dropped predictions | 2.518 (0,797166%) |
| Final sample test RMSE | 0.8146154253125608 |
| Final sample test MAE | 0.6200873149304171 |
| Training runtime | 9.186321 giây |
| Rating evaluation runtime | 2.478177 giây |
| Demo recommendations | 200 (20 users × Top-10) |

Tham số `rank=10, maxIter=8, regParam=0.05, seed=42`, explicit feedback,
`coldStartStrategy="drop"`. Candidate được chọn từ validation tuning, không
từ TV2 test. Final model fit trên toàn bộ sampled TV2 train, gồm cả phần từng
được dùng làm validation tuning; final metrics dùng matching sampled TV2 TEST.
Không dùng validation metrics làm final test metrics.

Sampling: distinct userId từ TV2 train, sort theo `xxhash64(userId, lit(42))`,
tie-break userId, limit 10.000. Tập user này khớp tuning Phase A và đã được lưu
riêng để đọc lại/kiểm chứng. Model reload có 10.000 user factors và 27.941 item
factors, không ID trùng và không feature null/NaN/Infinity.
Sample cold-start: 0 unknown users, 2.337 unknown movies, ảnh hưởng 2.518 test
rows. Các RMSE/MAE ở trên chỉ tính trên predictions hợp lệ; coverage 99,202834%.

## HDFS paths

Dùng prefix `hdfs://namenode:9000` khi chạy trong Docker network của project.

| Artifact | HDFS path |
|---|---|
| Saved SAMPLE model | `/project/movielens/output/tv3/model/als_best` |
| Materialized demo recommendations | `/project/movielens/output/tv3/recommendations/topn` |
| Verified reranked demo recommendations | `/project/movielens/output/tv3/recommendations/topn_reranked` |
| Final sample test metrics | `/project/movielens/output/tv3/metrics/final` |
| Top-K sample metrics | `/project/movielens/output/tv3/metrics/top_k` |
| Current handoff manifest | `/project/movielens/output/tv3/manifest` |
| Exact model sample users | `/project/movielens/output/tv3/scope/sample_users` |
| Exact demo users | `/project/movielens/output/tv3/scope/demo_users` |
| Preserved A/B/C experiment metrics | `/project/movielens/output/tv3/metrics/experiments` |
| Preserved Phase A tuning manifest | `/project/movielens/output/tv3/audit/tuning_manifest` |
| Phase D validation ranking metrics | `/project/movielens/output/tv3/metrics/reranking_validation` |
| Phase D confirmatory test ranking metrics | `/project/movielens/output/tv3/metrics/reranking_test` |
| Phase D locked reranking config | `/project/movielens/output/tv3/diagnostics/reranking_phase_d/config_lock` |
| Phase D confirmation report | `/project/movielens/output/tv3/diagnostics/reranking_phase_d/test_report` |
| Phase D verification report | `/project/movielens/output/tv3/diagnostics/reranking_phase_d/verification_report` |
| Temporary selection model, không dùng cho dashboard | `/project/movielens/output/tv3/diagnostics/reranking_phase_d/selection_model` |

Current manifest:

```text
status = SAMPLE_COMPLETE_WARNING
model_scope = SAMPLE
evaluation_scope = SAMPLE
recommendation_scope = SAMPLE_DEMO
```

Manifest includes actual counts, params, cold-start, factor reload checks,
recommendation checks, Top-K metrics and preserved tuning evidence. Không trình
bày trạng thái này là FULL_COMPLETE hoặc metrics của full MovieLens 32M.

## Recommendation contract

```text
userId     INT
movieId    INT
title      STRING
genres     STRING
prediction DOUBLE
rank       INT
```

Có đúng 10 dòng cho mỗi demo user, rank 1–10. Đã verify critical nulls,
NaN/Infinity, duplicate user/movie, duplicate user/rank và train-history overlap
đều bằng 0. Title/genres khớp STANDARD movies. Prediction là điểm ALS ước lượng,
không phải xác suất; không giả định nó bị giới hạn trong [0.5, 5.0].

Demo user IDs:

```text
806, 9770, 13769, 54836, 56041, 71795, 100346, 103252, 103269, 108505,
113006, 115392, 126896, 128517, 131122, 143637, 172400, 179925, 183649, 192892
```

Demo selection dùng cùng hash/seed trên model sample và limit 20. Dashboard
nên đưa danh sách 20 user này vào selector. User không có trong artifact demo
được báo "chưa có recommendation demo cho user này"; không tự train hay fit ALS.
Có 10.000 model users nhưng chỉ 20 user có Top-N đã materialize.

## Cách TV4 đọc output

Backend TV4 chỉ đọc Parquet đã có. Ví dụ với Spark được tạo bởi ứng dụng TV4:

```python
from pyspark.sql import functions as F

raw_recommendations = spark.read.parquet(
    "hdfs://namenode:9000/project/movielens/output/tv3/recommendations/topn"
)
reranked_recommendations = spark.read.parquet(
    "hdfs://namenode:9000/project/movielens/output/tv3/recommendations/topn_reranked"
)
demo_users = reranked_recommendations.select("userId").distinct().orderBy("userId")
user_recommendations = reranked_recommendations.where(
    F.col("userId") == 806
).orderBy("rank")
user_recommendations.show(10, truncate=False)

rating_metrics = spark.read.parquet(
    "hdfs://namenode:9000/project/movielens/output/tv3/metrics/final"
)
ranking_metrics = spark.read.parquet(
    "hdfs://namenode:9000/project/movielens/output/tv3/metrics/reranking_test"
)
ranking_metrics.select(
    "method", "parameter", "precision", "recall", "hit_rate", "users", "hits"
).show(truncate=False)
```

Đọc/caching 200 dòng demo ở backend khởi động là đủ cho dashboard. UI request
chỉ lọc/sắp xếp artifact này; không import ALS.fit, không chạy spark-submit
training và không chạy `recommend.py` mỗi lần mở trang. Kết nối HDFS cần backend
ở Docker network project hoặc có kết nối mạng phù hợp; browser không đọc HDFS
trực tiếp qua RPC. Không đổi Compose dùng chung để tích hợp trong phiên này.

### Mapping metrics cho dashboard

| Dataset / field | Ý nghĩa hiển thị |
|---|---|
| `metrics/final.rmse`, `mae` | Rating error trên predictions hợp lệ của final SAMPLE model |
| `metrics/final.evaluation_rows`, `valid_prediction_rows`, `dropped_rows` | Test coverage/cold-start; không suy ra ranking chất lượng từ RMSE |
| `metrics/top_k.precision_at_k`, `recall_at_k` | Raw ranking Phase B, giữ nguyên 0/0 |
| `metrics/reranking_test.method`, `parameter` | RAW_ALS/0, SHRINKAGE/50, POPULARITY/0 |
| `metrics/reranking_test.precision`, `recall`, `hit_rate` | Macro P@10/R@10/HR@10 của sample test confirmation |
| `metrics/reranking_test.users`, `hits`, `users_with_10` | Users evaluated, total hits, coverage đủ K |
| `metrics/reranking_validation` | Kết quả dùng chọn config; không trình bày như final test metrics |

Parquet Phase D dùng `method`/`parameter`; field `role` chỉ có trong JSON
confirmation report, không có trong metrics Parquet. UI có thể đặt nhãn
`SHRINKAGE/50` là "ALS + support-aware reranking". Không đổi hoặc làm đẹp metrics.

## Top-K và giới hạn chất lượng

- K=10, relevant item: sample TV2 test rating >= 4.0.
- 9.879 sample users có ít nhất một relevant item; đánh giá 500 user được chọn
  xác định bằng hash/seed. Không phải toàn bộ 10.000 user hay toàn bộ TV2 test.
- Macro Precision@10 = **0.0**, macro Recall@10 = **0.0**.
- Independent read-back audit: 5.000 recommendation, 8.569 relevant items,
  total_hits=0, users_with_hits=0. Output không rỗng.
- Ranking evaluation dùng unseen-item candidates từ Spark ALS, không tạo
  DataFrame cross join toàn bộ users × movies. Candidate pool 100/user,
  lọc train history rồi lấy Top-10. Candidate buffer hữu hạn có thể trả ít
  hơn N ở lần export khác; artifact demo hiện tại có đủ 200 dòng.
- Field cũ `eligible_users_full=9879` trong Top-K artifact là toàn bộ eligible
  users của INPUT SAMPLE truyền vào helper; không phải full MovieLens users.

Không gọi kết quả raw ranking này là tốt chỉ vì RMSE thấp. Phase C đã audit
nguyên nhân 0 hit, Phase D đã bổ sung reranking verified như bên dưới;
raw metrics vẫn được giữ nguyên để đối chiếu.

## Phase D: reranked SAMPLE demo đã verified

Ngày hoàn tất: 05/10/2026 (Asia/Saigon).
[Phase D completion report](TV3_RERANKING_PHASE_D_REPORT.md) và
[JSON evidence](TV3_RERANKING_PHASE_D_RESULTS.json).

Temporary selection model fit đúng một lần trên 1.123.778 train_core rows,
không chứa 119.934 validation rows. Reranking chọn trên 500 deterministic
validation users: **SHRINKAGE alpha=50**, primary Recall@10, rồi khóa config.
Selection dataset: `SAMPLE_VALIDATION_INDEPENDENT_OF_SELECTION_MODEL`.
Temporary model không dùng làm final output; confirmation và demo dùng saved
Phase B model. Support và global mean chỉ lấy từ train_core, không từ test.

| Sample test confirmation, cùng 500 users | Precision@10 | Recall@10 | HitRate@10 | Hits |
|---|---:|---:|---:|---:|
| RAW Phase B ALS, giữ nguyên | 0.000000 | 0.000000 | 0.000 | 0 |
| Phase B ALS + locked shrinkage | 0.090200 | 0.070510 | 0.454 | 451 |
| Popularity train_core-only baseline | 0.125400 | 0.100374 | 0.588 | 627 |

TV2 TEST đã được quan sát ở Phase B/C; đây là **confirmatory**, không phải
untouched holdout. Không retune sau test. Popularity vẫn cao hơn reranked ALS;
không dùng sample result để khẳng định chất lượng full MovieLens.
Model, final RMSE/MAE, raw Top-K, raw demo và manifest Phase B không đổi.

Output mới, **200 rows / 20 saved demo users / Top-10 mỗi user**:

```text
hdfs://namenode:9000/project/movielens/output/tv3/recommendations/topn_reranked
```

```text
userId         INT
movieId        INT
title          STRING
genres         STRING
prediction     DOUBLE
adjusted_score DOUBLE
train_support  BIGINT
rank           INT
```

`prediction` là raw ALS score của candidate; `adjusted_score` là score dùng để
xếp rank. `train_support` là số ratings/movie trong train_core. Cả hai score
không phải xác suất và không bị clip về rating range. Formula:

```text
adjusted_score = 3.5436634281859942
                + train_support / (train_support + 50)
                  * (prediction - 3.5436634281859942)
```

Verifier Phase D PASS: read-back equality, deterministic ranks, core support,
locked formula, null/non-finite rows, duplicate pairs/ranks, history overlap,
metadata mismatch và unexpected users đều đạt yêu cầu. Cùng danh sách 20 demo
users đã bàn giao ở trên. TV4 đọc dataset mới và sort theo `rank`:

```python
reranked = spark.read.parquet(
    "hdfs://namenode:9000/project/movielens/output/tv3/recommendations/topn_reranked"
)
user_recommendations = reranked.where(F.col("userId") == 806).orderBy("rank")
```

Giữ raw dataset `recommendations/topn` để đối chiếu. Handoff manifest cũ vẫn
`SAMPLE_COMPLETE_WARNING` và trỏ raw artifacts; reranked provenance/metrics/lock
nằm riêng dưới `diagnostics/reranking_phase_d` và `metrics/reranking_*`.
Không chạy training hoặc reranking stages trong dashboard request; các stages
từ chối outputs đã tồn tại. Không dùng temporary selection model thay model Phase B.

## Verify / tái tạo

Phase C đã hoàn tất: [ranking diagnostic report](TV3_RANKING_DIAGNOSTIC_REPORT.md).
Audit metric, movieId và filtering PASS; saved SAMPLE ALS xếp các phim ít dữ
liệu train nhưng có score cao lên đầu. Trên cùng 500 users, popularity train-only
có Precision@10=0.1252, Recall@10=0.100133; ALS vẫn 0/0. Đây là diagnostic riêng,
không thay model, metrics, Top-N hoặc manifest handoff. TV4 tiếp tục giữ cảnh báo
chất lượng khi dùng output demo hiện tại.

Đọc evidence Phase D đã lưu bằng lệnh read-only dưới đây; không chạy lại
`reranking_phase_d.py --stage verify` vì stage đó đã có output và từ chối overwrite:

```powershell
docker exec namenode hdfs dfs -cat /project/movielens/output/tv3/diagnostics/reranking_phase_d/verification_report/part-*
```

Verifier read-only đã reload model, dựng lại sample users từ TV2 train,
đối chiếu factors/counts/metrics, đo lại rating metrics và Top-K, kiểm tra
schema/history/metadata và xác nhận A/B/C không thay đổi. Result:
`verification_status=PASS`, `status=WARNING`, `errors=[]`.

```powershell
docker exec -e PYTHONDONTWRITEBYTECODE=1 spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 --deploy-mode client `
  --driver-memory 1g --executor-memory 1536m `
  --conf spark.driver.host=spark-master `
  --conf spark.driver.bindAddress=0.0.0.0 `
  --conf spark.cores.max=4 --conf spark.executor.cores=2 `
  --conf spark.sql.shuffle.partitions=64 `
  /opt/spark-apps/tv3/verify_outputs.py
```

Sample creation entrypoints là `final_sample.py` rồi `sample_handoff.py`, mỗi
job riêng và không tuning lại. Chúng từ chối ghi đè artifact đã tồn tại, nên
không chạy lại để xem dashboard. `recommend.py` chỉ cho phép output bên dưới
TV3/recommendations, từ chối output đã tồn tại và users ngoài saved model scope.
Đã test user -1 bị từ chối, không tạo output.

HDFS sau Phase B HEALTHY, 3/3 DataNodes, missing/corrupt blocks 0. Full-data
training vẫn chưa được phép; resource readiness từ Phase A vẫn là NO.

## Checklist TV4 tiếp nhận và nghiệm thu demo

Các ô dưới đây dành cho **TV4 tự xác nhận** sau khi tích hợp; chưa được đánh
dấu thay cho TV4. Evidence TV3 tại checkpoint Phase D: verifier PASS,
HDFS HEALTHY, 3/3 DataNodes, Safe Mode OFF, missing/corrupt blocks 0.

- [ ] Backend đọc được raw và reranked Parquet trên môi trường TV4.
- [ ] Mỗi dataset có 200 rows, 20 demo users, 10 recommendations/user.
- [ ] Selector dùng đúng demo users; user ngoài demo có thông báo phù hợp.
- [ ] User `806` hiển thị 10 phim theo rank, title/genres/movieId đúng output.
- [ ] Raw và reranked được phân biệt; không sort reranked bằng raw prediction.
- [ ] Hiển thị RMSE/MAE/test coverage và metrics raw/reranked/popularity đúng số liệu.
- [ ] Giữ raw P@10=0; ghi SAMPLE scope, confirmatory test và popularity cao hơn.
- [ ] Dashboard request chỉ đọc/cache/lọc dữ liệu; không fit ALS hoặc chạy pipelines.
- [ ] Reload backend/trang không ghi đè HDFS artifacts và không làm mất dữ liệu.
- [ ] Có hướng dẫn chạy TV4, ảnh/demo hoặc bằng chứng nghiệm thu của TV4.

## Tài liệu và giới hạn bàn giao

| Tài liệu | Nội dung |
|---|---|
| [Phase A](TV3_PHASE_A_COMPLETION_REPORT.md) | Smoke/tuning A/B/C, resource readiness |
| [Phase B](TV3_PHASE_B_COMPLETION_REPORT.md) | Final SAMPLE model, rating metrics, raw demo |
| [Phase C](TV3_RANKING_DIAGNOSTIC_REPORT.md) | Audit 0 hits và ranking diagnostic |
| [Phase D](TV3_RERANKING_PHASE_D_REPORT.md) | Temporary selection model, locked reranking, confirmation/verifier |
| [Phase D JSON](TV3_RERANKING_PHASE_D_RESULTS.json) | Exact metrics/provenance/lock/read-back evidence |
| [TV3 README](../spark/tv3/README.md) | Source entrypoints và contracts |
| [New-machine recovery](TV3_NEW_MACHINE_INPUT_RECOVERY.md) | Evidence khôi phục inputs và môi trường |

Phần đã hoàn tất: **TV3 SAMPLE, có thể bàn giao TV4**. Phần chưa thực hiện:
Full ALS 32M và dashboard/backend TV4. Không đánh đồng 10.000 model users với
20 materialized demo users; không coi kết quả là toàn bộ MovieLens 32M.
Source TV1/TV2 là read-only; model/artifacts Phase B/C được giữ để đối chiếu.
Full-data training hoặc mở rộng recommendation scope cần phạm vi công việc
riêng; không thuộc bước tiếp nhận dashboard hiện tại.
