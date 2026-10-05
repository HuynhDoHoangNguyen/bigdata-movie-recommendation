# TV3 → TV4: SAMPLE recommendation handoff

Ngày bàn giao: 05/10/2026 (Asia/Saigon).
**SAMPLE_COMPLETE_WARNING**. Model/sample evaluation và output contract đã
verify PASS; full MovieLens ALS chưa được train. Top-K trên 500 user có 0 hit,
vì vậy bộ output này dùng để demo chức năng, chưa chứng minh chất lượng ranking.

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
| Final sample test metrics | `/project/movielens/output/tv3/metrics/final` |
| Top-K sample metrics | `/project/movielens/output/tv3/metrics/top_k` |
| Current handoff manifest | `/project/movielens/output/tv3/manifest` |
| Exact model sample users | `/project/movielens/output/tv3/scope/sample_users` |
| Exact demo users | `/project/movielens/output/tv3/scope/demo_users` |
| Preserved A/B/C experiment metrics | `/project/movielens/output/tv3/metrics/experiments` |
| Preserved Phase A tuning manifest | `/project/movielens/output/tv3/audit/tuning_manifest` |

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

recommendations = spark.read.parquet(
    "hdfs://namenode:9000/project/movielens/output/tv3/recommendations/topn"
)
demo_users = recommendations.select("userId").distinct().orderBy("userId")
user_recommendations = recommendations.where(F.col("userId") == 806).orderBy("rank")
user_recommendations.show(10, truncate=False)
```

Đọc/caching 200 dòng demo ở backend khởi động là đủ cho dashboard. UI request
chỉ lọc/sắp xếp artifact này; không import ALS.fit, không chạy spark-submit
training và không chạy `recommend.py` mỗi lần mở trang. Kết nối HDFS cần backend
ở Docker network project hoặc có kết nối mạng phù hợp; browser không đọc HDFS
trực tiếp qua RPC. Không đổi Compose dùng chung để tích hợp trong phiên này.

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

Không gọi kết quả ranking này là tốt chỉ vì RMSE thấp. Nguyên nhân 0 hit chưa
được xác định trong Phase B; cải thiện ranking là công việc tiếp theo cần phạm
vi riêng, không chỉnh metric hoặc tuning để ép kết quả đẹp hơn.

## Verify / tái tạo

Phase C đã hoàn tất: [ranking diagnostic report](TV3_RANKING_DIAGNOSTIC_REPORT.md).
Audit metric, movieId và filtering PASS; saved SAMPLE ALS xếp các phim ít dữ
liệu train nhưng có score cao lên đầu. Trên cùng 500 users, popularity train-only
có Precision@10=0.1252, Recall@10=0.100133; ALS vẫn 0/0. Đây là diagnostic riêng,
không thay model, metrics, Top-N hoặc manifest handoff. TV4 tiếp tục giữ cảnh báo
chất lượng khi dùng output demo hiện tại.

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
