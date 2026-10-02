# TV2 – Data Processing & Analytics

## 1. Scope

TV2 nhận Parquet Standard Layer từ TV1, xác thực hợp đồng dữ liệu, tiền xử lý bằng Spark DataFrame, tạo các bảng phân tích, chuẩn bị bộ dữ liệu ALS và chia Train/Test. TV2 không huấn luyện hay đánh giá mô hình ALS.

## 2. Current Status

- Environment: **DONE**
- TV1 Standard Layer: **VERIFIED**
- Input contract: **PASS**
- Preprocessing and analytics: **DONE**
- ALS Train/Test: **DONE**
- HDFS read-back verification: **PASS**
- End-to-end run: **PASS** on 2026-10-02

## 3. Input from TV1

TV2 chỉ đọc Standard Layer dạng Parquet qua endpoint nội bộ `hdfs://namenode:9000`.

| Dataset | HDFS path | Schema | Verified rows |
|---|---|---|---:|
| ratings | `/project/movielens/standard/ratings` | `userId int, movieId int, rating double, timestamp bigint` | 32,000,204 |
| movies | `/project/movielens/standard/movies` | `movieId int, title string, genres string` | 87,585 |
| tags | `/project/movielens/standard/tags` | `userId int, movieId int, tag string, timestamp bigint` | 2,000,072 |
| links | `/project/movielens/standard/links` | `movieId int, imdbId string, tmdbId int` | 87,585 |

Boundary validation kiểm tra schema chính xác, số dòng lịch sử, null ở cột bắt buộc và miền rating `[0.5, 5.0]`. Cả bốn dataset đều PASS với 0 dòng không hợp lệ.

## 4. TV2 Pipeline

```text
TV1 HDFS STANDARD
        ↓
Input contract validation
        ↓
Column selection/type enforcement
        ↓
ratings + movies metadata join
        ↓
Distributed EDA / analytics
        ↓
ALS schema: userId, movieId, rating
        ↓
Deterministic per-user Train/Test
        ├──→ TV3: ALS input
        └──→ TV4: analytics tables
```

Các bảng lớn được xử lý bằng Spark DataFrame và lưu Parquet nén Snappy. Pipeline không dùng Pandas hoặc vòng lặp Python qua dữ liệu ratings.

## 5. Project Structure

| File | Responsibility |
|---|---|
| `spark/tv2/config.py` | HDFS paths, expected counts, seed, split ratio và ngưỡng popularity |
| `spark/tv2/load_standard.py` | Đọc Standard Layer và kiểm tra input contract |
| `spark/tv2/preprocess.py` | Chuẩn hóa cột, join movie metadata, tạo schema ALS |
| `spark/tv2/analytics.py` | Các phép tổng hợp phân tán phục vụ EDA/TV4 |
| `spark/tv2/split_data.py` | Chia per-user có seed và kiểm tra leakage/cold-start |
| `spark/tv2/main.py` | Điều phối, ghi Parquet, read-back và ghi manifest |
| `spark/tv2/verify_outputs.py` | Đọc độc lập mọi output và xác thực schema/count |

## 6. Environment

Docker Compose cung cấp `namenode`, ba DataNode, `spark-master` và hai Spark Worker. Endpoint dùng trong container là HDFS `namenode:9000` và Spark `spark-master:7077`.

Các cổng host được đổi tối thiểu để tránh xung đột máy hiện tại:

- HDFS RPC: `localhost:19000`
- NameNode UI: `http://localhost:9870`
- Spark Master UI: `http://localhost:18080`
- Spark Worker UI: `http://localhost:18081` và `http://localhost:18082`

Việc đổi cổng host không làm thay đổi endpoint nội bộ mà các pipeline đang dùng.

## 7. How to Run

Chạy tại root repository bằng PowerShell:

```powershell
docker compose up -d
docker compose ps

.\scripts\download_movielens.ps1
.\scripts\upload_to_hdfs.ps1

docker exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --deploy-mode client `
  --conf spark.driver.host=spark-master `
  --conf spark.driver.bindAddress=0.0.0.0 `
  /opt/spark-apps/tv1/build_standard_layer.py

docker exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --deploy-mode client `
  --conf spark.driver.host=spark-master `
  --conf spark.driver.bindAddress=0.0.0.0 `
  /opt/spark-apps/tv1/verify_standard_layer.py

docker exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --deploy-mode client `
  --conf spark.driver.host=spark-master `
  --conf spark.driver.bindAddress=0.0.0.0 `
  --conf spark.executor.memory=1536m `
  --conf spark.cores.max=4 `
  --conf spark.sql.shuffle.partitions=32 `
  /opt/spark-apps/tv2/main.py
```

Kiểm tra độc lập output sau khi pipeline hoàn tất:

```powershell
docker exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --deploy-mode client `
  --conf spark.driver.host=spark-master `
  --conf spark.driver.bindAddress=0.0.0.0 `
  /opt/spark-apps/tv2/verify_outputs.py
```

`main.py` ghi đè có kiểm soát toàn bộ `/project/movielens/output/tv2`, nên có thể chạy lại cùng input và cấu hình.

## 8. Preprocessing

Standard Layer đã sạch nên TV2 không xóa dòng nào. Pipeline chọn và ép lại đúng bốn cột ratings, sau đó broadcast-join với metadata movies theo `movieId`.

| Step | Before | Removed | After | Rule |
|---|---:|---:|---:|---|
| Enforce ratings columns/types | 32,000,204 | 0 | 32,000,204 | Không filter vì input contract PASS |
| Inner join ratings + movies | 32,000,204 | 0 | 32,000,204 | Mọi `movieId` rating đều có metadata |

## 9. Analytics

Kết quả chạy thật:

- Ratings: 32,000,204; users: 200,948; catalog movies: 87,585; rated movies: 84,432.
- Rating trung bình: 3.5404; min/max: 0.5/5.0.
- User activity: min 20, max 33,332, trung bình 159.2462 ratings/user.
- Rating phổ biến nhất là 4.0 với 8,367,654 lượt (26.1488%).
- Output rows: rating distribution 10, user activity 200,948, movie popularity 84,432, genre statistics 21, yearly trend 29, normalized tags 131,664.
- `highly_rated_eligible=true` nghĩa là phim có ít nhất 1,000 ratings; dùng cờ này khi xếp theo average rating để tránh phim có quá ít đánh giá.

## 10. ALS Dataset

Schema bàn giao:

```text
userId  int
movieId int
rating  double
```

Không có title, genres hoặc timestamp trong tập ALS. TV3 có thể đọc trực tiếp train/test mà không cần chạy lại preprocessing.

## 11. Train/Test

- Method: chia tương tác trong từng user, sắp theo `xxhash64(userId, movieId, seed)`.
- Seed: `42`.
- Requested ratio: `80/20`; mỗi user được giữ tối thiểu một rating ở train.
- Train: **25,520,897** rows (79.7523%).
- Test: **6,479,307** rows (20.2477%).
- Train/Test overlap: **0**.
- Test users chưa xuất hiện ở train: **0**.
- Test movies chưa xuất hiện ở train: **4,178** movies / **4,729** rows. TV3 cần dùng `coldStartStrategy="drop"` hoặc quy tắc tương đương khi đánh giá ALS.

Không tạo validation vì chưa có hợp đồng TV3 yêu cầu validation riêng.

## 12. HDFS Outputs

Base path: `/project/movielens/output/tv2`.

| Path | Schema/purpose | Consumer |
|---|---|---|
| `processed/ratings` | ratings chuẩn hóa: `userId, movieId, rating, timestamp` | TV2/audit |
| `processed/ratings_movies` | ratings kèm `title, genres` | TV2/TV4 |
| `als/train` | `userId, movieId, rating` | TV3 |
| `als/test` | `userId, movieId, rating` | TV3 |
| `analytics/dataset_summary` | tổng ratings/users/movies, avg/min/max và timestamp range | TV4 |
| `analytics/rating_distribution` | `rating, count, percentage` | TV4 |
| `analytics/user_activity` | `userId, rating_count, average_rating` | TV4 |
| `analytics/movie_popularity` | movie metadata, count, average và eligibility flag | TV4 |
| `analytics/genre_statistics` | `genre, movie_count, rating_count, average_rating` | TV4 |
| `analytics/rating_trend` | `year, rating_count, average_rating` | TV4 |
| `analytics/tag_statistics` | normalized `tag, tag_count` | TV4 |
| `manifest` | JSON status, configs, counts, validation và split statistics | TV3/TV4/audit |

## 13. Handover to TV3

TV3 đọc:

```python
train = spark.read.parquet("hdfs://namenode:9000/project/movielens/output/tv2/als/train")
test = spark.read.parquet("hdfs://namenode:9000/project/movielens/output/tv2/als/test")
```

Schema, seed, ratio, filter rules, counts và cold-start statistics nằm trong `manifest`. TV3 chịu trách nhiệm huấn luyện, tuning và evaluation; cần xử lý 4,729 test rows có movie mới đối với train.

## 14. Handover to TV4

TV4 đọc trực tiếp các bảng trong `analytics/`. Metric `rating_count` là số lượt đánh giá, `average_rating` là trung bình rating, `movie_count` là số phim khác nhau trong genre và `percentage` là tỷ lệ trên toàn bộ ratings. Dùng `movie_popularity.highly_rated_eligible` trước khi trình bày bảng phim điểm cao có độ tin cậy tối thiểu.

## 15. Checklist

### Environment

- [x] Đọc README/project documentation
- [x] Đọc TV1 documentation
- [x] Hiểu architecture
- [x] MovieLens local dataset ready và checksum verified
- [x] Docker cluster running
- [x] HDFS RAW available
- [x] HDFS STANDARD verified

### Input

- [x] Standard ratings loaded
- [x] Standard movies loaded
- [x] Input schema verified
- [x] Input contract verified

### Processing

- [x] Preprocessing pipeline implemented and run
- [x] ratings + movies join implemented and run
- [x] Processing counts documented

### Analytics

- [x] Dataset summary
- [x] Rating distribution
- [x] User activity
- [x] Movie popularity
- [x] Genre analytics
- [x] Rating trend
- [x] Tag analytics

### ALS Dataset

- [x] userId/movieId/rating dataset created
- [x] Schema verified
- [x] Train created
- [x] Test created
- [x] Seed and split rules documented
- [x] Cold-start statistics documented
- [x] Leakage/split checks completed

### Output and handover

- [x] ALS and analytics Parquet saved to HDFS
- [x] Manifest saved
- [x] Outputs read-back verified
- [x] TV3 input documented
- [x] TV4 analytics documented
- [x] Run commands documented
- [x] Known issues documented

## 16. Known Issues

- Test có 4,729 rows thuộc 4,178 movies không xuất hiện trong train. Đây là cold-start thực tế của phép chia và đã được ghi rõ để TV3 xử lý.
- Máy hiện tại đã có dịch vụ chiếm các cổng host 9000/8080/8081/8082; Compose dùng 19000/18080/18081/18082. Endpoint giữa container không đổi.
- Với 1.5 GiB/executor, một task Window từng phát cảnh báo thiếu memory và được Spark retry thành công. Cấu hình chạy đã kiểm chứng vẫn hoàn tất; tăng executor memory nếu chạy trên máy có tài nguyên lớn hơn.

## 17. Next Steps

- TV3: huấn luyện/tuning ALS và đánh giá trên hai Parquet được bàn giao.
- TV4: tích hợp các bảng analytics vào dashboard/demo.
- TV2 không còn hạng mục bắt buộc chưa hoàn thành.
