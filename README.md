# Big Data Movie Recommendation System

## 1. Giới thiệu

Đề tài: **Xây dựng hệ thống gợi ý phim dựa trên dữ liệu lớn sử dụng Apache Spark và thuật toán ALS**.

Hệ thống sử dụng bộ dữ liệu **MovieLens 32M**, lưu trữ trên **Hadoop HDFS**, xử lý bằng **Apache Spark / PySpark**, sau đó cung cấp dữ liệu cho giai đoạn preprocessing, EDA và xây dựng mô hình gợi ý phim bằng **Spark MLlib ALS**.

## 2. Flow tổng quan

```text
MovieLens 32M
    ↓
Local dataset (data/ml-32m)
    ↓
HDFS RAW
/project/movielens/raw
    ↓
Spark / PySpark
    ├── Explicit Schema
    ├── Data Quality
    ├── Referential Integrity
    └── Dataset Profiling
    ↓
HDFS STANDARD (Parquet)
/project/movielens/standard
    ↓
TV2: Preprocessing + EDA + Train/Test
    ↓
TV3: Spark MLlib ALS + Evaluation + Top-N
    ↓
TV4: Dashboard / Web Demo
```

## 3. Dataset

| Dataset | Số dòng thực tế | Vai trò |
|---|---:|---|
| `ratings.csv` | 32,000,204 | Dữ liệu đánh giá, đầu vào chính cho ALS |
| `movies.csv` | 87,585 | Danh mục phim, title và genres |
| `tags.csv` | 2,000,072 | Tag người dùng gắn cho phim |
| `links.csv` | 87,585 | Mapping MovieLens movieId sang IMDb/TMDB |

Schema chuẩn:

```text
ratings: userId integer, movieId integer, rating double, timestamp long
movies : movieId integer, title string, genres string
tags   : userId integer, movieId integer, tag string, timestamp long
links  : movieId integer, imdbId string, tmdbId integer
```

`imdbId` được giữ dạng `string` để không mất số `0` ở đầu.

## 4. Hạ tầng Big Data

### Hadoop HDFS

```text
1 NameNode
3 DataNodes
Replication Factor = 3
```

Container:

```text
namenode
datanode1
datanode2
datanode3
```

HDFS Web UI:

```text
http://localhost:9870
```

HDFS endpoint nội bộ Docker:

```text
hdfs://namenode:9000
```

### Apache Spark

```text
1 Spark Master
2 Spark Workers
```

Tài nguyên:

```text
spark-worker1: 2 cores / 2 GiB
spark-worker2: 2 cores / 2 GiB
Tổng: 4 cores / 4 GiB worker memory
```

Spark UI:

```text
Master : http://localhost:8080
Worker1: http://localhost:8081
Worker2: http://localhost:8082
```

Tất cả Hadoop và Spark dùng chung Docker network:

```text
bigdata-network
```

## 5. HDFS Data Layers

```text
/project/movielens/
├── raw/
├── standard/
└── output/
```

RAW:

```text
/project/movielens/raw/
├── links.csv
├── movies.csv
├── ratings.csv
└── tags.csv
```

STANDARD:

```text
/project/movielens/standard/
├── links/
├── movies/
├── ratings/
└── tags/
```

STANDARD dùng **Parquet + Snappy** để giữ schema, giảm dung lượng và tối ưu việc đọc bằng Spark.

Dung lượng thực tế:

```text
RAW logical size      : 911.4 MiB
RAW HDFS consumed     : 2.7 GiB
STANDARD logical size : 218.5 MiB
STANDARD HDFS consumed: 655.6 MiB
```

STANDARD nhỏ hơn RAW khoảng **76%** về logical size.

## 6. Kết quả Data Quality

```text
Total ratings                    : 32,000,204
NULL userId                      : 0
NULL movieId                     : 0
NULL rating                      : 0
NULL timestamp                   : 0
Exact duplicate                  : 0
Duplicate userId + movieId pair  : 0
Invalid userId                   : 0
Invalid movieId                  : 0
Invalid rating range             : 0
Invalid 0.5 rating step          : 0
```

Referential integrity:

```text
Duplicate movieId trong movies                : 0
ratings.movieId thiếu trong movies            : 0
tags.movieId thiếu trong movies               : 0
links.movieId thiếu trong movies              : 0
movies không có record tương ứng trong links  : 0
```

Kết luận: **không phát hiện lỗi trong phạm vi các kiểm tra chất lượng đã thực hiện**.

## 7. Dataset Profiling

```text
Ratings             : 32,000,204
Unique users        : 200,948
Movies trong catalog: 87,585
Movies có rating    : 84,432
Rating min/max      : 0.5 / 5.0
First rating        : 1995-01-09 11:46:44 UTC
Last rating         : 2023-10-13 02:29:07 UTC
Ratings/user min    : 20
Ratings/user max    : 33,332
Ratings/user avg    : 159.2462
Ratings/movie min   : 1
Ratings/movie max   : 102,929
Ratings/movie avg   : 379.0056
```

Có `3,153` phim trong catalog chưa xuất hiện trong `ratings.csv`; đây không được coi là lỗi dữ liệu.

## 8. Khởi động môi trường

```powershell
docker compose up -d
docker compose ps
```

Kỳ vọng 7 container:

```text
namenode
datanode1
datanode2
datanode3
spark-master
spark-worker1
spark-worker2
```

## 9. TV2 đọc Standard Layer

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

ratings_df = spark.read.parquet(
    "hdfs://namenode:9000/project/movielens/standard/ratings"
)

movies_df = spark.read.parquet(
    "hdfs://namenode:9000/project/movielens/standard/movies"
)
```

Không cần khai báo lại schema khi đọc Parquet.

## 10. Công nghệ

- Docker Desktop + WSL2
- Hadoop 3.5.0
- Apache Spark 3.5.9
- PySpark
- HDFS
- Spark DataFrame
- Parquet + Snappy
- MovieLens 32M
- Spark MLlib ALS (giai đoạn mô hình)

## 11. Phạm vi TV1

```text
Dataset
→ HDFS
→ Spark
→ Schema
→ Data Quality
→ Referential Integrity
→ Dataset Profiling
→ Standard Layer
→ Bàn giao TV2
```

TV1 không thực hiện ALS hoặc tuning mô hình.

## 12. Setup from a fresh clone

Raw MovieLens CSV files are **not committed to Git**. A fresh machine recreates them from the official GroupLens download, then uploads them to HDFS.

### Step 1 - Clone repository

```powershell
git clone https://github.com/<USERNAME>/bigdata-movie-recommendation.git
cd bigdata-movie-recommendation
```

### Step 2 - Download MovieLens 32M

```powershell
.\scripts\download_movielens.ps1
```

The script:

```text
Official GroupLens
    ↓
ml-32m.zip
    ↓
extract
    ↓
data/ml-32m/
    ↓
verify MD5 checksums
```

Expected local data:

```text
data/ml-32m/
├── checksums.txt
├── links.csv
├── movies.csv
├── ratings.csv
├── README.txt
└── tags.csv
```

If PowerShell blocks local scripts for the current terminal session:

```powershell
Set-ExecutionPolicy -Scope Process Bypass
```

Then run the download script again.

### Step 3 - Start Hadoop + Spark

```powershell
docker compose up -d
docker compose ps
```

Expected services:

```text
namenode
datanode1
datanode2
datanode3
spark-master
spark-worker1
spark-worker2
```

### Step 4 - Upload RAW data to HDFS

```powershell
.\scripts\upload_to_hdfs.ps1
```

The script automatically:

```text
creates /project/movielens/{raw,standard,output}
    ↓
copies local MovieLens files into namenode
    ↓
uploads CSV files into HDFS /raw
    ↓
lists HDFS files
    ↓
runs fsck on ratings.csv
    ↓
removes the temporary container copy
```

Expected RAW layer:

```text
/project/movielens/raw/
├── links.csv
├── movies.csv
├── ratings.csv
└── tags.csv
```

### Step 5 - Inspect and validate data

```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.driver.host=spark-master --conf spark.driver.bindAddress=0.0.0.0 /opt/spark-apps/inspect_movielens.py
```

```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.driver.host=spark-master --conf spark.driver.bindAddress=0.0.0.0 /opt/spark-apps/data_quality.py
```

```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.driver.host=spark-master --conf spark.driver.bindAddress=0.0.0.0 /opt/spark-apps/referential_integrity.py
```

```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.driver.host=spark-master --conf spark.driver.bindAddress=0.0.0.0 /opt/spark-apps/profile_movielens.py
```

### Step 6 - Build Standard Parquet layer

```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.driver.host=spark-master --conf spark.driver.bindAddress=0.0.0.0 /opt/spark-apps/build_standard_layer.py
```

### Step 7 - Verify Standard Layer

```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.driver.host=spark-master --conf spark.driver.bindAddress=0.0.0.0 /opt/spark-apps/verify_standard_layer.py
```

Expected counts:

```text
Ratings : 32,000,204
Movies  : 87,585
Tags    : 2,000,072
Links   : 87,585
```

At this point the TV1 pipeline has been reproduced from scratch and the Standard Layer is ready for TV2.

## 13. Why raw CSV files are not stored in Git

The repository stores **code, configuration, documentation, and reproducible setup scripts**, not the large MovieLens CSV payload.

The reproducible path is:

```text
Git repository
    ↓
download_movielens.ps1
    ↓
Official MovieLens 32M
    ↓
Local RAW files
    ↓
upload_to_hdfs.ps1
    ↓
HDFS RAW
    ↓
Spark
    ↓
HDFS STANDARD
```

This keeps the repository small and ensures every team member can recreate the same data pipeline from the official dataset source.
