# TV1 - Data & Big Data Infrastructure

## 1. Vai trò TV1

TV1 phụ trách **Data & Big Data Infrastructure**.

Luồng thực tế đã làm:

```text
MovieLens 32M
    ↓
Kiểm tra file local
    ↓
HDFS Cluster
    ↓
Upload RAW data
    ↓
Kiểm tra HDFS blocks + replication
    ↓
Spark Cluster
    ↓
Spark DataFrame + Explicit Schema
    ↓
Data Quality
    ↓
Referential Integrity
    ↓
Dataset Profiling
    ↓
Build STANDARD Parquet
    ↓
Verify STANDARD
    ↓
Bàn giao TV2
```

## 2. Môi trường

```text
Windows + WSL2 + Docker Desktop
Docker Engine 29.7.2
Docker Compose v5.5.0
Hadoop 3.5.0
Spark 3.5.9
```

Kiểm tra Docker:

```powershell
docker --version
docker compose version
docker info --format "{{.OSType}}"
```

Ý nghĩa:

- `docker --version`: kiểm tra Docker CLI.
- `docker compose version`: kiểm tra Docker Compose.
- `docker info --format "{{.OSType}}"`: xác nhận Docker đang chạy Linux container.

## 3. Kiểm tra và khởi động Docker Compose

```powershell
docker compose config
```

Dùng để parse YAML và kiểm tra các service, port, volume, network.

Khởi động:

```powershell
docker compose up -d
```

`-d` = chạy background.

Kiểm tra:

```powershell
docker compose ps
```

Kỳ vọng:

```text
namenode       Up
datanode1      Up
datanode2      Up
datanode3      Up
spark-master   Up
spark-worker1  Up
spark-worker2  Up
```

## 4. Hadoop HDFS Cluster

Kiến trúc:

```text
NameNode
   ├── DataNode1
   ├── DataNode2
   └── DataNode3
```

Replication Factor = `3`.

Kiểm tra cluster:

```powershell
docker exec -it namenode hdfs dfsadmin -report
```

Mục đích: xem số DataNode sống, dung lượng, trạng thái cluster và replication.

Kết quả đã xác nhận:

```text
Live datanodes (3)
```

## 5. Test HDFS ban đầu

Tạo thư mục:

```powershell
docker exec -it namenode hdfs dfs -mkdir -p /bigdata/test
```

Tạo file trong filesystem container:

```powershell
docker exec -it namenode bash -c "echo Hello-HDFS > /tmp/hello.txt"
```

Upload vào HDFS:

```powershell
docker exec -it namenode hdfs dfs -put /tmp/hello.txt /bigdata/test/
```

Đọc file:

```powershell
docker exec -it namenode hdfs dfs -cat /bigdata/test/hello.txt
```

Kiểm tra block:

```powershell
docker exec -it namenode hdfs fsck /bigdata/test/hello.txt -files -blocks -locations
```

Kết quả: file HEALTHY, replication = 3.

## 6. Tạo HDFS structure cho project

```powershell
docker exec -it namenode hdfs dfs -mkdir -p /project/movielens/raw
docker exec -it namenode hdfs dfs -mkdir -p /project/movielens/standard
docker exec -it namenode hdfs dfs -mkdir -p /project/movielens/output
```

Kiểm tra:

```powershell
docker exec -it namenode hdfs dfs -ls -R /project
```

Ý nghĩa:

```text
raw      = dữ liệu gốc
standard = dữ liệu chuẩn ban đầu
output   = nơi lưu kết quả xử lý về sau
```

## 7. Kiểm tra MovieLens local

Local:

```text
data/ml-32m/
├── checksums.txt
├── links.csv
├── movies.csv
├── ratings.csv
├── README.txt
└── tags.csv
```

Kiểm tra MD5:

```powershell
Get-FileHash data\ml-32m\links.csv -Algorithm MD5
Get-FileHash data\ml-32m\movies.csv -Algorithm MD5
Get-FileHash data\ml-32m\ratings.csv -Algorithm MD5
Get-FileHash data\ml-32m\tags.csv -Algorithm MD5
```

MD5 đã khớp với `checksums.txt`.

## 8. Upload MovieLens lên HDFS

Copy local → NameNode container:

```powershell
docker cp data\ml-32m namenode:/tmp/ml-32m
```

Kiểm tra:

```powershell
docker exec -it namenode ls -lh /tmp/ml-32m
```

Container → HDFS:

```powershell
docker exec -it namenode bash -c "hdfs dfs -put /tmp/ml-32m/*.csv /project/movielens/raw/"
```

Kiểm tra:

```powershell
docker exec -it namenode hdfs dfs -ls -h /project/movielens/raw
```

Kết quả:

```text
links.csv      ~1.9 MiB
movies.csv     ~4.0 MiB
ratings.csv    ~836.4 MiB
tags.csv       ~69.0 MiB
```

Sau khi xác nhận HDFS an toàn:

```powershell
docker exec namenode rm -rf /tmp/ml-32m
```

Lệnh này chỉ xóa bản tạm trong container.

## 9. Kiểm tra HDFS block của ratings.csv

```powershell
docker exec -it namenode hdfs fsck /project/movielens/raw/ratings.csv -files -blocks -locations
```

Kết quả chính:

```text
File size   : 877,076,222 bytes
HDFS blocks : 7
Replication : 3
Status      : HEALTHY
```

## 10. Spark Cluster

Kiến trúc:

```text
Spark Master
   ├── Worker1: 2 cores / 2 GiB
   └── Worker2: 2 cores / 2 GiB
```

Kiểm tra:

```powershell
docker logs spark-master --tail 30
docker logs spark-worker1 --tail 30
docker logs spark-worker2 --tail 30
```

Kết quả quan trọng:

```text
Successfully registered with master spark://spark-master:7077
```

UI:

```text
Master : http://localhost:8080
Worker1: http://localhost:8081
Worker2: http://localhost:8082
```

## 11. Test Spark phân tán

File:

```text
spark/hello_spark.py
```

Chạy:

```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.driver.host=spark-master --conf spark.driver.bindAddress=0.0.0.0 /opt/spark-apps/hello_spark.py
```

Giải thích:

- `docker exec -it spark-master`: chạy command trong container Spark Master.
- `spark-submit`: submit Spark application.
- `--master spark://spark-master:7077`: chạy trên Spark Standalone cluster.
- `--deploy-mode client`: Driver chạy trong container gọi `spark-submit`.
- `spark.driver.host=spark-master`: Executor liên lạc về Driver qua Docker hostname.
- `spark.driver.bindAddress=0.0.0.0`: Driver listen trên interface container.

Kết quả:

```text
Spark version : 3.5.9
Master        : spark://spark-master:7077
Partitions    : 4
Total rows    : 1,000,000
Executors     : 2
```

## 12. Đọc ratings.csv bằng Spark

File:

```text
spark/read_ratings.py
```

Chạy:

```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.driver.host=spark-master --conf spark.driver.bindAddress=0.0.0.0 /opt/spark-apps/read_ratings.py
```

Kết quả:

```text
Partitions    : 7
Total ratings : 32,000,204
```

Schema:

```text
userId     integer
movieId    integer
rating     double
timestamp  long
```

## 13. Inspect toàn bộ MovieLens

File:

```text
spark/inspect_movielens.py
```

Chạy:

```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.driver.host=spark-master --conf spark.driver.bindAddress=0.0.0.0 /opt/spark-apps/inspect_movielens.py
```

Kết quả:

```text
Ratings : 32,000,204
Movies  : 87,585
Tags    : 2,000,072
Links   : 87,585
```

## 14. Data Quality

File:

```text
spark/data_quality.py
```

Chạy:

```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.driver.host=spark-master --conf spark.driver.bindAddress=0.0.0.0 /opt/spark-apps/data_quality.py
```

Kiểm tra:

```text
NULL
exact duplicate
duplicate userId + movieId
invalid userId
invalid movieId
rating ngoài [0.5, 5.0]
```

Kết quả:

```text
NULL                      : 0
Exact duplicate           : 0
Duplicate user-movie pair : 0
Invalid userId            : 0
Invalid movieId           : 0
Invalid rating            : 0
```

## 15. Referential Integrity

File:

```text
spark/referential_integrity.py
```

Chạy:

```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.driver.host=spark-master --conf spark.driver.bindAddress=0.0.0.0 /opt/spark-apps/referential_integrity.py
```

Kiểm tra:

```text
ratings.movieId → movies.movieId
tags.movieId    → movies.movieId
links.movieId   → movies.movieId
movies.movieId  → links.movieId
```

Kết quả:

```text
Duplicate movieId in movies : 0
ratings missing movieId     : 0
tags missing movieId        : 0
links missing movieId       : 0
movies without links        : 0
invalid rating step         : 0
```

## 16. Dataset Profiling

File:

```text
spark/profile_movielens.py
```

Chạy:

```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.driver.host=spark-master --conf spark.driver.bindAddress=0.0.0.0 /opt/spark-apps/profile_movielens.py
```

Kết quả:

```text
total_ratings        : 32,000,204
unique_users         : 200,948
rated_movies         : 84,432
total_movies         : 87,585
min_rating           : 0.5
max_rating           : 5.0
first_rating_time    : 1995-01-09 11:46:44 UTC
last_rating_time     : 2023-10-13 02:29:07 UTC
avg_ratings_per_user : 159.2462
avg_ratings_per_movie: 379.0056
```

## 17. Build Standard Layer

File:

```text
spark/build_standard_layer.py
```

Chạy:

```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.driver.host=spark-master --conf spark.driver.bindAddress=0.0.0.0 /opt/spark-apps/build_standard_layer.py
```

Flow:

```text
HDFS RAW CSV
    ↓
Spark DataFrame
    ↓
Explicit Schema
    ↓
Parquet + Snappy
    ↓
HDFS STANDARD
```

## 18. Kiểm tra Standard Layer

```powershell
docker exec -it namenode hdfs dfs -ls /project/movielens/standard
docker exec -it namenode hdfs dfs -ls -h /project/movielens/standard/ratings
```

Kết quả ratings:

```text
_SUCCESS
part-00000-....snappy.parquet
...
part-00006-....snappy.parquet
```

Kiểm tra dung lượng:

```powershell
docker exec -it namenode hdfs dfs -du -h -s /project/movielens/raw
docker exec -it namenode hdfs dfs -du -h -s /project/movielens/standard
```

Kết quả:

```text
RAW      : 911.4 MiB logical / 2.7 GiB HDFS consumed
STANDARD : 218.5 MiB logical / 655.6 MiB HDFS consumed
```

## 19. Verify Standard Layer

File:

```text
spark/verify_standard_layer.py
```

Chạy:

```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.driver.host=spark-master --conf spark.driver.bindAddress=0.0.0.0 /opt/spark-apps/verify_standard_layer.py
```

Kết quả:

```text
Ratings : 32,000,204
Movies  : 87,585
Tags    : 2,000,072
Links   : 87,585
```

Spark đọc lại schema trực tiếp từ Parquet mà không cần `StructType`.

## 20. Cách bàn giao TV2

```python
ratings_df = spark.read.parquet(
    "hdfs://namenode:9000/project/movielens/standard/ratings"
)

movies_df = spark.read.parquet(
    "hdfs://namenode:9000/project/movielens/standard/movies"
)

tags_df = spark.read.parquet(
    "hdfs://namenode:9000/project/movielens/standard/tags"
)

links_df = spark.read.parquet(
    "hdfs://namenode:9000/project/movielens/standard/links"
)
```

TV2 tiếp tục:

```text
STANDARD
→ preprocessing
→ EDA
→ feature/data preparation
→ train/test split
```

Không sửa trực tiếp RAW layer.

## 21. Checklist TV1

```text
[✓] MovieLens 32M downloaded
[✓] MD5 checksum verified
[✓] Hadoop/HDFS configured
[✓] 1 NameNode + 3 DataNodes
[✓] Replication factor = 3
[✓] RAW data uploaded to HDFS
[✓] HDFS block distribution verified
[✓] Spark cluster configured
[✓] 1 Master + 2 Workers
[✓] Spark distributed job verified
[✓] Explicit schema created
[✓] All 4 datasets inspected
[✓] Data quality checked
[✓] Referential integrity checked
[✓] Dataset profiling completed
[✓] STANDARD Parquet layer generated
[✓] STANDARD layer verified
[✓] Ready for TV2 handover
```
