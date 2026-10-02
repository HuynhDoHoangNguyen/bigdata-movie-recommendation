# Project Architecture

## 1. Mục tiêu

Tài liệu này quy định cách tổ chức project để các thành viên làm việc không bị lộn xộn, không ghi nhầm vào RAW layer và không trộn source của từng giai đoạn.

Flow chuẩn:

```text
Local dataset
    ↓
HDFS RAW
    ↓
TV1 infrastructure + validation
    ↓
HDFS STANDARD
    ↓
TV2 preprocessing/EDA
    ↓
TV3 ALS
    ↓
TV4 application/dashboard
```

## 2. Project Directory Structure

```text
bigdata-movie-recommendation/
│
├── README.md
├── TV1.md
├── ARCHITECTURE.md
├── VERIFICATION_RESULT.md
├── docker-compose.yml
│
├── config/
│   ├── core-site.xml
│   └── hdfs-site.xml
│
├── data/
│   └── ml-32m/
│       ├── checksums.txt
│       ├── README.txt
│       ├── ratings.csv
│       ├── movies.csv
│       ├── tags.csv
│       └── links.csv
│
├── spark/
│   ├── hello_spark.py
│   ├── read_ratings.py
│   ├── inspect_movielens.py
│   ├── data_quality.py
│   ├── referential_integrity.py
│   ├── profile_movielens.py
│   ├── build_standard_layer.py
│   └── verify_standard_layer.py
│
├── docs/
│   ├── screenshots/
│   ├── reports/
│   └── results/
│
└── .gitignore
```

## 3. Root files

### `README.md`

Mô tả toàn bộ đồ án, kiến trúc tổng quan, flow xử lý và cách khởi động.

### `TV1.md`

Mô tả toàn bộ phần Data & Big Data Infrastructure của TV1, kèm lệnh đã chạy và ý nghĩa.

### `ARCHITECTURE.md`

Quy ước tổ chức project, HDFS layer, Docker cluster và trách nhiệm giữa các thành viên.

### `VERIFICATION_RESULT.md`

Lưu bằng chứng verify Standard Layer sau khi Spark đọc lại Parquet.

### `docker-compose.yml`

Định nghĩa toàn bộ Hadoop + Spark cluster.

## 4. `config/`

```text
config/
├── core-site.xml
└── hdfs-site.xml
```

`core-site.xml` khai báo filesystem mặc định:

```text
hdfs://namenode:9000
```

`hdfs-site.xml` khai báo các cấu hình HDFS quan trọng như:

```text
dfs.replication = 3
NameNode data directory
DataNode data directory
```

Không đặt source PySpark trong `config/`.

## 5. `data/`

```text
data/ml-32m/
```

Chỉ chứa dữ liệu MovieLens tải về.

Quy tắc:

```text
KHÔNG sửa trực tiếp dữ liệu nguồn.
KHÔNG ghi output Spark vào data/ml-32m.
```

Nếu dùng Git, nên ignore các file dataset lớn:

```gitignore
data/ml-32m/*.csv
data/ml-32m/*.zip
```

Có thể giữ lại `README.txt` và `checksums.txt`.

## 6. `spark/`

Chứa source code PySpark.

```text
hello_spark.py
```

Kiểm tra Spark cluster chạy distributed.

```text
read_ratings.py
```

Kiểm tra Spark đọc `ratings.csv` từ HDFS.

```text
inspect_movielens.py
```

Đọc 4 dataset, in schema, count và sample.

```text
data_quality.py
```

Kiểm tra NULL, duplicate, invalid IDs và invalid ratings.

```text
referential_integrity.py
```

Kiểm tra quan hệ `movieId` giữa ratings/tags/links và movies.

```text
profile_movielens.py
```

Thống kê quy mô dataset.

```text
build_standard_layer.py
```

Chuyển RAW CSV thành STANDARD Parquet.

```text
verify_standard_layer.py
```

Đọc lại STANDARD để xác nhận count + schema.

## 7. Khi project lớn hơn

Có thể refactor source theo thành viên:

```text
spark/
├── tv1/
│   ├── inspect_movielens.py
│   ├── data_quality.py
│   ├── referential_integrity.py
│   ├── profile_movielens.py
│   ├── build_standard_layer.py
│   └── verify_standard_layer.py
│
├── tv2/
│   ├── preprocessing.py
│   ├── eda.py
│   └── train_test_split.py
│
└── tv3/
    ├── train_als.py
    ├── evaluate_als.py
    └── generate_topn.py
```

Nếu source hiện tại đang ổn ở `spark/`, chưa cần di chuyển ngay. Nên refactor khi TV2/TV3 bắt đầu commit code.

## 8. HDFS Architecture

```text
/project/movielens/
│
├── raw/
│   ├── ratings.csv
│   ├── movies.csv
│   ├── tags.csv
│   └── links.csv
│
├── standard/
│   ├── ratings/
│   ├── movies/
│   ├── tags/
│   └── links/
│
└── output/
```

### `raw/`

Dữ liệu nguồn.

Quy tắc:

```text
READ ONLY theo workflow.
Không dùng Spark overwrite lên RAW.
```

### `standard/`

Đầu ra TV1 ở dạng Parquet.

TV2 sử dụng đây làm input chính.

### `output/`

Dành cho output của các giai đoạn sau.

Có thể mở rộng:

```text
output/
├── eda/
├── train/
├── test/
├── models/
└── recommendations/
```

Không để model hoặc recommendation trong `raw/` hay `standard/`.

## 9. Docker Architecture

```text
                           Docker Desktop / WSL2
                                    │
                          bigdata-network
                                    │
                 ┌──────────────────┴──────────────────┐
                 │                                     │
               HDFS                                  Spark
                 │                                     │
            NameNode                              Spark Master
           namenode:9000                       spark-master:7077
                 │                                     │
        ┌────────┼────────┐                   ┌────────┴────────┐
        │        │        │                   │                 │
   DataNode1 DataNode2 DataNode3        Spark Worker1    Spark Worker2
```

HDFS ports:

```text
9870 → NameNode Web UI
9000 → HDFS RPC
9864 → DataNode1 Web UI
9865 → DataNode2 Web UI (host mapping)
9866 → DataNode3 Web UI (host mapping)
```

Spark ports:

```text
7077 → Spark Standalone Master
8080 → Spark Master UI
8081 → Worker1 UI
8082 → Worker2 UI
```

## 10. Docker Volumes

Persistent HDFS data:

```text
namenode_data
datanode1_data
datanode2_data
datanode3_data
```

Lưu ý:

```text
docker compose down
```

thông thường giữ volume.

Còn:

```text
docker compose down -v
```

xóa volume và có thể xóa toàn bộ HDFS data. Không dùng `-v` nếu không chủ động reset cluster.

## 11. Bind Mounts

Spark source:

```text
./spark
    ↓
/opt/spark-apps
```

Hadoop config cho Spark:

```text
./config
    ↓
/opt/hadoop-conf
```

Environment:

```text
HADOOP_CONF_DIR=/opt/hadoop-conf
```

Nhờ vậy Spark biết HDFS endpoint:

```text
hdfs://namenode:9000
```

## 12. Luồng dữ liệu bắt buộc

Luồng chính:

```text
data/ml-32m
    ↓
HDFS /raw
    ↓
Spark DataFrame
    ↓
Validation
    ↓
HDFS /standard
    ↓
TV2
```

Không dùng đường dẫn ổ `E:\...` làm input trực tiếp cho pipeline Big Data chính.

## 13. Quy tắc đặt tên

Python dùng `snake_case`:

```text
data_quality.py
build_standard_layer.py
verify_standard_layer.py
```

HDFS directory dùng chữ thường:

```text
raw
standard
output
ratings
movies
tags
links
```

Spark Application Name dùng tên rõ nghĩa:

```text
MovieLensInspection
MovieLensDataQuality
MovieLensProfiling
MovieLensBuildStandardLayer
VerifyMovieLensStandardLayer
```

## 14. Quy tắc dữ liệu

RAW:

```text
Không overwrite.
Không clean trực tiếp.
Không đổi schema nguồn.
```

STANDARD:

```text
Có explicit schema.
Đã qua kiểm tra cơ bản.
Dùng Parquet.
Có thể rebuild bằng script.
```

Nếu TV2 tạo dữ liệu sau preprocessing, nên dùng layer mới, ví dụ:

```text
/project/movielens/processed/
```

hoặc:

```text
/project/movielens/output/processed/
```

Cả nhóm nên thống nhất trước khi TV2 bắt đầu.

## 15. Dependency giữa thành viên

```text
TV1
Data + HDFS + Spark + Standard
        ↓
TV2
Preprocessing + EDA + Train/Test
        ↓
TV3
ALS + Evaluation + Top-N
        ↓
TV4
Dashboard / Web Demo
```

Quy tắc:

- TV2 không sửa RAW.
- TV3 không tự tạo lại pipeline dữ liệu nếu TV2 đã có output chuẩn.
- TV4 lấy recommendation/output, không train lại model.
- Mỗi giai đoạn phải có input/output path rõ ràng.

## 16. Trạng thái hiện tại

```text
HDFS Cluster       ✅
Spark Cluster      ✅
RAW Layer          ✅
STANDARD Layer     ✅
Data Validation    ✅
Data Profiling     ✅
TV1 → TV2 Handover ✅
```
