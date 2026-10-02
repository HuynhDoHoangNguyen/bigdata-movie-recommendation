# Standard Layer Verification Result

## 1. Command

```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.driver.host=spark-master --conf spark.driver.bindAddress=0.0.0.0 /opt/spark-apps/verify_standard_layer.py
```

Spark application:

```text
VerifyMovieLensStandardLayer
```

Cluster connection đã thành công với:

```text
spark://spark-master:7077
```

và có 2 executors chạy trên 2 Spark workers.

## 2. RATINGS

```text
Rows: 32000204

root
 |-- userId: integer (nullable = true)
 |-- movieId: integer (nullable = true)
 |-- rating: double (nullable = true)
 |-- timestamp: long (nullable = true)
```

Sample:

```text
+------+-------+------+---------+
|userId|movieId|rating|timestamp|
+------+-------+------+---------+
|1     |17     |4.0   |944249077|
|1     |25     |1.0   |944250228|
|1     |29     |2.0   |943230976|
|1     |30     |5.0   |944249077|
|1     |32     |5.0   |943228858|
+------+-------+------+---------+
```

## 3. MOVIES

```text
Rows: 87585

root
 |-- movieId: integer (nullable = true)
 |-- title: string (nullable = true)
 |-- genres: string (nullable = true)
```

Sample:

```text
1 | Toy Story (1995)                   | Adventure|Animation|Children|Comedy|Fantasy
2 | Jumanji (1995)                     | Adventure|Children|Fantasy
3 | Grumpier Old Men (1995)            | Comedy|Romance
4 | Waiting to Exhale (1995)           | Comedy|Drama|Romance
5 | Father of the Bride Part II (1995) | Comedy
```

## 4. TAGS

```text
Rows: 2000072

root
 |-- userId: integer (nullable = true)
 |-- movieId: integer (nullable = true)
 |-- tag: string (nullable = true)
 |-- timestamp: long (nullable = true)
```

Sample:

```text
78213 | 108289 | airplane      | 1527374442
78213 | 108289 | passenger     | 1527374442
78213 | 108289 | terrorist     | 1527374442
78213 | 108289 | war on terror | 1527374442
78213 | 108289 | weapon        | 1527374442
```

## 5. LINKS

```text
Rows: 87585

root
 |-- movieId: integer (nullable = true)
 |-- imdbId: string (nullable = true)
 |-- tmdbId: integer (nullable = true)
```

Sample:

```text
1 | 0114709 | 862
2 | 0113497 | 8844
3 | 0113228 | 15602
4 | 0114885 | 31357
5 | 0113041 | 11862
```

## 6. HDFS Standard Structure

```text
/project/movielens/standard/
├── links/
├── movies/
├── ratings/
└── tags/
```

`ratings/` gồm `_SUCCESS` và 7 file `part-*.snappy.parquet`.

## 7. Storage Result

```text
RAW
Logical size  : 911.4 MiB
HDFS consumed : 2.7 GiB

STANDARD
Logical size  : 218.5 MiB
HDFS consumed : 655.6 MiB
```

Parquet Standard Layer giảm khoảng 76% logical size so với CSV RAW.

## 8. Kết luận verify

```text
[✓] Spark đọc được toàn bộ STANDARD layer từ HDFS
[✓] Record counts giữ nguyên
[✓] Schema được lưu trong Parquet
[✓] ratings: 32,000,204 rows
[✓] movies : 87,585 rows
[✓] tags   : 2,000,072 rows
[✓] links  : 87,585 rows
[✓] TV2 có thể đọc trực tiếp bằng spark.read.parquet(...)
```

Standard Layer sẵn sàng bàn giao cho giai đoạn preprocessing/EDA.
