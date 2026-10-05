# TV4 — Dashboard, Web Demo & Integration

Tài liệu báo cáo hoàn thành và bàn giao module của **Thành viên 4 (TV4)** trong đồ án Big Data: **Hệ thống gợi ý phim sử dụng Apache Spark và ALS (MovieLens 32M)**.

---

## 1. Vai trò và Trách nhiệm của TV4

Theo bảng phân công công việc đồ án:
- **Module phụ trách**: Dashboard, Web Demo & Integration.
- **Đầu vào nhận từ các thành viên**:
  * **Từ TV1**: Cấu trúc hạ tầng HDFS (`/project/movielens/`), schema dữ liệu chuẩn Parquet, kiểm tra tính toàn vẹn và chất lượng dữ liệu.
  * **Từ TV2**: 7 bảng thống kê phân tích phân tán PySpark (`dataset_summary`, `rating_distribution`, `user_activity`, `movie_popularity`, `genre_statistics`, `rating_trend`, `tag_statistics`).
  * **Từ TV3**: Kết quả mô hình Spark MLlib ALS, các chỉ số đánh giá (RMSE, MAE), bảng so sánh hiệu năng xếp hạng (Top-K) và danh sách gợi ý phim Top-10 cho 20 demo users (gồm cả raw ALS và support-aware reranked).
- **Đầu ra bàn giao của TV4**:
  * Ứng dụng Web Demo chạy hoàn chỉnh, giao diện responsive, hiện đại.
  * Pipeline xuất dữ liệu tự động từ HDFS sang Web (`spark/tv4/export_for_web.py`).
  * Bộ kiểm thử tự động 10/10 test cases (`web/test_web.py`).
  * Tài liệu hướng dẫn sử dụng và kịch bản thuyết trình (`HDSD_WEB_DEMO.md`, `web/README.md`).
  * Script khởi động nhanh 1-click (`run_web_demo.ps1`).

---

## 2. Kiến trúc Giải pháp Kỹ thuật

```text
                            HẠ TẦNG BIG DATA (HDFS + Spark)
┌────────────────────────────────────────────────────────────────────────────────────────┐
│  /project/movielens/output/tv2/analytics/       /project/movielens/output/tv3/         │
│  ├── dataset_summary (Parquet)                  ├── recommendations/topn_reranked      │
│  ├── rating_distribution (Parquet)              ├── recommendations/topn (raw)         │
│  ├── movie_popularity (Parquet)                 ├── metrics/final (RMSE/MAE)           │
│  ├── genre_statistics (Parquet)                 ├── metrics/reranking_test (Top-K)     │
│  ├── rating_trend (Parquet)                     └── scope/demo_users                   │
│  └── tag_statistics (Parquet)                                                          │
└──────────────────────────────────────────┬─────────────────────────────────────────────┘
                                           │
                        spark/tv4/export_for_web.py (PySpark Batch Job)
                                           │
                                           ▼
                            TẦNG DỮ LIỆU ĐỆM (JSON Cache)
┌────────────────────────────────────────────────────────────────────────────────────────┐
│  web/data/*.json (12 datasets: summary, dist, pop, genres, trend, tags, recs, metrics) │
└──────────────────────────────────────────┬─────────────────────────────────────────────┘
                                           │
                                           ▼
                            TẦNG ỨNG DỤNG WEB (Flask Backend)
┌────────────────────────────────────────────────────────────────────────────────────────┐
│  web/app.py (REST APIs & Template Rendering)                                           │
│  ├── GET /                      -> Dashboard tổng quan                                 │
│  ├── GET /analytics             -> Phân tích trực quan EDA                             │
│  ├── GET /recommend             -> Trình diễn gợi ý Top-N phim                         │
│  └── GET /api/*                 -> JSON API Endpoints phục vụ Frontend                │
└──────────────────────────────────────────┬─────────────────────────────────────────────┘
                                           │
                                           ▼
                           TẦNG GIAO DIỆN (Frontend Client)
┌────────────────────────────────────────────────────────────────────────────────────────┐
│  HTML5 + Bootstrap 5 + Chart.js 4.x + Custom CSS/JS                                    │
│  [Trực quan hóa tương tác] [Lọc User ID] [So sánh Raw vs Reranked] [Mobile Responsive] │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

### Ưu điểm của kiến trúc phân tầng (Decoupled Architecture):
1. **Hiệu năng cao & Tải tức thì**: Web server không phải khởi tạo SparkContext nặng nề trên mỗi HTTP request. Toàn bộ dữ liệu demo được nạp vào bộ nhớ RAM dưới dạng cấu trúc JSON tối ưu, phản hồi API trong thời gian mili-giây (< 10ms).
2. **Độc lập và An toàn**: Người dùng truy cập hoặc reload trang web không bao giờ kích hoạt lại lệnh train mô hình hoặc ghi đè dữ liệu trên HDFS, tuân thủ tuyệt đối quy định an toàn tại `docs/TV3_HANDOFF_TO_TV4.md`.
3. **Linh hoạt khi demo**: Web Demo có thể chạy trên bất kỳ máy tính nào có Python/Flask mà không đòi hỏi phải dựng cụm Hadoop 7 container ngốn 8–12 GB RAM khi thuyết trình.

---

## 3. Danh mục các thành phần đã phát triển

| STT | Tên file / Thành phần | Chức năng chính |
|:---:|---|---|
| 1 | `web/app.py` | Ứng dụng Flask trung tâm, xử lý định tuyến (routing), lọc template Jinja2, REST APIs. |
| 2 | `web/static/css/style.css` | Thiết kế giao diện hiện đại, bảng màu trực quan, responsive tương thích điện thoại/máy tính. |
| 3 | `web/static/js/main.js` | Logic frontend: Khởi tạo 5 biểu đồ Chart.js, gọi API gợi ý phim, chuyển đổi chế độ Raw/Reranked. |
| 4 | `web/templates/base.html` | Layout khung dùng chung (Navbar responsive, Container, Footer). |
| 5 | `web/templates/index.html` | Trang chủ Dashboard: Thẻ thống kê 32M ratings, lưu đồ pipeline, thông tin mô hình ALS, bảng so sánh phương pháp. |
| 6 | `web/templates/analytics.html` | Trang Phân tích: 5 khung biểu đồ trực quan hóa dữ liệu EDA. |
| 7 | `web/templates/recommend.html` | Trang Gợi ý phim: Chọn mã User, danh sách thẻ phim với điểm số, rank và support. |
| 8 | `spark/tv4/export_for_web.py` | Script PySpark đọc dữ liệu Parquet phân tán từ HDFS của TV2 và TV3, chuyển đổi và xuất ra file JSON. |
| 9 | `web/populate_data.py` | Script nạp dữ liệu chuẩn xác định từ kết quả thực nghiệm của TV2 và TV3 vào `web/data/`. |
| 10 | `web/test_web.py` | Bộ kiểm thử tự động 10 test cases kiểm tra toàn bộ views, templates, filter và API endpoints. |
| 11 | `web/Dockerfile` | Cấu hình Docker để đóng gói ứng dụng web thành container độc lập nếu cần. |
| 12 | `run_web_demo.ps1` | Script PowerShell 1-click tự động kiểm tra môi trường, chạy test và bật Web Demo. |
| 13 | `HDSD_WEB_DEMO.md` | Hướng dẫn sử dụng chi tiết cho người dùng và kịch bản trình diễn khi bảo vệ đồ án. |
| 14 | `web/README.md` | Tài liệu kỹ thuật tóm tắt trong thư mục module web. |

---

## 4. Nghiệm thu Checklist tiếp nhận từ TV3

Đối chiếu trực tiếp với 10 yêu cầu trong phần *"Checklist TV4 tiếp nhận và nghiệm thu demo"* của tài liệu `docs/TV3_HANDOFF_TO_TV4.md`:

| STT | Tiêu chí nghiệm thu từ TV3 | Trạng thái | Minh chứng thực tế |
|:---:|---|:---:|---|
| 1 | Backend đọc được raw và reranked Parquet trên môi trường TV4 | ✅ **ĐẠT** | Đã triển khai trong `spark/tv4/export_for_web.py` và lưu trữ tại `web/data/recommendations.json` và `recommendations_raw.json`. |
| 2 | Mỗi dataset có 200 rows, 20 demo users, 10 recommendations/user | ✅ **ĐẠT** | 20 users × 10 phim = 200 bản ghi chính xác, kiểm tra tự động qua `test_api_recommend_valid_user`. |
| 3 | Selector dùng đúng demo users; user ngoài demo có thông báo phù hợp | ✅ **ĐẠT** | Dropdown chứa đúng 20 demo users; nhập user lạ (như 9999999) trả về thông báo kèm 20 nút chọn nhanh (`test_api_recommend_invalid_user` PASS). |
| 4 | User `806` hiển thị 10 phim theo rank, title/genres/movieId đúng output | ✅ **ĐẠT** | User `806` được tải tự động làm mặc định khi mở trang, hiển thị đầy đủ thứ hạng 1–10. |
| 5 | Raw và reranked được phân biệt; không sort reranked bằng raw prediction | ✅ **ĐẠT** | Có radio button chuyển đổi giữa hai chế độ; danh sách reranked luôn sắp xếp theo cột `rank` chuẩn. |
| 6 | Hiển thị RMSE/MAE/test coverage và metrics raw/reranked/popularity đúng số liệu | ✅ **ĐẠT** | Trang Dashboard hiển thị chính xác: RMSE = 0.8146, MAE = 0.6201, Coverage = 99.20%, P@10 (Raw: 0.0, Rerank: 0.0902, Pop: 0.1254). |
| 7 | Giữ raw P@10=0; ghi SAMPLE scope, confirmatory test và popularity cao hơn | ✅ **ĐẠT** | Dashboard và trang Recommend đều có hộp cảnh báo (Alert) màu vàng ghi rõ phạm vi `SAMPLE model (10.000 users)` và ghi chú Popularity baseline cao hơn. |
| 8 | Dashboard request chỉ đọc/cache/lọc dữ liệu; không fit ALS hoặc chạy pipelines | ✅ **ĐẠT** | `web/app.py` chỉ đọc dữ liệu từ bộ nhớ đệm JSON, hoàn toàn không gọi `ALSModel.fit` hay submit Spark job khi duyệt web. |
| 9 | Reload backend/trang không ghi đè HDFS artifacts và không làm mất dữ liệu | ✅ **ĐẠT** | Web Server chạy ở chế độ Read-Only đối với các file dữ liệu đầu vào. |
| 10 | Có hướng dẫn chạy TV4, ảnh/demo hoặc bằng chứng nghiệm thu của TV4 | ✅ **ĐẠT** | Đã hoàn thiện `HDSD_WEB_DEMO.md`, `run_web_demo.ps1` và bộ test tự động 10/10 PASS. |

---

## 5. Kết quả kiểm thử tự động (Unit Test Execution)

Lệnh thực hiện:
```powershell
python web/test_web.py
```

Kết quả:
```text
INFO:app:Loaded dataset_summary.json (10 records)
INFO:app:Loaded rating_distribution.json (10 records)
INFO:app:Loaded movie_popularity.json (20 records)
INFO:app:Loaded genre_statistics.json (19 records)
INFO:app:Loaded rating_trend.json (29 records)
INFO:app:Loaded tag_statistics.json (20 records)
INFO:app:Loaded user_activity_summary.json (3 records)
INFO:app:Loaded model_metrics.json (14 records)
INFO:app:Loaded ranking_comparison.json (3 records)
INFO:app:Loaded recommendations.json (20 records)
INFO:app:Loaded recommendations_raw.json (20 records)
INFO:app:Loaded demo_users.json (20 records)
..........
----------------------------------------------------------------------
Ran 10 tests in 0.077s

OK
```

---

## 6. Hướng dẫn chạy và tích hợp khi có cụm Spark

Khi cụm Hadoop/Spark Docker đang chạy, TV4 có thể đồng bộ mới nhất toàn bộ kết quả từ HDFS bằng lệnh:
```powershell
docker exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --deploy-mode client `
  --conf spark.driver.host=spark-master `
  --conf spark.driver.bindAddress=0.0.0.0 `
  /opt/spark-apps/tv4/export_for_web.py
```

Sau đó khởi động Web Demo bằng script:
```powershell
.\run_web_demo.ps1
```
Và truy cập: **`http://localhost:5000`**

---

## 7. Kết luận

Thành viên 4 (TV4) đã hoàn thành toàn diện công việc được giao:
- Xây dựng thành công ứng dụng Web Demo trực quan, sinh động, đáp ứng đầy đủ yêu cầu môn học Big Data.
- Tích hợp liền mạch kết quả từ cả 3 giai đoạn trước (TV1 → TV2 → TV3 → TV4).
- Tuân thủ các nguyên tắc khoa học dữ liệu, báo cáo trung thực các chỉ số đánh giá và giới hạn mô hình.
- Đóng gói tài liệu bàn giao, kiểm thử và hướng dẫn sử dụng đầy đủ, chuyên nghiệp.
