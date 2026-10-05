# MovieLens 32M Big Data Recommendation — Web Demo (TV4)

Ứng dụng Web Demo trực quan hóa dữ liệu lớn và trình diễn hệ thống gợi ý phim sử dụng Apache Spark MLlib ALS và MovieLens 32M.

---

## 1. Cấu trúc thư mục `web/`

```text
web/
├── app.py                  # Backend Flask (Routing, REST API, JSON in-memory cache)
├── populate_data.py        # Script nạp dữ liệu mẫu/thống kê chuẩn
├── test_web.py             # Bộ kiểm thử tự động 10/10 test cases
├── requirements.txt        # Thư viện phụ thuộc (flask)
├── Dockerfile              # Dockerfile chạy ứng dụng độc lập
├── data/                   # 12 file JSON lưu trữ dữ liệu đã xuất từ HDFS
│   ├── dataset_summary.json
│   ├── rating_distribution.json
│   ├── movie_popularity.json
│   ├── genre_statistics.json
│   ├── rating_trend.json
│   ├── tag_statistics.json
│   ├── user_activity_summary.json
│   ├── model_metrics.json
│   ├── ranking_comparison.json
│   ├── demo_users.json
│   ├── recommendations.json
│   └── recommendations_raw.json
├── static/
│   ├── css/style.css       # Giao diện responsive, hiện đại
│   └── js/main.js          # Khởi tạo biểu đồ Chart.js và tương tác gọi API
└── templates/
    ├── base.html           # Layout chuẩn Bootstrap 5
    ├── index.html          # Trang chủ Dashboard: thống kê, pipeline, mô hình ALS
    ├── analytics.html      # Trang Phân tích EDA (5 biểu đồ tương tác)
    └── recommend.html      # Trang Gợi ý phim cho 20 demo users
```

---

## 2. Hướng dẫn chạy nhanh

### Cách 1: Chạy trực tiếp trên máy Host (Khuyến nghị, nhanh nhất)

```powershell
# Di chuyển vào thư mục web (hoặc từ root)
python web/app.py
```
Mở trình duyệt: **[http://localhost:5000](http://localhost:5000)**

### Cách 2: Chạy kiểm thử tự động

```powershell
python web/test_web.py
```

### Cách 3: Chạy qua Docker

```powershell
docker build -t movie-web-demo ./web
docker run -p 5000:5000 movie-web-demo
```

---

## 3. Các chức năng chính

1. **Dashboard (`/`)**:
   - Thẻ thống kê: **32.000.204** Ratings, **200.948** Users, **87.585** Phim, **28** năm lịch sử (1995–2023).
   - Kiến trúc Pipeline 6 giai đoạn rõ ràng.
   - Bảng thông tin mô hình ALS (RMSE = 0.8146, MAE = 0.6201, coverage 99.20%).
   - Bảng so sánh 3 phương pháp xếp hạng (Raw ALS, Reranked ALS, Popularity Baseline).

2. **Phân tích EDA (`/analytics`)**:
   - Biểu đồ phân bố Rating (cột).
   - Xu hướng rating theo năm (đường).
   - Top 20 phim phổ biến nhất (thanh ngang).
   - Phân tích 21 thể loại phim (kết hợp cột + đường).
   - Top 20 Tags được người dùng gắn nhiều nhất.

3. **Gợi ý phim (`/recommend`)**:
   - Mặc định tải sẵn **User 806** (Top-10 phim kèm Adjusted Score & Support).
   - Dropdown chọn nhanh 20 demo users được TV3 chỉ định.
   - Chuyển đổi giữa chế độ **Reranked (đề xuất)** và **Raw ALS (đối chiếu)**.
   - Xử lý khi nhập User ID ngoài danh sách (hiển thị danh sách 20 user hợp lệ).
