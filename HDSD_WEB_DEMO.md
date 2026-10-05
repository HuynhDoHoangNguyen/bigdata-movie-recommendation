# Hướng Dẫn Sử Dụng (HDSD) — Web Demo Gợi Ý Phim Big Data

Tài liệu hướng dẫn cài đặt, khởi chạy và kịch bản trình diễn (demo) cho module Web Demo của Thành viên 4 (**TV4 — Dashboard, Demo & Integration**) trong đề tài: **"Hệ thống gợi ý phim dựa trên dữ liệu lớn sử dụng Apache Spark và thuật toán ALS (MovieLens 32M)"**.

---

## 1. Yêu cầu môi trường

Để chạy Web Demo, máy tính chỉ cần:
- **Python 3.10+** (đã kiểm thử tốt trên Python 3.11, 3.12, 3.13)
- Gói thư viện **Flask**: `pip install flask` (hoặc `python -m pip install -r web/requirements.txt`)
- Trình duyệt hiện đại (Chrome, Edge, Firefox, Safari) hỗ trợ HTML5, CSS3, JavaScript.

> *Lưu ý*: Nhờ thiết kế tách biệt (decoupled architecture) giữa tầng Big Data (Hadoop HDFS / Spark) và tầng Web Serving (Flask in-memory JSON cache), **Web Demo có thể chạy độc lập ngay lập tức mà không bắt buộc phải bật toàn bộ cụm Hadoop/Spark Docker 7 containers**. Khi cụm Docker hoạt động, script `spark/tv4/export_for_web.py` sẽ làm mới dữ liệu từ HDFS sang Web.

---

## 2. Cách khởi chạy nhanh (3 bước)

### Bước 1: Mở PowerShell tại thư mục gốc của đồ án
```powershell
cd "d:\HOANGVU\TAI LIEU HOC PHAN\Nam04 (2026-2027)\HK1\BigData\Doan\bigdata-movie-recommendation"
```

### Bước 2: Chạy script tự động 1-click
```powershell
.\run_web_demo.ps1
```
*(Hoặc chạy trực tiếp: `python web/app.py`)*

### Bước 3: Truy cập Web Demo
Mở trình duyệt và truy cập:
👉 **[http://localhost:5000](http://localhost:5000)**

---

## 3. Hướng dẫn sử dụng các trang chức năng

### 3.1. Trang 1: Dashboard tổng quan (`/`)
*Mục tiêu: Cung cấp bức tranh toàn cảnh về quy mô dữ liệu, hạ tầng xử lý và hiệu năng mô hình gợi ý.*

1. **Thẻ thống kê quy mô Big Data**:
   - **32.000.204 Ratings**: Tổng số bản ghi đánh giá đã được kiểm tra chất lượng ở TV1 và tiền xử lý ở TV2.
   - **200.948 Users**: Số lượng người dùng duy nhất.
   - **87.585 Movies**: Danh mục phim đầy đủ.
   - **28 năm dữ liệu**: Khoảng thời gian từ 1995 đến 2023.
2. **Kiến trúc luồng xử lý (Pipeline Flow)**:
   - Thể hiện rõ 6 mắt xích: `MovieLens 32M → HDFS (Hadoop) → Spark DataFrame → Tiền xử lý & EDA → ALS Model → Web Demo`.
3. **Thông tin mô hình Spark MLlib ALS**:
   - Thể hiện các siêu tham số huấn luyện: `rank=10`, `maxIter=8`, `regParam=0.05`.
   - Sai số dự đoán: **RMSE = 0.8146**, **MAE = 0.6201**, độ bao phủ (coverage): **99.20%**.
   - Cảnh báo rõ ràng: Kết quả đánh giá thuộc tập `SAMPLE (10.000 users)`.
4. **Bảng so sánh phương pháp xếp hạng (Ranking Comparison Top-10)**:
   - **Raw ALS**: P@10 = 0.0000, R@10 = 0.0000, Hits = 0 (do thiên kiến phim ngách ít dữ liệu).
   - **ALS + Support Shrinkage (Reranked)**: P@10 = 0.0902, R@10 = 0.0705, Hits = 451.
   - **Popularity Baseline**: P@10 = 0.1254, R@10 = 0.1004, Hits = 627.

---

### 3.2. Trang 2: Phân tích dữ liệu EDA (`/analytics`)
*Mục tiêu: Trực quan hóa 5 khía cạnh cốt lõi của tập dữ liệu lớn bằng biểu đồ tương tác Chart.js.*

1. **Phân bố Rating (Histogram)**:
   - Hiển thị 10 mức điểm (0.5 đến 5.0).
   - Điểm đánh giá phổ biến nhất là **4.0** (chiếm 26.15% với hơn 8.36 triệu lượt đánh giá).
2. **Xu hướng Rating theo năm (Line Chart)**:
   - Đường biểu diễn số lượng đánh giá và điểm trung bình biến động qua 29 năm (1995–2023).
3. **Top 20 phim phổ biến nhất (Horizontal Bar Chart)**:
   - Liệt kê các phim kinh điển có số lượt đánh giá cao nhất: *Forrest Gump (1994)*, *The Shawshank Redemption (1994)*, *Pulp Fiction (1994)*, *The Silence of the Lambs (1991)*, *The Matrix (1999)*,...
4. **Phân tích 21 Thể loại phim (Dual-Axis Chart)**:
   - Trục cột: Số lượt đánh giá của từng thể loại (*Drama*, *Comedy*, *Action*, *Thriller* đứng đầu).
   - Trục đường: Điểm đánh giá trung bình của từng thể loại (*Film-Noir*, *War*, *Documentary* có điểm TB cao nhất).
5. **Top 20 Tags phổ biến (Bar Chart)**:
   - Trực quan hóa các nhãn từ vựng được người dùng gắn nhiều nhất (*sci-fi*, *based on a book*, *atmospheric*, *superhero*,...).

---

### 3.3. Trang 3: Gợi ý phim cho người dùng (`/recommend`)
*Mục tiêu: Trình diễn khả năng gợi ý Top-N phim cá nhân hóa theo từng User ID cụ thể.*

1. **Cách xem gợi ý cho một User**:
   - Chọn một User ID trong menu dropdown (hoặc gõ User ID vào ô nhập).
   - Hệ thống tự động chọn mặc định **User 806** ngay khi vừa mở trang.
   - Bấm nút **"🔍 Gợi ý"** (hoặc nhấn phím *Enter*).
2. **Xem danh sách Top-10 phim gợi ý**:
   - Mỗi bộ phim được hiển thị dưới dạng thẻ (Card) gồm:
     * Thứ hạng gợi ý (`#1` đến `#10`).
     * Tên phim và năm sản xuất.
     * Các thể loại phim (dưới dạng nhãn badge).
     * Điểm gợi ý đã hiệu chỉnh (**Adjusted Score**).
     * Độ phổ biến trong tập huấn luyện (**Train Support**).
     * Điểm dự đoán thô ban đầu (**ALS Raw Prediction**).
3. **Chuyển đổi chế độ (Reranked vs Raw ALS)**:
   - Chọn radio button **"Reranked (đề xuất)"**: Xem danh sách phim đã được áp dụng thuật toán co rút Bayesian Shrinkage (ưu tiên phim chất lượng và có độ tin cậy thống kê cao).
   - Chọn radio button **"Raw ALS (đối chiếu)"**: Xem danh sách phim thuần túy theo điểm dự đoán của ALS (giúp minh họa lý do tại sao một số phim ngách ít dữ liệu bị đẩy lên đầu nếu không rerank).
4. **Xử lý khi nhập User ID ngoài phạm vi demo**:
   - Nếu nhập một mã người dùng bất kỳ (ví dụ: `999999`), hệ thống sẽ xuất hiện thông báo lịch sự, giải thích giới hạn của bản demo và cung cấp sẵn 20 nút bấm nhanh để chọn các user hợp lệ.

---

## 4. Kịch bản trình diễn khi Thuyết trình (Demo Flow)

| Bước | Thời gian | Thao tác thực hiện | Nội dung thuyết minh tương ứng |
|---|---|---|---|
| **1. Khởi động** | 30s | Mở terminal gõ `.\run_web_demo.ps1`, mở trình duyệt `localhost:5000` | Giới thiệu hệ thống được đóng gói tự động, có kiểm thử 10/10 test cases trước khi chạy web. |
| **2. Giới thiệu tổng thể** | 1 phút | Mở trang **Dashboard (`/`)** | Nhấn mạnh quy mô Big Data: 32 triệu ratings, 200.000 users, xử lý qua cụm Hadoop HDFS 3 DataNodes và Apache Spark phân tán. |
| **3. Giải thích Pipeline** | 1 phút | Cuộn xuống phần **Kiến trúc Pipeline** | Nêu rõ phân công 4 thành viên: TV1 (Hạ tầng, Standard Parquet), TV2 (Làm sạch, EDA), TV3 (Mô hình Spark ALS), TV4 (Tích hợp, Web Demo). |
| **4. Trực quan hóa EDA** | 1.5 phút | Chuyển sang trang **Phân tích (`/analytics`)** | Chỉ ra các phát hiện thú vị: Mức điểm 4.0 chiếm ưu thế lớn nhất; Thể loại Drama và Comedy áp đảo số lượt xem; các bộ phim kinh điển 1994 (Forrest Gump, Shawshank) giữ kỷ lục tương tác. |
| **5. Demo gợi ý phim** | 2 phút | Chuyển sang trang **Gợi ý phim (`/recommend`)** | - Thao tác với **User 806**: hiển thị 10 phim xuất sắc được cá nhân hóa.<br>- Chuyển sang **Raw ALS** để chứng minh hiện tượng overfitting/cold-start ở phim ngách.<br>- Chuyển lại **Reranked** để chứng minh thuật toán Shrinkage khắc phục triệt để vấn đề.<br>- Thử chọn thêm User `54836` và User `103269` để thấy danh mục phim thay đổi theo sở thích từng người. |
| **6. Kết luận & Q&A** | 1 phút | Quay lại Dashboard, mở bảng so sánh phương pháp | Khẳng định tính trung thực học thuật (ghi nhận Popularity và ALS Reranked), đề xuất hướng mở rộng huấn luyện Full Data trên cụm máy chủ lớn. |

---

## 5. Danh sách 20 Demo Users hợp lệ

Hệ thống đã chuẩn bị sẵn kết quả Top-10 gợi ý cho 20 người dùng sau:

| STT | User ID | STT | User ID | STT | User ID | STT | User ID |
|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| 1 | **806** (Mặc định) | 6 | **71795** | 11 | **113006** | 16 | **143637** |
| 2 | **9770** | 7 | **100346** | 12 | **115392** | 17 | **172400** |
| 3 | **13769** | 8 | **103252** | 13 | **126896** | 18 | **179925** |
| 4 | **54836** | 9 | **103269** | 14 | **128517** | 19 | **183649** |
| 5 | **56041** | 10 | **108505** | 15 | **131122** | 20 | **192892** |

---

## 6. Xử lý sự cố thường gặp (Troubleshooting)

1. **Cổng 5000 đã bị chiếm dụng**:
   - Mở file `web/app.py`, sửa dòng cuối cùng: `app.run(host='0.0.0.0', port=5001, debug=True)`.
2. **Trang web không tải được biểu đồ Chart.js**:
   - Kiểm tra kết nối Internet để trình duyệt tải thư viện Bootstrap 5 và Chart.js từ CDN.
3. **Lỗi thiếu thư viện Flask**:
   - Chạy lệnh: `python -m pip install -r web/requirements.txt`.
