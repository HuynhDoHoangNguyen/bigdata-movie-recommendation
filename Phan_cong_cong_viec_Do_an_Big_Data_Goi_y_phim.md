# Phân công công việc Đồ án Big Data – Gợi ý phim

**MovieLens 32M • Hadoop HDFS • Apache Spark • Spark MLlib ALS • Dashboard/Web Demo**

> Mục tiêu: chia rõ phạm vi cho 4 thành viên, đầu ra của người trước là đầu vào cho người sau; mỗi người chịu trách nhiệm code + kiểm thử + tài liệu cho module của mình. Tài liệu này không ghi ngày, tuần hoặc số giờ.

## 1. Luồng triển khai chung

**MovieLens 32M → HDFS → Spark DataFrame → Tiền xử lý & EDA → ALS → Đánh giá → Top-N Recommendation → Dashboard/Web Demo**

## 2. Bảng phân công tổng quát

| **Thành viên** | **Module phụ trách**              | **Công việc trọng tâm**                                                                                      | **Đầu ra bàn giao**                                                              | **Phối hợp chính**                                                       |
|----------------|-----------------------------------|--------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------|--------------------------------------------------------------------------|
| Thành viên 1   | Data & Big Data Infrastructure    | Chuẩn bị MovieLens 32M; HDFS; Spark; kiểm tra schema/chất lượng dữ liệu; tạo lớp dữ liệu chuẩn dùng chung.   | HDFS dataset + script ingest + Spark DataFrame chuẩn + thống kê dữ liệu ban đầu. | → Bàn giao dữ liệu sạch/chuẩn cho TV2 và TV3.                            |
| Thành viên 2   | Data Processing & Analytics       | Làm sạch, biến đổi dữ liệu bằng PySpark; EDA; thống kê user/movie/rating/genre/tag; tạo tập dữ liệu cho ALS. | Pipeline preprocessing + bảng/biểu đồ EDA + train/test data.                     | Nhận từ TV1 → bàn giao dataset mô hình cho TV3; số liệu cho TV4.         |
| Thành viên 3   | Recommendation Model & Evaluation | Xây dựng Collaborative Filtering bằng Spark MLlib ALS; thử tham số; đánh giá; tạo Top-N cho user.            | ALS model + kết quả RMSE/MAE/Top-K + file/bảng recommendation.                   | Nhận từ TV2 → bàn giao kết quả/model/API data cho TV4.                   |
| Thành viên 4   | Dashboard, Demo & Integration     | Xây dashboard/web demo; hiển thị thống kê, biểu đồ và Top-N; tích hợp output Spark/ALS; chuẩn hóa demo.      | Dashboard/web chạy được + luồng demo hoàn chỉnh + hướng dẫn chạy tích hợp.       | Nhận số liệu TV2 + recommendation TV3; phối hợp toàn nhóm kiểm thử cuối. |

## 3. Công việc chi tiết của từng thành viên

### THÀNH VIÊN 1 — DATA & BIG DATA INFRASTRUCTURE

- Tải và tổ chức bộ MovieLens 32M; xác định các file sử dụng chính như ratings, movies, tags và liên kết dữ liệu cần thiết.
- Khảo sát cấu trúc dữ liệu: cột, kiểu dữ liệu, khóa liên kết, số lượng bản ghi và ý nghĩa từng thuộc tính.

- Thiết lập Hadoop/HDFS và Apache Spark/PySpark cho môi trường chạy của nhóm.
- Đưa dữ liệu gốc lên HDFS; tổ chức thư mục input/output rõ ràng để các thành viên khác dùng chung.

- Viết script đọc dữ liệu từ HDFS bằng Spark DataFrame; khai báo/kiểm tra schema và kiểu dữ liệu.
- Kiểm tra dữ liệu thiếu, trùng lặp, giá trị rating không hợp lệ, movieId/userId lỗi và các vấn đề dữ liệu cơ bản.

- Tạo lớp dữ liệu chuẩn ban đầu để TV2 tiếp tục tiền xử lý; không tự làm phần ALS để tránh chồng việc.
- Ghi lại lệnh cài đặt/chạy HDFS–Spark, cấu trúc thư mục và cách nạp dữ liệu để phục vụ phần hướng dẫn chạy.

- Chuẩn bị nội dung báo cáo thuộc phần: dataset, đặc trưng Big Data, kiến trúc lưu trữ/xử lý và môi trường thực nghiệm.

**Đầu ra cần bàn giao: dữ liệu trên HDFS + script ingest/read + DataFrame chuẩn ban đầu + thống kê quy mô/schema + hướng dẫn môi trường.**

### THÀNH VIÊN 2 — DATA PROCESSING & ANALYTICS

- Nhận DataFrame chuẩn từ TV1 và xây pipeline tiền xử lý hoàn toàn bằng Spark DataFrame/PySpark.
- Xử lý null, duplicate, dữ liệu không nhất quán; ép kiểu và chuẩn hóa các trường cần thiết.

- Join ratings với movies và các dữ liệu liên quan khi cần cho phân tích/hiển thị.
- Thực hiện EDA: số user, số movie, số rating; phân bố rating; số lượt đánh giá theo user/movie.

- Phân tích phim phổ biến, thể loại phổ biến, xu hướng rating và một số thống kê có giá trị cho dashboard.
- Chuẩn bị dữ liệu đúng định dạng userId–movieId–rating cho Spark MLlib ALS.

- Chia dữ liệu train/test theo một cách nhất quán; cố định seed nếu dùng randomSplit để có thể tái lập kết quả.
- Xuất các bảng thống kê/dataset trung gian mà TV4 có thể dùng trực tiếp cho dashboard.

- Chuẩn bị nội dung báo cáo thuộc phần: tiền xử lý, phân tích dữ liệu, mô tả pipeline và kết quả EDA.

**Đầu ra cần bàn giao: preprocessing pipeline + dữ liệu train/test cho ALS + bảng thống kê/EDA + dữ liệu dashboard.**

### THÀNH VIÊN 3 — RECOMMENDATION MODEL & EVALUATION

- Nhận dữ liệu train/test từ TV2; kiểm tra schema userId–movieId–rating trước khi huấn luyện.
- Xây mô hình Collaborative Filtering bằng ALS trong Spark MLlib.

- Thiết lập các tham số quan trọng như rank, maxIter, regParam; xử lý cold-start phù hợp khi đánh giá.
- Thực hiện nhiều cấu hình tham số có kiểm soát để chọn mô hình sử dụng cho demo.

- Đánh giá mô hình bằng RMSE; bổ sung MAE và/hoặc chỉ số Top-K như Precision@K/Recall@K nếu triển khai phù hợp.
- Phân tích kết quả: cấu hình, chỉ số, ưu/nhược điểm và các trường hợp mô hình gợi ý chưa tốt.

- Tạo chức năng lấy Top-N phim cho một userId; ánh xạ movieId sang title/genre để kết quả dễ hiểu.
- Lưu model hoặc xuất kết quả recommendation theo định dạng thống nhất để TV4 tích hợp.

- Chuẩn bị nội dung báo cáo thuộc phần: ALS, Collaborative Filtering, thực nghiệm, tham số và đánh giá mô hình.

**Đầu ra cần bàn giao: ALS model + bảng thực nghiệm/metrics + hàm Top-N + recommendation data có tên phim/thể loại.**

### THÀNH VIÊN 4 — DASHBOARD, WEB DEMO & INTEGRATION

- Nhận dữ liệu thống kê từ TV2 và kết quả Top-N/model output từ TV3.
- Thiết kế giao diện demo tối giản, tập trung đúng bài toán: nhập/chọn userId → hiển thị danh sách phim gợi ý.

- Hiển thị các thông tin phim cần thiết như title, genre và điểm dự đoán/xếp hạng nếu phù hợp.
- Xây dashboard trực quan: quy mô dữ liệu, phân bố rating, phim/thể loại phổ biến và kết quả đánh giá mô hình.

- Tổ chức lớp tích hợp để dashboard đọc output Spark/ALS ổn định; ưu tiên cách triển khai dễ chạy khi thuyết trình.
- Thêm xử lý trường hợp userId không tồn tại hoặc không có recommendation để demo không bị lỗi.

- Kiểm thử luồng end-to-end: dữ liệu → xử lý → model → recommendation → giao diện.
- Chuẩn hóa README/hướng dẫn chạy demo tích hợp và kịch bản trình diễn sản phẩm.

- Tổng hợp hình ảnh kết quả chạy phục vụ báo cáo/slide; phối hợp cả nhóm rà soát source code và demo cuối.

**Đầu ra cần bàn giao: dashboard/web demo chạy được + tích hợp recommendation + biểu đồ + README/kịch bản demo.**

## 4. Trình tự công việc (không gắn thời gian)

**Giai đoạn A — Chuẩn bị dữ liệu & nền tảng**: TV1 hoàn thành môi trường, HDFS, ingest và DataFrame chuẩn. TV2 có thể bắt đầu khảo sát schema ngay khi dữ liệu đọc được.

**Giai đoạn B — Tiền xử lý & phân tích**: TV2 hoàn thiện cleaning/EDA và tạo train/test. TV4 có thể bắt đầu dựng khung dashboard bằng dữ liệu mẫu/thống kê đã có.

**Giai đoạn C — Xây dựng mô hình**: TV3 huấn luyện ALS, đánh giá và xuất Top-N. TV2 hỗ trợ xác minh dữ liệu đầu vào khi metrics bất thường.

**Giai đoạn D — Tích hợp sản phẩm**: TV4 tích hợp thống kê + recommendation; TV3 hỗ trợ format output/model; TV1 bảo đảm đường dẫn dữ liệu/môi trường chạy thống nhất.

**Giai đoạn E — Kiểm thử & hoàn thiện hồ sơ**: Cả nhóm chạy end-to-end, sửa lỗi, chụp kết quả; mỗi người hoàn thiện phần báo cáo đúng module; sau đó ghép báo cáo, slide và hướng dẫn chạy.

## 5. Điểm giao tiếp giữa các module

| **Bàn giao**          | **Người giao** | **Người nhận** | **Nội dung phải thống nhất**                                              |
|-----------------------|----------------|----------------|---------------------------------------------------------------------------|
| Data layer            | TV1            | TV2            | Tên file/thư mục HDFS, schema, kiểu dữ liệu, dữ liệu đã loại lỗi cơ bản.  |
| Model dataset         | TV2            | TV3            | Các cột userId, movieId, rating; train/test; seed; quy tắc lọc dữ liệu.   |
| Analytics data        | TV2            | TV4            | Tên bảng/file output, trường dữ liệu, ý nghĩa từng metric/biểu đồ.        |
| Recommendation output | TV3            | TV4            | userId, movieId, title/genres, prediction/rank; cách gọi hoặc đọc output. |
| Final integration     | TV4            | Cả nhóm        | Lệnh chạy, cấu hình, luồng demo, lỗi thường gặp và cách kiểm tra kết quả. |

## 6. Checklist hoàn thành chung

- [ ] Source code được chia thư mục/module rõ ràng, không để một notebook duy nhất chứa toàn bộ đồ án.
- [ ] MovieLens 32M được đọc/xử lý bằng công nghệ Big Data đã chọn; thể hiện rõ HDFS và Spark trong luồng chạy.
- [ ] Có pipeline tiền xử lý và phân tích dữ liệu tái chạy được.
- [ ] Có mô hình ALS, kết quả đánh giá và chức năng Top-N recommendation.
- [ ] Có dashboard/web demo thể hiện cả phân tích dữ liệu và kết quả gợi ý.
- [ ] Có báo cáo Word, slide, hướng dẫn cài đặt/chạy và bảng phân công mức độ đóng góp.
- [ ] Mỗi thành viên nắm được module của mình và hiểu đầu vào/đầu ra của module liền kề để thuyết trình không bị rời rạc.

**Nguyên tắc triển khai: rõ phạm vi • có đầu ra bàn giao • dễ tích hợp • demo được • bám yêu cầu môn học**
