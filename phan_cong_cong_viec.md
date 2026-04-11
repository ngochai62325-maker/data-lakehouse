# Phân công công việc Dự án Data Lakehouse (Nhóm 6)

Dựa trên kiến trúc dự án với Delta Lake, Spark, Airflow, AWS (S3, Glue, Athena) và PowerBI, công việc được phân bổ thành 4 giai đoạn chính để chia đều cho 4 thành viên như sau:

## 🧑‍💻 Thành viên 1: Infrastructure & Bronze Layer (Dữ liệu Raw)
**Vai trò:** Xây dựng nền tảng và nạp dữ liệu bước đầu vào hệ thống.
- **Hạ tầng (Docker & AWS):** Cài đặt môi trường chạy lập lịch (Docker Compose) chứa Spark, Airflow, Jupyter. Khởi tạo kho S3 bucket và AWS Glue Data Catalog cho cả nhóm xài.
- **Bronze Layer (Ingestion):** Viết Spark job kéo data thô từ Olist dataset vào S3 (chuyển sang định dạng Delta Table). Đảm bảo cơ chế lấy dữ liệu toàn bộ và gia tăng.
- **Airflow DAG (Bronze):** Lập trình file DAG bằng Python để tự động hóa/hẹn giờ chạy luồng Ingestion của chính mình.

## 🧑‍💻 Thành viên 2: Data Transformation & Silver Layer (Làm sạch dữ liệu)
**Vai trò:** (Data Engineer) Trực tiếp chuẩn hóa và lọc sạch dữ liệu mỏ.
- **Silver Layer (Cleaned Data):** Nhận dữ liệu Delta từ bảng Bronze, viết các module Spark chuyên làm sạch (xử lý null, bỏ trùng lặp rác, ép kiểu chuẩn về định dạng String/Timestamp).
- **Quản lý chất lượng:** Đảm bảo dữ liệu luân chuyển vào Silver đạt mức hoàn hảo để làm "Single Source of Truth" (Nguồn dữ liệu chân lý).
- **Airflow DAG (Silver):** Viết DAG kết nối tiếp nối với thành viên 1. Tự động kích hoạt job xử lý Silver ngay khi job Bronze kết thúc.

## 🧑‍💻 Thành viên 3: Data Modeling & Gold Layer (Mô hình dữ liệu)
**Vai trò:** (Data Architect) Thiết kế mô hình chuẩn hóa phục vụ phân tích.
- **Gold Layer (Fact & Dimension):** Thực hiện Join các bảng Silver lại với nhau, phân rã và thiết kế theo dạng Star Schema (bảng lịch sử sự kiện Fact và các nhánh Dimension bổ trợ).
- **Tối ưu hóa (Optimization):** Cải thiện tốc độ bằng cách ứng dụng Spark Partitioning, Repartition và Z-Ordering cho các bảng Gold trước khi lưu vào S3.
- **Airflow DAG (Gold):** Viết DAG tự động kích hoạt job Gold ngay khi Silver đã xong, là mắt xích kết nối giữa Silver và Platinum trong chuỗi pipeline.

## 🧑‍💻 Thành viên 4: Platinum Layer, Query & Power BI (Tầng Phân tích)
**Vai trò:** (Data Analytics/BI) Tổng hợp số liệu cuối và Trực quan hóa thành báo cáo kinh doanh.
- **Platinum Layer – Tạo Data Mart (BI-Ready):** Viết Spark job đọc dữ liệu từ các bảng Gold (Star Schema), thực hiện tổng hợp/tính toán sẵn các chỉ số kinh doanh (ví dụ: phân nhóm RFM khách hàng, tổng doanh thu theo khu vực, tỷ lệ giao hàng đúng hạn...) và lưu kết quả ra các **bảng Mart** dạng Delta Table trên S3 (tầng Platinum). Đây là bước tạo Data Mart thực sự để BI đọc siêu nhanh.
- **Airflow DAG (Platinum):** Viết DAG kết nối tiếp nối với thành viên 3. Tự động kích hoạt Spark job tạo bảng Mart ngay khi job Gold hoàn tất, hoàn chỉnh chuỗi Pipeline: Bronze → Silver → Gold → Platinum.
- **Query Engine (Athena):** Đảm bảo Athena và AWS Glue map chuẩn schema của các bảng Platinum trên S3, sẵn sàng kết nối qua JDBC cho PowerBI.
- **Dashboard PowerBI:** Nối PowerBI thẳng vào tầng Platinum (đã tính toán sẵn). Dùng DAX vẽ Dashboard và thuyết trình mạch lạc 4 mục tiêu phân tích kinh doanh.

---
### 📋 Nguyên tắc phối hợp chung
- Cả nhóm sử dụng chung Git để quản lý source code (tạo branch riêng cho mỗi task).
- Review code tập thể ở các pipeline nối tiếp nhau (Ví dụ TV2 hoàn tất Sensor/DAG để DAG của TV3 chạy tiếp).
- Cùng nhau hỗ trợ gỡ lỗi và kiểm thử toàn vẹn dữ liệu từ đầu vào tới báo cáo cuối.
