# Phân công công việc Dự án Data Lakehouse (Nhóm 6)

Dựa trên kiến trúc dự án với Delta Lake, Spark, Airflow, AWS (S3, Glue, Athena) và PowerBI, công việc được chia phân bổ thành 4 giai đoạn chính để chia đều cho 4 thành viên như sau:

## 🧑‍💻 Thành viên 1: Infrastructure, Orchestration & Metadata
**Vai trò:** Thiết lập hạ tầng và hệ thống điều phối pipeline.
- **Hạ tầng (Docker):** Thiết lập, tối ưu và bảo trì cấu hình Docker/Docker Compose gồm các dịch vụ Apache Spark, Airflow, Jupyter Notebook, cơ sở dữ liệu.
- **Hạ tầng Đám mây:** Cấu hình lưu trữ AWS S3 và hệ thống quản lý siêu dữ liệu (Metadata) với AWS Glue Data Catalog.
- **Điều phối (Airflow):** Viết các script tạo Airflow DAGs nhằm lập lịch trình (schedule) và quản lý luồng thực thi (dependencies) cho các Spark jobs.
- **Giám sát:** Đảm bảo toàn bộ hệ thống cluster chạy ổn định và debug các luồng chạy bị lỗi.

## 🧑‍💻 Thành viên 2: Data Ingestion & Bronze Layer (Dữ liệu thô)
**Vai trò:** Xây dựng luồng kéo dữ liệu gốc (Olist dataset, API) vào hệ thống.
- **Data Ingestion:** Viết Spark application/scripts để thu thập dữ liệu từ các nguồn cung cấp (File cục bộ, API...).
- **Chiến lược nạp:** Xây dựng cơ chế nạp toàn bộ dữ liệu lần đầu (Full load) và nạp dữ liệu gia tăng (Incremental load).
- **Bronze Layer (Raw Data):** Ghi toàn bộ dữ liệu thô (giữ nguyên gốc không chỉnh sửa) vào S3 dưới định dạng Delta Table.
- **Kiểm định:** Đối chiếu đảm bảo dữ liệu ghi vào không bị mất mát hay sai lệch so với nguồn.

## 🧑‍💻 Thành viên 3: Data Transformation & Silver Layer (Làm sạch dữ liệu)
**Vai trò:** Chuẩn hóa, xử lý và làm sạch dữ liệu.
- **Transform (Apache Spark):** Xây dựng các Spark jobs để đọc dữ liệu từ Bronze Layer.
- **Làm sạch (Cleaning):** Loại bỏ dữ liệu rác, trùng lặp (deduplication), xử lý các giá trị NaN/Null, và làm mịn dữ liệu.
- **Chuẩn hóa (Standardization):** Chuẩn hóa định dạng (schema, ngày tháng, kiểu chuỗi) theo quy chuẩn nghiệp vụ.
- **Silver Layer:** Chuyển đổi và lưu trữ bản ghi chất lượng cao này trở lại S3 dưới dạng Delta Table làm nguồn dữ liệu đáng tin cậy.

## 🧑‍💻 Thành viên 4: Data Modeling, Query Engine & Dashboard (Tầng phục vụ phân tích)
**Vai trò:** Xây dựng mô hình dữ liệu tổng hợp và trực quan hóa lên báo cáo.
- **Gold Layer (Fact & Dimension Tables):** Tổng hợp dữ liệu từ Silver Layer, mô hình hóa theo dạng Fact/Dim dựa trên các chỉ số (metrics) cần tính toán; lưu dữ liệu vào Gold Layer (Delta format).
- **Trích xuất & Truy vấn:** Kết nối và cấu hình Amazon Athena làm Query Engine để truy vấn dữ liệu trực tiếp trên Data Lake S3 thông qua Glue Catalog.
- **Consumption (Power BI):** Kết nối Power BI với tập dữ liệu sạch/Gold layer (thông qua Athena) để tạo các biểu đồ, dashboards và báo cáo kinh doanh.
- **Tối ưu:** Tối ưu hóa để cải thiện tốc độ truy cập báo cáo (như thiết lập Partitioning khi ghi dữ liệu Gold).

---
### 📋 Nguyên tắc phối hợp chung
- Cả nhóm sử dụng chung Git để quản lý source code (tạo branch riêng cho mỗi task).
- Review code tập thể ở các pipeline nối tiếp nhau (Ví dụ TV2 chuyển giao cho TV3).
- Cùng nhau hỗ trợ gỡ lỗi và kiểm thử toàn vẹn dữ liệu từ đầu vào tới báo cáo cuối.
