# Project Data Engineering (Nhóm 6)

**Tên đề tài:** "Xây dựng hệ thống Data Lakehouse phân tích dữ liệu bán hàng và hành vi khách hàng trên tập dữ liệu thương mại điện tử Olist"

**Mô tả tổng quan:** Ứng dụng kiến trúc Medallion (Bronze - Silver - Gold) kết hợp với các công cụ như Apache Spark, Apache Airflow, Amazon S3, Delta Lake, AWS Athena và Power BI để xây dựng luồng xử lý dữ liệu (ETL Pipeline) tự động quy mô lớn.

## Cấu trúc thư mục
- `etl_pipeline/`: Chứa các Spark jobs xử lý dữ liệu theo tầng Medallion (bronze, silver, gold).
- `dags/`: Chứa các Airflow DAGs để lập lịch và điều phối luồng dữ liệu (ETL pipeline).
- `docker/`: Chứa Dockerfile và cấu hình cho các dịch vụ (Spark, Airflow).
- `dataset/`: Chứa bộ dữ liệu gốc của Olist (các tệp CSV).
- `config/`: Configuration files cho Spark, Airflow hoặc các dịch vụ khác.
- `notebooks/`: Chứa các Jupyter notebooks dùng để phát triển, thử nghiệm Spark jobs.
- `docker-compose.yml`: Triển khai hạ tầng container hóa.
- `.env`: Tệp cấu hình các biến môi trường cho hệ thống.
- `requirements.txt`: Danh sách các thư viện Python cần thiết.

## Bài toán Phân tích Dữ liệu (Analytical Objectives)

Dựa trên bộ dữ liệu **Olist E-commerce** (Brazil), hệ thống Data Lakehouse này sẽ tiến hành xử lý và xây dựng các mô hình dữ liệu (tầng Gold) để giải quyết các bài toán kinh doanh sau:

1. **Phân tích Doanh thu & Tăng trưởng:**
   - Theo dõi sự biến động của doanh thu và số lượng đơn đặt hàng theo thời gian (tháng, quý, năm).
   - Phân tích các yếu tố thúc đẩy doanh thu chính theo khu vực địa lý tại Brazil và theo danh mục sản phẩm.

2. **Đánh giá Hiệu suất Giao hàng:**
   - So sánh thời gian giao hàng thực tế với thời gian giao hàng dự kiến.
   - Phân tích ảnh hưởng của việc giao hàng chậm trễ đến mức độ hài lòng của khách hàng (thông qua điểm đánh giá - Review Scores).

3. **Hiệu suất Sản phẩm & Người bán:**
   - Xác định danh sách các sản phẩm mang lại doanh thu cao nhất và thấp nhất.
   - Xếp hạng hiệu quả của Người bán dựa trên khối lượng đơn hàng và đánh giá từ khách hàng.

4. **Hành vi Khách hàng & Phân khúc RFM:**
   - Khám phá hành vi mua sắm và sự tập trung của khách hàng theo các tiểu bang/thành phố.
   - Áp dụng mô hình RFM (Recency, Frequency, Monetary) để nhận diện khách hàng VIP, khách hàng mới và khách hàng có nguy cơ rời bỏ.

## Cách chạy Project

### Yêu cầu hệ thống
- Đã cài đặt Docker và Docker Compose.

### 1. Build và Khởi động Dịch vụ
Chạy lệnh sau để build images và khởi động tất cả các dịch vụ ở chế độ chạy ngầm:
```bash
docker compose up -d --build
```

### 2. Truy cập các Dịch vụ
Sau khi các container đã sẵn sàng, bạn có thể truy cập các dịch vụ sau qua trình duyệt:

- **Giao diện Apache Airflow**: [http://localhost:8081](http://localhost:8081)
  - **Tên đăng nhập**: `admin`
  - **Mật khẩu**: `admin`
- **Giao diện Spark Master**: [http://localhost:8080](http://localhost:8080)
- **Jupyter Notebook**: [http://localhost:8888](http://localhost:8888)
  - **Token**: `nhom6`
  - Sử dụng Jupyter để phân tích dữ liệu tương tác hoặc phát triển code.

### 3. Dừng các Dịch vụ
Để dừng các dịch vụ và gỡ bỏ các container:
```bash
docker compose down
```
Nếu bạn muốn xóa cả dữ liệu trong database (volumes):
```bash
docker compose down -v
```
