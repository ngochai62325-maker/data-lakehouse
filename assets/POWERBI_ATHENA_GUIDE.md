# Hướng dẫn kết nối Power BI với Amazon Athena

## Tổng quan Pipeline

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         DATA LAKEHOUSE PIPELINE                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────────┐          │
│   │  Bronze  │───▶│  Silver  │───▶│   Gold   │───▶│   Platinum   │          │
│   │  (Raw)   │    │ (Clean)  │    │  (Star)  │    │  (Data Mart) │          │
│   └──────────┘    └──────────┘    └──────────┘    └──────┬───────┘          │
│                                                          │                   │
│                                                          ▼                   │
│   ┌──────────────────────────────────────────────────────────────────┐      │
│   │                        Amazon S3                                  │      │
│   │   s3://bucket/platinum/                                          │      │
│   │   ├── mart_sales/                                                │      │
│   │   ├── mart_kpi_daily/                                           │      │
│   │   ├── mart_top_products/                                        │      │
│   │   ├── mart_customer_analysis/                                   │      │
│   │   └── mart_seller_performance/                                  │      │
│   └───────────────────────────────┬──────────────────────────────────┘      │
│                                   │                                          │
│                                   ▼                                          │
│   ┌───────────────────────────────────────────────────────────────────┐     │
│   │                    AWS Glue Data Catalog                           │     │
│   │   Database: olist_lakehouse                                        │     │
│   │   Tables: mart_sales, mart_kpi_daily, ...                         │     │
│   └───────────────────────────────┬───────────────────────────────────┘     │
│                                   │                                          │
│                                   ▼                                          │
│   ┌───────────────────────────────────────────────────────────────────┐     │
│   │                      Amazon Athena                                  │     │
│   │   • SQL Query Engine                                               │     │
│   │   • Serverless                                                     │     │
│   │   • Pay per query                                                  │     │
│   └───────────────────────────────┬───────────────────────────────────┘     │
│                                   │                                          │
│                                   ▼                                          │
│   ┌───────────────────────────────────────────────────────────────────┐     │
│   │                       Power BI Desktop                              │     │
│   │   • ODBC Connection                                                │     │
│   │   • Dashboard & Reports                                            │     │
│   └───────────────────────────────────────────────────────────────────┘     │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Phần 1: Cài đặt AWS Glue Tables

### 1.1 Chạy script tạo Glue tables

```bash
# Tạo tất cả tables
python -m etl_pipeline.utils.glue_catalog --action create --table all

# Kiểm tra tables đã tạo
python -m etl_pipeline.utils.glue_catalog --action list
```

### 1.2 Verify trong AWS Console

1. Mở AWS Console → Glue → Databases
2. Tìm database `olist_lakehouse`
3. Kiểm tra 5 tables đã được tạo

---

## Phần 2: Test với Amazon Athena

### 2.1 Cấu hình Athena Output Location

1. Mở AWS Console → Athena
2. Click "Settings" → "Manage"
3. Set Query result location: `s3://your-bucket/athena-results/`

### 2.2 Test Queries

```sql
-- Chọn database
USE olist_lakehouse;

-- Test mart_sales
SELECT COUNT(*) as total_records FROM mart_sales;

-- Test monthly revenue
SELECT 
    year, month,
    SUM(payment_value) as revenue
FROM mart_sales
GROUP BY year, month
ORDER BY year, month;
```

Xem thêm các query mẫu tại: `etl_pipeline/utils/athena_queries.sql`

---

## Phần 3: Kết nối Power BI với Athena

### 3.1 Cài đặt ODBC Driver

1. **Download driver:**
   - Truy cập: https://docs.aws.amazon.com/athena/latest/ug/connect-with-odbc.html
   - Download "Amazon Athena ODBC driver" phù hợp với Windows (64-bit)

2. **Cài đặt:**
   - Chạy file `.msi` đã download
   - Chọn "Typical installation"
   - Hoàn tất cài đặt

### 3.2 Cấu hình ODBC DSN

1. **Mở ODBC Data Source Administrator:**
   - Tìm kiếm "ODBC Data Sources (64-bit)" trong Windows
   - Hoặc: Control Panel → Administrative Tools → ODBC Data Sources

2. **Tạo System DSN mới:**
   - Click tab "System DSN"
   - Click "Add..."
   - Chọn "Simba Athena ODBC Driver"
   - Click "Finish"

3. **Cấu hình DSN:**

   | Field | Value |
   |-------|-------|
   | **Data Source Name** | `Athena_Olist` |
   | **Description** | `Olist Lakehouse Connection` |
   | **AWS Region** | `us-east-1` (hoặc region của bạn) |
   | **Catalog** | `AwsDataCatalog` |
   | **Database** | `olist_lakehouse` |
   | **Workgroup** | `primary` |
   | **S3 Output Location** | `s3://your-bucket/athena-results/` |

4. **Authentication Options:**

   **Option A: IAM Credentials (Recommended for development)**
   - Authentication Type: `IAM Credentials`
   - User: `<AWS_ACCESS_KEY_ID>`
   - Password: `<AWS_SECRET_ACCESS_KEY>`

   **Option B: IAM Profile**
   - Authentication Type: `IAM Profile`
   - Profile Name: `default` (từ ~/.aws/credentials)

5. **Test Connection:**
   - Click "Test" để verify kết nối
   - Nếu thành công, click "OK" để lưu

### 3.3 Kết nối từ Power BI Desktop

1. **Mở Power BI Desktop**

2. **Get Data:**
   - Click "Get Data" → "More..."
   - Tìm "ODBC"
   - Click "Connect"

3. **Chọn DSN:**
   - Trong dropdown, chọn `Athena_Olist`
   - Click "OK"

4. **Navigator:**
   - Expand `olist_lakehouse`
   - Chọn các tables cần import:
     - ☑️ mart_sales
     - ☑️ mart_kpi_daily
     - ☑️ mart_top_products
     - ☑️ mart_customer_analysis
     - ☑️ mart_seller_performance
   - Click "Load" hoặc "Transform Data"

---

## Phần 4: Thiết kế Dashboard Power BI

### Dashboard 1: Sales Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         SALES OVERVIEW                                   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │ Total Revenue│  │ Total Orders │  │  Total Items │  │  Avg Order   │ │
│  │  R$ 15.4M    │  │    99,441    │  │   112,650    │  │   R$ 154.92  │ │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘ │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────────┐│
│  │                    Revenue by Month (Line Chart)                     ││
│  └─────────────────────────────────────────────────────────────────────┘│
│                                                                          │
│  ┌─────────────────────────────┐  ┌─────────────────────────────────┐   │
│  │  Revenue by State (Map)     │  │  Order Status (Donut)           │   │
│  └─────────────────────────────┘  └─────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘

Data Source: mart_sales, mart_kpi_daily
```

**DAX Measures:**
```dax
Total Revenue = SUM(mart_sales[payment_value])
Total Orders = DISTINCTCOUNT(mart_sales[order_id])
Avg Order Value = DIVIDE([Total Revenue], [Total Orders])
```

---

### Dashboard 2: Product Analysis

```
┌─────────────────────────────────────────────────────────────────────────┐
│                       PRODUCT ANALYSIS                                   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────────┐│
│  │              Top 10 Products by Revenue (Bar Chart)                  ││
│  └─────────────────────────────────────────────────────────────────────┘│
│                                                                          │
│  ┌─────────────────────────────┐  ┌─────────────────────────────────┐   │
│  │  Sales by Category          │  │  Category Tree Map              │   │
│  │  (Stacked Bar)              │  │                                 │   │
│  └─────────────────────────────┘  └─────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘

Data Source: mart_top_products, mart_sales
```

---

### Dashboard 3: Customer Analysis

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      CUSTOMER ANALYSIS                                   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                   │
│  │Total Customers│  │ Avg Spending │  │ Repeat Rate  │                   │
│  │    96,096     │  │   R$ 160     │  │    3.2%      │                   │
│  └──────────────┘  └──────────────┘  └──────────────┘                   │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────────┐│
│  │              Customer Distribution by State (Map)                    ││
│  └─────────────────────────────────────────────────────────────────────┘│
│                                                                          │
│  ┌─────────────────────────────┐  ┌─────────────────────────────────┐   │
│  │  Top Customers (Table)      │  │  Customer Segments (Pie)        │   │
│  └─────────────────────────────┘  └─────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘

Data Source: mart_customer_analysis, mart_sales
```

---

### Dashboard 4: Seller Performance

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     SELLER PERFORMANCE                                   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                   │
│  │ Total Sellers│  │ Avg Revenue  │  │ Avg Review   │                   │
│  │    3,095     │  │   R$ 4,979   │  │    4.09 ⭐   │                   │
│  └──────────────┘  └──────────────┘  └──────────────┘                   │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────────┐│
│  │              Top 10 Sellers by Revenue (Bar Chart)                   ││
│  └─────────────────────────────────────────────────────────────────────┘│
│                                                                          │
│  ┌─────────────────────────────┐  ┌─────────────────────────────────┐   │
│  │  Sellers by State (Map)     │  │  Review Score Distribution      │   │
│  └─────────────────────────────┘  └─────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘

Data Source: mart_seller_performance
```

---

## Phần 5: Best Practices

### 5.1 Tối ưu Athena Queries

```sql
-- ❌ BAD: Full table scan
SELECT * FROM mart_sales;

-- ✅ GOOD: Select specific columns
SELECT order_id, payment_value, year, month 
FROM mart_sales 
WHERE year = 2018;

-- ✅ GOOD: Use LIMIT for testing
SELECT * FROM mart_sales LIMIT 100;
```

### 5.2 Power BI Tips

1. **Import vs DirectQuery:**
   - Import: Faster, but data may be stale
   - DirectQuery: Always fresh, but slower

2. **Incremental Refresh:**
   - Set up incremental refresh for large tables

3. **Relationships:**
   ```
   mart_sales.customer_id → mart_customer_analysis.customer_id
   mart_sales.seller_id → mart_seller_performance.seller_id
   mart_sales.product_id → mart_top_products.product_id
   ```

---

## Phần 6: Troubleshooting

### Lỗi thường gặp

| Lỗi | Nguyên nhân | Giải pháp |
|-----|-------------|-----------|
| `Access Denied` | IAM permissions | Kiểm tra IAM policy có quyền Athena, Glue, S3 |
| `Table not found` | Glue table chưa tạo | Chạy lại glue_catalog.py |
| `No output location` | Chưa set S3 output | Cấu hình trong Athena settings |
| `ODBC connection failed` | Driver chưa cài | Cài lại ODBC driver |

### IAM Policy cần thiết

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "athena:*",
                "glue:GetDatabase",
                "glue:GetDatabases",
                "glue:GetTable",
                "glue:GetTables",
                "glue:GetPartition",
                "glue:GetPartitions",
                "s3:GetObject",
                "s3:ListBucket",
                "s3:PutObject"
            ],
            "Resource": "*"
        }
    ]
}
```

---

## Tóm tắt

| Step | Tool | Action |
|------|------|--------|
| 1 | Python | Run `glue_catalog.py` |
| 2 | AWS Console | Verify Glue tables |
| 3 | Athena | Test SQL queries |
| 4 | Windows | Install ODBC driver |
| 5 | Windows | Configure DSN |
| 6 | Power BI | Connect via ODBC |
| 7 | Power BI | Build dashboards |

**Pipeline hoàn chỉnh:**
```
S3 (Parquet) → Glue Catalog → Athena → ODBC → Power BI
```
