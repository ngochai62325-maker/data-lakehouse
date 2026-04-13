-- =============================================================================
-- Amazon Athena Queries for Olist Lakehouse — Platinum Layer
-- =============================================================================
-- Database: olist_lakehouse
-- Tables (4 marts — khớp với gold_to_platinum.py & glue_catalog.py):
--   1. sales_summary_mart   — Daily sales KPIs (time-series)
--   2. customer_mart        — Customer-level behavior metrics
--   3. product_mart         — Product-level performance
--   4. kpi_summary          — Global headline KPIs (single row)
-- =============================================================================


-- =============================================================================
-- 1. TEST QUERIES — Verify data loaded correctly
-- =============================================================================

-- Row counts — All 4 marts
SELECT 'sales_summary_mart' AS table_name, COUNT(*) AS row_count FROM olist_lakehouse.sales_summary_mart
UNION ALL
SELECT 'customer_mart', COUNT(*) FROM olist_lakehouse.customer_mart
UNION ALL
SELECT 'product_mart', COUNT(*) FROM olist_lakehouse.product_mart
UNION ALL
SELECT 'kpi_summary', COUNT(*) FROM olist_lakehouse.kpi_summary;

-- Preview each mart
SELECT * FROM olist_lakehouse.sales_summary_mart LIMIT 10;
SELECT * FROM olist_lakehouse.customer_mart LIMIT 10;
SELECT * FROM olist_lakehouse.product_mart LIMIT 10;
SELECT * FROM olist_lakehouse.kpi_summary;


-- =============================================================================
-- 2. KPI SUMMARY — Global Dashboard Cards (single row)
-- =============================================================================

-- All headline KPIs
SELECT 
    total_revenue,
    total_orders,
    total_items,
    total_customers,
    total_sellers,
    total_products,
    avg_order_value,
    revenue_per_customer,
    avg_delivery_time_days,
    avg_delivery_delay_days,
    on_time_delivery_rate_pct,
    late_delivery_rate_pct,
    avg_review_score,
    positive_review_rate_pct,
    total_reviews,
    repeat_customer_count,
    repeat_customer_rate_pct
FROM olist_lakehouse.kpi_summary;


-- =============================================================================
-- 3. SALES SUMMARY — Time Series Analysis
-- =============================================================================

-- Daily Sales Trend (last 30 days)
SELECT 
    purchase_date_key,
    full_date,
    day_name,
    is_weekend,
    total_revenue,
    total_payment,
    total_orders,
    total_items,
    avg_order_value,
    avg_item_price,
    total_freight,
    revenue_day_growth_pct
FROM olist_lakehouse.sales_summary_mart
ORDER BY purchase_date_key DESC
LIMIT 30;

-- Monthly Revenue Trend
SELECT 
    year,
    month,
    SUM(total_revenue) AS monthly_revenue,
    SUM(total_payment) AS monthly_payment,
    SUM(total_orders) AS monthly_orders,
    SUM(total_items) AS monthly_items,
    SUM(total_freight) AS monthly_freight,
    ROUND(SUM(total_revenue) / SUM(total_orders), 2) AS avg_order_value
FROM olist_lakehouse.sales_summary_mart
GROUP BY year, month
ORDER BY year, month;

-- Quarterly Revenue
SELECT 
    year,
    quarter,
    SUM(total_revenue) AS quarterly_revenue,
    SUM(total_orders) AS quarterly_orders,
    SUM(total_items) AS quarterly_items
FROM olist_lakehouse.sales_summary_mart
GROUP BY year, quarter
ORDER BY year, quarter;

-- Weekend vs Weekday Performance
SELECT 
    CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    COUNT(*) AS num_days,
    SUM(total_revenue) AS total_revenue,
    ROUND(AVG(total_revenue), 2) AS avg_daily_revenue,
    SUM(total_orders) AS total_orders,
    ROUND(AVG(total_orders), 2) AS avg_daily_orders,
    ROUND(AVG(avg_item_price), 2) AS avg_item_price
FROM olist_lakehouse.sales_summary_mart
GROUP BY is_weekend;

-- Revenue by Day of Week
SELECT 
    day_name,
    day_of_week,
    SUM(total_revenue) AS total_revenue,
    ROUND(AVG(total_revenue), 2) AS avg_daily_revenue,
    SUM(total_orders) AS total_orders
FROM olist_lakehouse.sales_summary_mart
GROUP BY day_name, day_of_week
ORDER BY day_of_week;

-- Top 10 Revenue Days
SELECT 
    purchase_date_key,
    full_date,
    day_name,
    total_revenue,
    total_orders,
    avg_order_value
FROM olist_lakehouse.sales_summary_mart
ORDER BY total_revenue DESC
LIMIT 10;

-- Sales Growth Trend (days with highest growth)
SELECT 
    purchase_date_key,
    full_date,
    total_revenue,
    revenue_day_growth_pct
FROM olist_lakehouse.sales_summary_mart
WHERE revenue_day_growth_pct IS NOT NULL
ORDER BY revenue_day_growth_pct DESC
LIMIT 10;


-- =============================================================================
-- 4. CUSTOMER ANALYSIS
-- =============================================================================

-- Top 20 Customers by Spending
SELECT 
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state,
    total_orders,
    total_spent,
    avg_order_value,
    total_items_purchased,
    total_freight_paid,
    avg_item_price,
    last_purchase_date,
    is_repeat_customer
FROM olist_lakehouse.customer_mart
ORDER BY total_spent DESC
LIMIT 20;

-- Customer Distribution by State
SELECT 
    customer_state,
    COUNT(*) AS customer_count,
    SUM(total_spent) AS total_revenue,
    ROUND(AVG(total_spent), 2) AS avg_customer_value,
    SUM(total_orders) AS total_orders,
    SUM(CASE WHEN is_repeat_customer = 1 THEN 1 ELSE 0 END) AS repeat_customers,
    ROUND(SUM(CASE WHEN is_repeat_customer = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS repeat_rate_pct
FROM olist_lakehouse.customer_mart
GROUP BY customer_state
ORDER BY total_revenue DESC;

-- Repeat vs One-time Customers
SELECT 
    CASE WHEN is_repeat_customer = 1 THEN 'Repeat' ELSE 'One-time' END AS customer_type,
    COUNT(*) AS customer_count,
    SUM(total_spent) AS total_revenue,
    ROUND(AVG(total_spent), 2) AS avg_spending,
    ROUND(AVG(total_orders), 2) AS avg_orders,
    ROUND(AVG(avg_item_price), 2) AS avg_item_price
FROM olist_lakehouse.customer_mart
GROUP BY is_repeat_customer;

-- Customer Segmentation by Spending Tiers
SELECT 
    CASE 
        WHEN total_spent >= 1000 THEN 'High (1000+)'
        WHEN total_spent >= 500 THEN 'Medium (500-999)'
        WHEN total_spent >= 100 THEN 'Low (100-499)'
        ELSE 'Minimal (<100)'
    END AS spending_tier,
    COUNT(*) AS customer_count,
    SUM(total_spent) AS total_revenue,
    ROUND(AVG(total_orders), 2) AS avg_orders,
    ROUND(AVG(total_items_purchased), 2) AS avg_items
FROM olist_lakehouse.customer_mart
GROUP BY 
    CASE 
        WHEN total_spent >= 1000 THEN 'High (1000+)'
        WHEN total_spent >= 500 THEN 'Medium (500-999)'
        WHEN total_spent >= 100 THEN 'Low (100-499)'
        ELSE 'Minimal (<100)'
    END
ORDER BY total_revenue DESC;

-- Top 10 Cities by Customer Revenue
SELECT 
    customer_city,
    customer_state,
    COUNT(*) AS customer_count,
    SUM(total_spent) AS total_revenue,
    ROUND(AVG(total_spent), 2) AS avg_customer_value
FROM olist_lakehouse.customer_mart
GROUP BY customer_city, customer_state
ORDER BY total_revenue DESC
LIMIT 10;


-- =============================================================================
-- 5. PRODUCT ANALYSIS
-- =============================================================================

-- Top 20 Products by Revenue
SELECT 
    product_id,
    category,
    total_sales,
    total_quantity,
    total_orders,
    avg_price,
    avg_freight,
    total_freight
FROM olist_lakehouse.product_mart
ORDER BY total_sales DESC
LIMIT 20;

-- Sales by Category
SELECT 
    category,
    COUNT(*) AS product_count,
    SUM(total_sales) AS total_revenue,
    SUM(total_quantity) AS total_items,
    SUM(total_orders) AS total_orders,
    ROUND(AVG(avg_price), 2) AS avg_price,
    ROUND(AVG(avg_freight), 2) AS avg_freight,
    SUM(total_freight) AS total_freight
FROM olist_lakehouse.product_mart
GROUP BY category
ORDER BY total_revenue DESC;

-- Top 10 Categories by Revenue
SELECT 
    category,
    SUM(total_sales) AS total_revenue,
    SUM(total_quantity) AS total_items,
    COUNT(*) AS product_count
FROM olist_lakehouse.product_mart
GROUP BY category
ORDER BY total_revenue DESC
LIMIT 10;

-- Product Size vs Freight Analysis
SELECT 
    CASE 
        WHEN product_weight_g >= 10000 THEN 'Heavy (10kg+)'
        WHEN product_weight_g >= 5000 THEN 'Medium (5-10kg)'
        WHEN product_weight_g >= 1000 THEN 'Light (1-5kg)'
        ELSE 'Very Light (<1kg)'
    END AS weight_tier,
    COUNT(*) AS product_count,
    ROUND(AVG(avg_freight), 2) AS avg_freight,
    ROUND(AVG(avg_price), 2) AS avg_price,
    SUM(total_sales) AS total_revenue,
    SUM(total_freight) AS total_freight
FROM olist_lakehouse.product_mart
WHERE product_weight_g IS NOT NULL
GROUP BY 
    CASE 
        WHEN product_weight_g >= 10000 THEN 'Heavy (10kg+)'
        WHEN product_weight_g >= 5000 THEN 'Medium (5-10kg)'
        WHEN product_weight_g >= 1000 THEN 'Light (1-5kg)'
        ELSE 'Very Light (<1kg)'
    END
ORDER BY avg_freight DESC;

-- Product Dimensions: Price per Volume Unit
SELECT 
    product_id,
    category,
    total_sales,
    avg_price,
    product_weight_g,
    ROUND(product_length_cm * product_height_cm * product_width_cm, 2) AS volume_cm3,
    avg_freight
FROM olist_lakehouse.product_mart
WHERE product_length_cm IS NOT NULL
  AND product_height_cm IS NOT NULL
  AND product_width_cm IS NOT NULL
ORDER BY total_sales DESC
LIMIT 20;


-- =============================================================================
-- 6. CROSS-MART ANALYSIS — Kết hợp nhiều mart
-- =============================================================================

-- Customer State + Sales Summary: Revenue & Customer Count by State
SELECT 
    c.customer_state,
    COUNT(*) AS customer_count,
    SUM(c.total_spent) AS total_customer_revenue,
    ROUND(AVG(c.total_spent), 2) AS avg_customer_value,
    SUM(c.total_orders) AS total_orders
FROM olist_lakehouse.customer_mart c
GROUP BY c.customer_state
ORDER BY total_customer_revenue DESC
LIMIT 10;

-- Product Category Performance (Top categories with details)
SELECT 
    p.category,
    COUNT(*) AS product_count,
    SUM(p.total_sales) AS category_revenue,
    SUM(p.total_quantity) AS total_items_sold,
    ROUND(AVG(p.avg_price), 2) AS avg_product_price,
    ROUND(AVG(p.product_weight_g), 2) AS avg_weight_g,
    ROUND(SUM(p.total_freight), 2) AS total_freight_cost
FROM olist_lakehouse.product_mart p
GROUP BY p.category
ORDER BY category_revenue DESC
LIMIT 15;


-- =============================================================================
-- 7. VIEWS FOR POWER BI (Create once, query easily)
-- =============================================================================

-- View: Monthly Sales Summary
CREATE OR REPLACE VIEW olist_lakehouse.vw_monthly_sales AS
SELECT 
    year,
    month,
    SUM(total_revenue) AS monthly_revenue,
    SUM(total_payment) AS monthly_payment,
    SUM(total_orders) AS monthly_orders,
    SUM(total_items) AS monthly_items,
    SUM(total_freight) AS monthly_freight,
    ROUND(SUM(total_revenue) / SUM(total_orders), 2) AS avg_order_value
FROM olist_lakehouse.sales_summary_mart
GROUP BY year, month;

-- View: Category Performance
CREATE OR REPLACE VIEW olist_lakehouse.vw_category_performance AS
SELECT 
    category,
    COUNT(*) AS product_count,
    SUM(total_sales) AS total_revenue,
    SUM(total_quantity) AS total_items,
    SUM(total_orders) AS total_orders,
    ROUND(AVG(avg_price), 2) AS avg_price,
    ROUND(AVG(avg_freight), 2) AS avg_freight,
    SUM(total_freight) AS total_freight
FROM olist_lakehouse.product_mart
GROUP BY category;

-- View: Customer State Summary
CREATE OR REPLACE VIEW olist_lakehouse.vw_customer_state_summary AS
SELECT 
    customer_state,
    COUNT(*) AS customer_count,
    SUM(total_spent) AS total_revenue,
    SUM(total_orders) AS total_orders,
    ROUND(AVG(total_spent), 2) AS avg_customer_value,
    SUM(CASE WHEN is_repeat_customer = 1 THEN 1 ELSE 0 END) AS repeat_customers,
    ROUND(SUM(CASE WHEN is_repeat_customer = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS repeat_rate_pct
FROM olist_lakehouse.customer_mart
GROUP BY customer_state;

-- View: Customer Spending Tiers
CREATE OR REPLACE VIEW olist_lakehouse.vw_customer_tiers AS
SELECT 
    CASE 
        WHEN total_spent >= 1000 THEN 'High (1000+)'
        WHEN total_spent >= 500 THEN 'Medium (500-999)'
        WHEN total_spent >= 100 THEN 'Low (100-499)'
        ELSE 'Minimal (<100)'
    END AS spending_tier,
    COUNT(*) AS customer_count,
    SUM(total_spent) AS total_revenue,
    ROUND(AVG(total_orders), 2) AS avg_orders,
    ROUND(AVG(total_items_purchased), 2) AS avg_items
FROM olist_lakehouse.customer_mart
GROUP BY 
    CASE 
        WHEN total_spent >= 1000 THEN 'High (1000+)'
        WHEN total_spent >= 500 THEN 'Medium (500-999)'
        WHEN total_spent >= 100 THEN 'Low (100-499)'
        ELSE 'Minimal (<100)'
    END;

-- View: Weekend vs Weekday Sales
CREATE OR REPLACE VIEW olist_lakehouse.vw_weekday_weekend AS
SELECT 
    CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    COUNT(*) AS num_days,
    SUM(total_revenue) AS total_revenue,
    ROUND(AVG(total_revenue), 2) AS avg_daily_revenue,
    SUM(total_orders) AS total_orders,
    ROUND(AVG(total_orders), 2) AS avg_daily_orders
FROM olist_lakehouse.sales_summary_mart
GROUP BY is_weekend;
