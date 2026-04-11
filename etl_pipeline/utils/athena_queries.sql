-- =============================================================================
-- Amazon Athena Queries for Olist Lakehouse — Platinum Layer
-- =============================================================================
-- Database: olist_lakehouse
-- Tables:
--   1. sales_summary_mart       — Daily sales KPIs
--   2. customer_mart            — Customer-level behavior
--   3. product_mart             — Product performance
--   4. fulfillment_mart         — Delivery & review metrics
--   5. geo_sales_mart           — Geographic revenue
--   6. kpi_summary              — Global headline KPIs
--   7. seller_performance_mart  — Seller performance
-- =============================================================================


-- =============================================================================
-- 1. TEST QUERIES — Verify data loaded correctly
-- =============================================================================

-- Row counts
SELECT 'sales_summary_mart' AS table_name, COUNT(*) AS row_count FROM olist_lakehouse.sales_summary_mart
UNION ALL
SELECT 'customer_mart', COUNT(*) FROM olist_lakehouse.customer_mart
UNION ALL
SELECT 'product_mart', COUNT(*) FROM olist_lakehouse.product_mart
UNION ALL
SELECT 'fulfillment_mart', COUNT(*) FROM olist_lakehouse.fulfillment_mart
UNION ALL
SELECT 'geo_sales_mart', COUNT(*) FROM olist_lakehouse.geo_sales_mart
UNION ALL
SELECT 'kpi_summary', COUNT(*) FROM olist_lakehouse.kpi_summary
UNION ALL
SELECT 'seller_performance_mart', COUNT(*) FROM olist_lakehouse.seller_performance_mart;

-- Preview each mart
SELECT * FROM olist_lakehouse.sales_summary_mart LIMIT 10;
SELECT * FROM olist_lakehouse.customer_mart LIMIT 10;
SELECT * FROM olist_lakehouse.product_mart LIMIT 10;
SELECT * FROM olist_lakehouse.fulfillment_mart LIMIT 10;
SELECT * FROM olist_lakehouse.geo_sales_mart LIMIT 10;
SELECT * FROM olist_lakehouse.kpi_summary;
SELECT * FROM olist_lakehouse.seller_performance_mart LIMIT 10;


-- =============================================================================
-- 2. KPI SUMMARY — Global Dashboard Cards
-- =============================================================================

-- All headline KPIs (single row)
SELECT 
    total_revenue,
    total_orders,
    total_customers,
    avg_order_value,
    revenue_per_customer,
    avg_delivery_time_days,
    on_time_delivery_rate_pct,
    late_delivery_rate_pct,
    avg_review_score,
    positive_review_rate_pct,
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
    total_orders,
    total_items,
    avg_order_value,
    revenue_day_growth_pct
FROM olist_lakehouse.sales_summary_mart
ORDER BY purchase_date_key DESC
LIMIT 30;

-- Monthly Revenue Trend
SELECT 
    year,
    month,
    SUM(total_revenue) AS monthly_revenue,
    SUM(total_orders) AS monthly_orders,
    SUM(total_items) AS monthly_items,
    ROUND(SUM(total_revenue) / SUM(total_orders), 2) AS avg_order_value
FROM olist_lakehouse.sales_summary_mart
GROUP BY year, month
ORDER BY year, month;

-- Quarterly Revenue
SELECT 
    year,
    quarter,
    SUM(total_revenue) AS quarterly_revenue,
    SUM(total_orders) AS quarterly_orders
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
    ROUND(AVG(total_orders), 2) AS avg_daily_orders
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
    SUM(CASE WHEN is_repeat_customer = 1 THEN 1 ELSE 0 END) AS repeat_customers
FROM olist_lakehouse.customer_mart
GROUP BY customer_state
ORDER BY total_revenue DESC;

-- Repeat vs One-time Customers
SELECT 
    CASE WHEN is_repeat_customer = 1 THEN 'Repeat' ELSE 'One-time' END AS customer_type,
    COUNT(*) AS customer_count,
    SUM(total_spent) AS total_revenue,
    ROUND(AVG(total_spent), 2) AS avg_spending,
    ROUND(AVG(total_orders), 2) AS avg_orders
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
    ROUND(AVG(total_orders), 2) AS avg_orders
FROM olist_lakehouse.customer_mart
GROUP BY 
    CASE 
        WHEN total_spent >= 1000 THEN 'High (1000+)'
        WHEN total_spent >= 500 THEN 'Medium (500-999)'
        WHEN total_spent >= 100 THEN 'Low (100-499)'
        ELSE 'Minimal (<100)'
    END
ORDER BY total_revenue DESC;


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
    avg_freight
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
    ROUND(AVG(avg_freight), 2) AS avg_freight
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

-- Product Size vs Freight Analysis (heavy products cost more to ship?)
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
    SUM(total_sales) AS total_revenue
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


-- =============================================================================
-- 6. FULFILLMENT & DELIVERY ANALYSIS
-- =============================================================================

-- Delivery Status Distribution
SELECT 
    delivery_status,
    COUNT(*) AS order_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct
FROM olist_lakehouse.fulfillment_mart
GROUP BY delivery_status;

-- Average Delivery Metrics by State
SELECT 
    customer_state,
    COUNT(*) AS order_count,
    ROUND(AVG(shipping_duration_days), 2) AS avg_shipping_days,
    ROUND(AVG(delivery_delay_days), 2) AS avg_delay_days,
    SUM(is_late_delivery) AS late_deliveries,
    ROUND(SUM(is_late_delivery) * 100.0 / COUNT(*), 2) AS late_delivery_pct,
    ROUND(AVG(review_score), 2) AS avg_review_score
FROM olist_lakehouse.fulfillment_mart
WHERE shipping_duration_days IS NOT NULL
GROUP BY customer_state
ORDER BY late_delivery_pct DESC;

-- Late Delivery Impact on Reviews
SELECT 
    delivery_status,
    COUNT(*) AS order_count,
    ROUND(AVG(review_score), 2) AS avg_review_score,
    SUM(is_positive_review) AS positive_reviews,
    ROUND(SUM(is_positive_review) * 100.0 / COUNT(*), 2) AS positive_review_pct
FROM olist_lakehouse.fulfillment_mart
WHERE review_score IS NOT NULL
GROUP BY delivery_status;

-- Review Score Distribution
SELECT 
    review_score,
    COUNT(*) AS order_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct
FROM olist_lakehouse.fulfillment_mart
WHERE review_score IS NOT NULL
GROUP BY review_score
ORDER BY review_score;

-- Order Status Breakdown
SELECT 
    order_status,
    COUNT(*) AS order_count,
    ROUND(AVG(review_score), 2) AS avg_review_score
FROM olist_lakehouse.fulfillment_mart
GROUP BY order_status
ORDER BY order_count DESC;


-- =============================================================================
-- 7. GEOGRAPHIC ANALYSIS
-- =============================================================================

-- Top 10 States by Revenue
SELECT 
    state,
    total_customers,
    total_orders,
    total_revenue,
    avg_order_value,
    avg_freight
FROM olist_lakehouse.geo_sales_mart
WHERE city = (
    SELECT city FROM olist_lakehouse.geo_sales_mart g2 
    WHERE g2.state = geo_sales_mart.state 
    ORDER BY g2.total_revenue DESC LIMIT 1
)
ORDER BY total_revenue DESC;

-- State-level Aggregation
SELECT 
    state,
    SUM(total_revenue) AS state_revenue,
    SUM(total_orders) AS state_orders,
    SUM(total_customers) AS state_customers,
    ROUND(SUM(total_revenue) / SUM(total_orders), 2) AS state_avg_order_value,
    ROUND(AVG(avg_freight), 2) AS state_avg_freight
FROM olist_lakehouse.geo_sales_mart
GROUP BY state
ORDER BY state_revenue DESC;

-- Top 20 Cities by Revenue
SELECT 
    state,
    city,
    total_revenue,
    total_orders,
    total_customers,
    avg_order_value,
    avg_freight
FROM olist_lakehouse.geo_sales_mart
ORDER BY total_revenue DESC
LIMIT 20;


-- =============================================================================
-- 8. SELLER PERFORMANCE
-- =============================================================================

-- Top 20 Sellers by Revenue
SELECT 
    seller_id,
    seller_city,
    seller_state,
    total_orders,
    total_revenue,
    total_items,
    avg_order_value,
    unique_customers,
    avg_review_score,
    positive_review_rate_pct
FROM olist_lakehouse.seller_performance_mart
ORDER BY total_revenue DESC
LIMIT 20;

-- Sellers by State
SELECT 
    seller_state,
    COUNT(*) AS seller_count,
    SUM(total_orders) AS total_orders,
    SUM(total_revenue) AS total_revenue,
    ROUND(AVG(avg_review_score), 2) AS avg_review_score,
    ROUND(AVG(positive_review_rate_pct), 2) AS avg_positive_review_pct
FROM olist_lakehouse.seller_performance_mart
GROUP BY seller_state
ORDER BY total_revenue DESC;

-- Seller Performance Tiers
SELECT 
    CASE 
        WHEN avg_review_score >= 4.5 THEN 'Excellent (4.5+)'
        WHEN avg_review_score >= 4.0 THEN 'Good (4.0-4.5)'
        WHEN avg_review_score >= 3.0 THEN 'Average (3.0-4.0)'
        ELSE 'Below Average (<3.0)'
    END AS performance_tier,
    COUNT(*) AS seller_count,
    SUM(total_revenue) AS total_revenue,
    ROUND(AVG(avg_review_score), 2) AS avg_score,
    SUM(total_orders) AS total_orders,
    SUM(unique_customers) AS total_customers
FROM olist_lakehouse.seller_performance_mart
WHERE avg_review_score IS NOT NULL
GROUP BY 
    CASE 
        WHEN avg_review_score >= 4.5 THEN 'Excellent (4.5+)'
        WHEN avg_review_score >= 4.0 THEN 'Good (4.0-4.5)'
        WHEN avg_review_score >= 3.0 THEN 'Average (3.0-4.0)'
        ELSE 'Below Average (<3.0)'
    END
ORDER BY avg_score DESC;

-- Seller Revenue Tiers
SELECT 
    CASE 
        WHEN total_revenue >= 50000 THEN 'Top Seller (50k+)'
        WHEN total_revenue >= 10000 THEN 'Mid Seller (10k-50k)'
        WHEN total_revenue >= 1000 THEN 'Small Seller (1k-10k)'
        ELSE 'Micro Seller (<1k)'
    END AS revenue_tier,
    COUNT(*) AS seller_count,
    SUM(total_revenue) AS total_revenue,
    ROUND(AVG(avg_review_score), 2) AS avg_review_score
FROM olist_lakehouse.seller_performance_mart
GROUP BY 
    CASE 
        WHEN total_revenue >= 50000 THEN 'Top Seller (50k+)'
        WHEN total_revenue >= 10000 THEN 'Mid Seller (10k-50k)'
        WHEN total_revenue >= 1000 THEN 'Small Seller (1k-10k)'
        ELSE 'Micro Seller (<1k)'
    END
ORDER BY total_revenue DESC;


-- =============================================================================
-- 9. VIEWS FOR POWER BI (Create once, query easily)
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
    ROUND(AVG(avg_freight), 2) AS avg_freight
FROM olist_lakehouse.product_mart
GROUP BY category;

-- View: State Revenue Summary
CREATE OR REPLACE VIEW olist_lakehouse.vw_state_summary AS
SELECT 
    state,
    SUM(total_revenue) AS total_revenue,
    SUM(total_orders) AS total_orders,
    SUM(total_customers) AS total_customers,
    ROUND(SUM(total_revenue) / SUM(total_orders), 2) AS avg_order_value,
    ROUND(AVG(avg_freight), 2) AS avg_freight
FROM olist_lakehouse.geo_sales_mart
GROUP BY state;

-- View: Delivery Performance by State
CREATE OR REPLACE VIEW olist_lakehouse.vw_delivery_performance AS
SELECT 
    customer_state,
    COUNT(*) AS order_count,
    ROUND(AVG(shipping_duration_days), 2) AS avg_shipping_days,
    ROUND(AVG(delivery_delay_days), 2) AS avg_delay_days,
    ROUND(SUM(is_late_delivery) * 100.0 / COUNT(*), 2) AS late_delivery_pct,
    ROUND(AVG(review_score), 2) AS avg_review_score
FROM olist_lakehouse.fulfillment_mart
WHERE shipping_duration_days IS NOT NULL
GROUP BY customer_state;

-- View: Seller State Summary
CREATE OR REPLACE VIEW olist_lakehouse.vw_seller_summary AS
SELECT 
    seller_state,
    COUNT(*) AS seller_count,
    SUM(total_orders) AS total_orders,
    SUM(total_revenue) AS total_revenue,
    SUM(unique_customers) AS total_customers,
    ROUND(AVG(avg_review_score), 2) AS avg_review_score,
    ROUND(AVG(positive_review_rate_pct), 2) AS avg_positive_review_pct
FROM olist_lakehouse.seller_performance_mart
GROUP BY seller_state;
