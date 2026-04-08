-- =============================================================================
-- Amazon Athena Queries for Olist Lakehouse
-- =============================================================================
-- Database: olist_lakehouse
-- Tables: mart_sales, mart_kpi_daily, mart_top_products, 
--         mart_customer_analysis, mart_seller_performance
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. TEST QUERIES - Verify data loaded correctly
-- -----------------------------------------------------------------------------

-- Test mart_sales
SELECT COUNT(*) as total_records FROM olist_lakehouse.mart_sales;

-- Test mart_kpi_daily
SELECT COUNT(*) as total_records FROM olist_lakehouse.mart_kpi_daily;

-- Preview mart_sales
SELECT * FROM olist_lakehouse.mart_sales LIMIT 10;

-- Preview mart_kpi_daily
SELECT * FROM olist_lakehouse.mart_kpi_daily LIMIT 10;


-- -----------------------------------------------------------------------------
-- 2. SALES OVERVIEW QUERIES
-- -----------------------------------------------------------------------------

-- Total Revenue & Orders Summary
SELECT 
    COUNT(DISTINCT order_id) as total_orders,
    SUM(payment_value) as total_revenue,
    AVG(payment_value) as avg_order_value
FROM olist_lakehouse.mart_sales;

-- Revenue by Month
SELECT 
    year,
    month,
    COUNT(DISTINCT order_id) as total_orders,
    SUM(payment_value) as total_revenue,
    SUM(price) as total_price,
    SUM(freight_value) as total_freight
FROM olist_lakehouse.mart_sales
GROUP BY year, month
ORDER BY year, month;

-- Revenue by Year
SELECT 
    year,
    COUNT(DISTINCT order_id) as total_orders,
    SUM(payment_value) as total_revenue
FROM olist_lakehouse.mart_sales
GROUP BY year
ORDER BY year;

-- Daily KPI from mart_kpi_daily
SELECT 
    date_key,
    year,
    month,
    total_orders,
    total_revenue,
    total_items,
    avg_order_value
FROM olist_lakehouse.mart_kpi_daily
ORDER BY date_key DESC
LIMIT 30;


-- -----------------------------------------------------------------------------
-- 3. PRODUCT ANALYSIS QUERIES
-- -----------------------------------------------------------------------------

-- Top 10 Products by Revenue
SELECT 
    product_id,
    product_category_name_english,
    total_sales,
    total_items
FROM olist_lakehouse.mart_top_products
ORDER BY total_sales DESC
LIMIT 10;

-- Sales by Category
SELECT 
    product_category_name_english as category,
    SUM(total_sales) as total_sales,
    SUM(total_items) as total_items,
    COUNT(*) as product_count
FROM olist_lakehouse.mart_top_products
GROUP BY product_category_name_english
ORDER BY total_sales DESC
LIMIT 20;

-- Product Performance from mart_sales
SELECT 
    product_category_name_english as category,
    COUNT(DISTINCT order_id) as total_orders,
    SUM(payment_value) as total_revenue,
    AVG(price) as avg_price
FROM olist_lakehouse.mart_sales
GROUP BY product_category_name_english
ORDER BY total_revenue DESC;


-- -----------------------------------------------------------------------------
-- 4. CUSTOMER ANALYSIS QUERIES
-- -----------------------------------------------------------------------------

-- Top 10 Customers by Spending
SELECT 
    customer_id,
    customer_state,
    total_orders,
    total_spent,
    avg_order_value
FROM olist_lakehouse.mart_customer_analysis
ORDER BY total_spent DESC
LIMIT 10;

-- Revenue by Customer State
SELECT 
    customer_state,
    COUNT(*) as customer_count,
    SUM(total_spent) as total_revenue,
    AVG(total_spent) as avg_customer_spending
FROM olist_lakehouse.mart_customer_analysis
GROUP BY customer_state
ORDER BY total_revenue DESC;

-- Customer Distribution by State from mart_sales
SELECT 
    customer_state,
    COUNT(DISTINCT customer_id) as customer_count,
    COUNT(DISTINCT order_id) as order_count,
    SUM(payment_value) as total_revenue
FROM olist_lakehouse.mart_sales
GROUP BY customer_state
ORDER BY total_revenue DESC;

-- Customer Segmentation by Order Count
SELECT 
    CASE 
        WHEN total_orders = 1 THEN '1 order'
        WHEN total_orders BETWEEN 2 AND 3 THEN '2-3 orders'
        WHEN total_orders BETWEEN 4 AND 5 THEN '4-5 orders'
        ELSE '6+ orders'
    END as order_segment,
    COUNT(*) as customer_count,
    SUM(total_spent) as total_revenue
FROM olist_lakehouse.mart_customer_analysis
GROUP BY 
    CASE 
        WHEN total_orders = 1 THEN '1 order'
        WHEN total_orders BETWEEN 2 AND 3 THEN '2-3 orders'
        WHEN total_orders BETWEEN 4 AND 5 THEN '4-5 orders'
        ELSE '6+ orders'
    END
ORDER BY customer_count DESC;


-- -----------------------------------------------------------------------------
-- 5. SELLER PERFORMANCE QUERIES
-- -----------------------------------------------------------------------------

-- Top 10 Sellers by Revenue
SELECT 
    seller_id,
    seller_state,
    total_orders,
    total_revenue,
    avg_review_score
FROM olist_lakehouse.mart_seller_performance
ORDER BY total_revenue DESC
LIMIT 10;

-- Sellers by State
SELECT 
    seller_state,
    COUNT(*) as seller_count,
    SUM(total_orders) as total_orders,
    SUM(total_revenue) as total_revenue,
    AVG(avg_review_score) as avg_review_score
FROM olist_lakehouse.mart_seller_performance
GROUP BY seller_state
ORDER BY total_revenue DESC;

-- Seller Performance Tiers
SELECT 
    CASE 
        WHEN avg_review_score >= 4.5 THEN 'Excellent (4.5+)'
        WHEN avg_review_score >= 4.0 THEN 'Good (4.0-4.5)'
        WHEN avg_review_score >= 3.0 THEN 'Average (3.0-4.0)'
        ELSE 'Below Average (<3.0)'
    END as performance_tier,
    COUNT(*) as seller_count,
    SUM(total_revenue) as total_revenue,
    AVG(avg_review_score) as avg_score
FROM olist_lakehouse.mart_seller_performance
WHERE avg_review_score IS NOT NULL
GROUP BY 
    CASE 
        WHEN avg_review_score >= 4.5 THEN 'Excellent (4.5+)'
        WHEN avg_review_score >= 4.0 THEN 'Good (4.0-4.5)'
        WHEN avg_review_score >= 3.0 THEN 'Average (3.0-4.0)'
        ELSE 'Below Average (<3.0)'
    END
ORDER BY avg_score DESC;


-- -----------------------------------------------------------------------------
-- 6. VIEWS FOR POWER BI (Create once, query easily)
-- -----------------------------------------------------------------------------

-- View: Monthly Sales Summary
CREATE OR REPLACE VIEW olist_lakehouse.vw_monthly_sales AS
SELECT 
    year,
    month,
    COUNT(DISTINCT order_id) as total_orders,
    SUM(payment_value) as total_revenue,
    SUM(price) as total_price,
    SUM(freight_value) as total_freight,
    AVG(payment_value) as avg_order_value
FROM olist_lakehouse.mart_sales
GROUP BY year, month;

-- View: Category Performance
CREATE OR REPLACE VIEW olist_lakehouse.vw_category_performance AS
SELECT 
    product_category_name_english as category,
    SUM(total_sales) as total_sales,
    SUM(total_items) as total_items,
    COUNT(*) as product_count,
    AVG(total_sales) as avg_product_sales
FROM olist_lakehouse.mart_top_products
GROUP BY product_category_name_english;

-- View: State Summary
CREATE OR REPLACE VIEW olist_lakehouse.vw_state_summary AS
SELECT 
    customer_state as state,
    COUNT(DISTINCT customer_id) as customer_count,
    SUM(total_spent) as total_revenue,
    AVG(total_spent) as avg_customer_value,
    SUM(total_orders) as total_orders
FROM olist_lakehouse.mart_customer_analysis
GROUP BY customer_state;

-- View: Seller Summary
CREATE OR REPLACE VIEW olist_lakehouse.vw_seller_summary AS
SELECT 
    seller_state,
    COUNT(*) as seller_count,
    SUM(total_orders) as total_orders,
    SUM(total_revenue) as total_revenue,
    AVG(avg_review_score) as avg_review_score
FROM olist_lakehouse.mart_seller_performance
GROUP BY seller_state;


-- -----------------------------------------------------------------------------
-- 7. KPI DASHBOARD QUERIES
-- -----------------------------------------------------------------------------

-- Overall KPIs
SELECT 
    SUM(total_orders) as total_orders,
    SUM(total_revenue) as total_revenue,
    SUM(total_items) as total_items,
    AVG(avg_order_value) as avg_order_value
FROM olist_lakehouse.mart_kpi_daily;

-- Monthly Trend
SELECT 
    year,
    month,
    SUM(total_orders) as total_orders,
    SUM(total_revenue) as total_revenue,
    AVG(avg_order_value) as avg_order_value
FROM olist_lakehouse.mart_kpi_daily
GROUP BY year, month
ORDER BY year, month;

-- Year-over-Year Comparison
SELECT 
    year,
    SUM(total_orders) as total_orders,
    SUM(total_revenue) as total_revenue,
    SUM(total_items) as total_items
FROM olist_lakehouse.mart_kpi_daily
GROUP BY year
ORDER BY year;
