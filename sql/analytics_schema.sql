CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.report_products_top10 (
    product_id UInt32,
    product_name String,
    product_category String,
    total_revenue Float64,
    total_sales UInt64
) ENGINE = MergeTree()
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS analytics.report_products_revenue_by_category (
    product_category String,
    total_revenue Float64,
    total_sales UInt64
) ENGINE = MergeTree()
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS analytics.report_products_rating_reviews (
    product_id UInt32,
    product_name String,
    product_category String,
    avg_rating Float64,
    review_count Int64
) ENGINE = MergeTree()
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS analytics.report_customers_top10 (
    customer_id UInt32,
    customer_name String,
    country String,
    total_spent Float64,
    avg_check Float64,
    orders_count UInt64
) ENGINE = MergeTree()
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS analytics.report_customers_by_country (
    country String,
    customers_count UInt64
) ENGINE = MergeTree()
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS analytics.report_customers_avg_check (
    customer_id UInt32,
    customer_name String,
    country String,
    avg_check Float64,
    orders_count UInt64,
    total_spent Float64
) ENGINE = MergeTree()
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS analytics.report_time_monthly_trends (
    year UInt16,
    month UInt8,
    total_revenue Float64,
    orders_count UInt64
) ENGINE = MergeTree()
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS analytics.report_time_yearly_trends (
    year UInt16,
    total_revenue Float64,
    orders_count UInt64,
    avg_order_value Float64
) ENGINE = MergeTree()
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS analytics.report_time_avg_order_by_month (
    year UInt16,
    month UInt8,
    avg_order_value Float64
) ENGINE = MergeTree()
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS analytics.report_stores_top5 (
    store_id UInt64,
    store_name String,
    store_city String,
    store_state String,
    total_revenue Float64,
    avg_check Float64,
    orders_count UInt64
) ENGINE = MergeTree()
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS analytics.report_stores_by_city_state (
    store_city String,
    store_state String,
    total_revenue Float64,
    orders_count UInt64,
    avg_check Float64
) ENGINE = MergeTree()
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS analytics.report_stores_avg_check (
    store_id UInt64,
    store_name String,
    store_city String,
    store_state String,
    avg_check Float64,
    orders_count UInt64,
    total_revenue Float64
) ENGINE = MergeTree()
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS analytics.report_suppliers_top5 (
    supplier_id UInt64,
    supplier_name String,
    seller_country String,
    total_revenue Float64,
    avg_price Float64,
    orders_count UInt64
) ENGINE = MergeTree()
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS analytics.report_suppliers_avg_price (
    supplier_id UInt64,
    supplier_name String,
    seller_country String,
    avg_price Float64,
    orders_count UInt64,
    total_revenue Float64
) ENGINE = MergeTree()
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS analytics.report_suppliers_by_country (
    seller_country String,
    total_revenue Float64,
    suppliers_count UInt64,
    orders_count UInt64
) ENGINE = MergeTree()
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS analytics.report_quality_best_worst_products (
    product_id UInt32,
    product_name String,
    product_category String,
    avg_rating Float64,
    total_sales UInt64,
    review_count Int64,
    segment String
) ENGINE = MergeTree()
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS analytics.report_quality_rating_sales_correlation (
    rating_sales_correlation Float64
) ENGINE = MergeTree()
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS analytics.report_quality_most_reviews (
    product_id UInt32,
    product_name String,
    product_category String,
    avg_rating Float64,
    total_sales UInt64,
    review_count Int64
) ENGINE = MergeTree()
ORDER BY tuple();