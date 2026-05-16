CREATE TABLE dim_products (
    product_id INT PRIMARY KEY,
    product_name TEXT,
    product_category TEXT,
    product_price FLOAT
);

CREATE TABLE dim_customers (
    customer_id INT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    country TEXT
);

CREATE TABLE dim_stores (
    store_id INT PRIMARY KEY,
    store_name TEXT,
    store_city TEXT,
    store_state TEXT,
    store_country TEXT,
    store_phone TEXT,
    store_email TEXT
);

CREATE TABLE dim_suppliers (
    supplier_id INT PRIMARY KEY,
    seller_first_name TEXT,
    seller_last_name TEXT,
    seller_email TEXT,
    seller_country TEXT
);

CREATE TABLE fact_sales (
    id SERIAL PRIMARY KEY,
    product_id INT,
    customer_id INT,
    store_id INT,
    supplier_id INT,
    price FLOAT,
    rating FLOAT,
    review_id INT,
    order_date DATE
);