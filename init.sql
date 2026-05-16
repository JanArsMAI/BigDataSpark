CREATE TABLE dim_products (
    product_id SERIAL PRIMARY KEY,
    name TEXT,
    category TEXT,
    brand TEXT,
    price DECIMAL,
    rating DECIMAL,
    reviews INT
);

CREATE TABLE dim_customers (
    customer_id SERIAL PRIMARY KEY,
    full_name TEXT,
    email TEXT,
    country TEXT,
    age INT
);

CREATE TABLE dim_stores (
    store_id SERIAL PRIMARY KEY,
    name TEXT,
    city TEXT,
    country TEXT
);

CREATE TABLE dim_suppliers (
    supplier_id SERIAL PRIMARY KEY,
    name TEXT,
    country TEXT
);

CREATE TABLE fact_sales (
    sale_id SERIAL PRIMARY KEY,
    sale_date DATE,
    product_id INT,
    customer_id INT,
    store_id INT,
    supplier_id INT,
    quantity INT,
    total_price DECIMAL
);