#!/bin/bash

# Big Data Spark Pipeline Runner

set -e 

echo "Starting Big Data Spark Pipeline..."

if ! docker info > /dev/null 2>&1; then
    echo "Docker is not running. Please start Docker first."
    exit 1
fi

echo "Starting Docker containers..."
docker-compose up -d

echo "Waiting for databases to start..."
sleep 10

echo "Checking PostgreSQL connection..."
until docker exec postgres_bd pg_isready -U postgres; do
    echo "Waiting for PostgreSQL..."
    sleep 2
done

echo "Checking ClickHouse connection..."
until docker exec clickhouse_bd clickhouse-client --query "SELECT 1"; do
    echo "Waiting for ClickHouse..."
    sleep 2
done

echo "Databases are ready!"

echo "🧹 Cleaning up existing tables..."
docker exec postgres_bd psql -U postgres -c "DROP TABLE IF EXISTS fact_sales CASCADE; DROP TABLE IF EXISTS dim_products CASCADE; DROP TABLE IF EXISTS dim_customers CASCADE; DROP TABLE IF EXISTS dim_stores CASCADE; DROP TABLE IF EXISTS dim_suppliers CASCADE;"

echo "Setting up PostgreSQL schema..."
cat sql/postgres_schema.sql | docker exec -i postgres_bd psql -U postgres

echo "Loading CSV data into PostgreSQL..."
docker exec postgres_bd psql -U postgres -c "
CREATE TABLE IF NOT EXISTS mock_data (
    id INT,
    customer_first_name TEXT,
    customer_last_name TEXT,
    customer_age INT,
    customer_email TEXT,
    customer_country TEXT,
    customer_postal_code TEXT,
    customer_pet_type TEXT,
    customer_pet_name TEXT,
    customer_pet_breed TEXT,
    seller_first_name TEXT,
    seller_last_name TEXT,
    seller_email TEXT,
    seller_country TEXT,
    seller_postal_code TEXT,
    product_name TEXT,
    product_category TEXT,
    product_price FLOAT,
    product_quantity INT,
    sale_date TEXT,
    sale_customer_id INT,
    sale_seller_id INT,
    sale_product_id INT,
    sale_quantity INT,
    sale_total_price FLOAT,
    store_name TEXT,
    store_location TEXT,
    store_city TEXT,
    store_state TEXT,
    store_country TEXT,
    store_phone TEXT,
    store_email TEXT,
    pet_category TEXT,
    product_weight FLOAT,
    product_color TEXT,
    product_size TEXT,
    product_brand TEXT,
    product_material TEXT,
    product_description TEXT,
    product_rating FLOAT,
    product_reviews INT,
    product_release_date TEXT,
    product_expiry_date TEXT,
    supplier_name TEXT,
    supplier_contact TEXT,
    supplier_email TEXT,
    supplier_phone TEXT,
    supplier_address TEXT,
    supplier_city TEXT,
    supplier_country TEXT
);
"

docker exec postgres_bd psql -U postgres -c "\copy mock_data FROM '/data/MOCK_DATA.csv' WITH CSV HEADER"

echo "Setting up ClickHouse analytics schema..."
cat sql/analytics_schema.sql | docker exec -i clickhouse_bd clickhouse-client --database=default

echo "Running star schema creation..."
docker exec spark_bd /opt/spark/bin/spark-submit \
    --jars /opt/jars/postgresql.jar \
    --master local[*] \
    /opt/jobs/create_star_schema.py

echo "Generating analytics reports..."
docker exec spark_bd /opt/spark/bin/spark-submit \
    --jars /opt/jars/postgresql.jar,/opt/jars/clickhouse-jdbc.jar \
    --master local[*] \
    /opt/jobs/generate_analytics.py

echo " Pipeline completed successfully!"
echo ""
echo "Analytics are now available in ClickHouse at http://localhost:8123"
echo "PostgreSQL is available at localhost:5433"
echo ""
echo "To check analytics data, run:"
echo "docker exec clickhouse_bd clickhouse-client --query='SELECT * FROM analytics.report_products_top10 LIMIT 5'"