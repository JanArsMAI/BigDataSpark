import sys
from typing import Dict
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, monotonically_increasing_id


def create_spark_session() -> SparkSession:
    """Creates and returns a SparkSession with PostgreSQL JDBC driver."""
    return (
        SparkSession.builder
        .appName("ETL to Star Schema")
        .config("spark.jars", "/opt/jars/postgresql.jar")
        .getOrCreate()
    )


def get_jdbc_properties() -> Dict[str, str]:
    """Returns JDBC connection properties for PostgreSQL."""
    return {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }


def load_source_data(spark: SparkSession, jdbc_url: str, properties: Dict[str, str]) -> DataFrame:
    """Loads the source mock_data table from PostgreSQL."""
    return spark.read.jdbc(jdbc_url, "mock_data", properties=properties)


def create_products_dimension(df: DataFrame) -> DataFrame:
    """Creates the products dimension table."""
    return (
        df.select(
            col("sale_product_id").alias("product_id"),
            col("product_name"),
            col("product_category"),
            col("product_price")
        )
        .dropDuplicates(["product_id"])
    )


def create_customers_dimension(df: DataFrame) -> DataFrame:
    """Creates the customers dimension table."""
    return (
        df.select(
            col("sale_customer_id").alias("customer_id"),
            col("customer_first_name").alias("first_name"),
            col("customer_last_name").alias("last_name"),
            col("customer_country").alias("country")
        )
        .dropDuplicates(["customer_id"])
    )


def create_stores_dimension(df: DataFrame) -> DataFrame:
    """Creates the stores dimension table with auto-generated IDs."""
    stores_raw = (
        df.select(
            col("store_city"),
            col("store_name"),
            col("store_state"),
            col("store_country"),
            col("store_email"),
            col("store_phone")
        )
        .dropDuplicates()
    )
    
    return stores_raw.withColumn(
        "store_id",
        monotonically_increasing_id()
    )


def create_suppliers_dimension(df: DataFrame) -> DataFrame:
    """Creates the suppliers dimension table with auto-generated IDs."""
    suppliers_raw = (
        df.select(
            col("seller_first_name"),
            col("seller_last_name"),
            col("seller_email"),
            col("seller_country")
        )
        .dropDuplicates()
    )
    
    return suppliers_raw.withColumn(
        "supplier_id",
        monotonically_increasing_id()
    )


def create_fact_sales_table(df: DataFrame, dim_stores: DataFrame, dim_suppliers: DataFrame) -> DataFrame:
    """Creates the fact sales table by joining with dimension tables."""
    fact_base = (
        df.select(
            col("sale_product_id").alias("product_id"),
            col("sale_customer_id").alias("customer_id"),
            col("store_city"),
            col("store_name"),
            col("store_state"),
            col("store_country"),
            col("store_email"),
            col("store_phone"),
            col("seller_first_name"),
            col("seller_last_name"),
            col("seller_email"),
            col("seller_country"),
            col("product_price").alias("price"),
            col("product_rating").alias("rating"),
            col("product_reviews").alias("review_id"),
            col("sale_date").alias("order_date")
        )
    )
    
    fact = fact_base.join(
        dim_stores,
        on=[
            "store_city",
            "store_name",
            "store_state",
            "store_country",
            "store_email",
            "store_phone"
        ],
        how="left"
    )
    
    fact = fact.join(
        dim_suppliers,
        on=[
            "seller_first_name",
            "seller_last_name",
            "seller_email",
            "seller_country"
        ],
        how="left"
    )
    
    return fact.select(
        "product_id",
        "customer_id",
        "store_id",
        "supplier_id",
        "price",
        "rating",
        "review_id",
        "order_date"
    )


def write_to_postgresql(df: DataFrame, table_name: str, jdbc_url: str, properties: Dict[str, str]) -> None:
    """Writes a DataFrame to PostgreSQL table."""
    # First truncate the table if it exists
    try:
        df.write.jdbc(jdbc_url, table_name, "overwrite", properties)
    except Exception as e:
        # If overwrite fails due to constraints, try to truncate first
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if spark:
            properties_copy = properties.copy()
            properties_copy['truncate'] = 'true'
            df.write.jdbc(jdbc_url, table_name, "overwrite", properties_copy)
        else:
            raise e


def main():
    """Main ETL pipeline execution."""
    spark = None
    try:
        spark = create_spark_session()
        
        jdbc_url = "jdbc:postgresql://postgres:5432/postgres"
        properties = get_jdbc_properties()
        
        source_df = load_source_data(spark, jdbc_url, properties)
        
        dim_products = create_products_dimension(source_df)
        dim_customers = create_customers_dimension(source_df)
        dim_stores = create_stores_dimension(source_df)
        dim_suppliers = create_suppliers_dimension(source_df)
        
        fact_sales = create_fact_sales_table(source_df, dim_stores, dim_suppliers)
        
        write_to_postgresql(dim_products, "dim_products", jdbc_url, properties)
        write_to_postgresql(dim_customers, "dim_customers", jdbc_url, properties)
        write_to_postgresql(dim_stores, "dim_stores", jdbc_url, properties)
        write_to_postgresql(dim_suppliers, "dim_suppliers", jdbc_url, properties)
        write_to_postgresql(fact_sales, "fact_sales", jdbc_url, properties)
        
        print("ETL pipeline completed successfully!")
        
    except Exception as e:
        print(f"Error during ETL execution: {e}", file=sys.stderr)
        sys.exit(1)
        
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()