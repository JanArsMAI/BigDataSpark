import sys
from typing import Dict, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def create_spark_session() -> SparkSession:
    """Creates and returns a SparkSession with PostgreSQL and ClickHouse JDBC drivers and memory configurations."""
    return (
        SparkSession.builder
        .appName("ETL to ClickHouse")
        .config("spark.jars", "/opt/jars/postgresql.jar,/opt/jars/clickhouse-jdbc.jar")
        .config("spark.driver.memory", "1g")
        .config("spark.driver.maxResultSize", "512m")
        .config("spark.sql.debug.maxToStringFields", "50")
        .config("spark.sql.autoBroadcastJoinThreshold", "10m")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )


def get_postgres_config() -> Dict[str, str]:
    """Returns PostgreSQL JDBC connection configuration."""
    return {
        "url": "jdbc:postgresql://postgres:5432/postgres",
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }


def get_clickhouse_config() -> Dict[str, str]:
    """Returns ClickHouse JDBC connection configuration."""
    return {
        "url": "jdbc:clickhouse://clickhouse:8123/analytics",
        "user": "default",
        "password": "",
        "driver": "com.clickhouse.jdbc.ClickHouseDriver"
    }


def read_table_from_postgres(spark: SparkSession, table: str, config: Dict[str, str]) -> DataFrame:
    """Reads a table from PostgreSQL."""
    return spark.read.format("jdbc").options(**config).option("dbtable", table).load()


def load_dimension_tables(spark: SparkSession, pg_config: Dict[str, str]) -> Dict[str, DataFrame]:
    """Loads all dimension tables from PostgreSQL."""
    return {
        "fact": read_table_from_postgres(spark, "fact_sales", pg_config),
        "products": read_table_from_postgres(spark, "dim_products", pg_config),
        "customers": read_table_from_postgres(spark, "dim_customers", pg_config),
        "stores": read_table_from_postgres(spark, "dim_stores", pg_config),
        "suppliers": read_table_from_postgres(spark, "dim_suppliers", pg_config)
    }


def create_sales_dataframe(dimensions: Dict[str, DataFrame]) -> DataFrame:
    """Creates the main sales dataframe by joining all dimension tables."""
    return (
        dimensions["fact"]
        .join(dimensions["products"], on="product_id", how="left")
        .join(dimensions["customers"], on="customer_id", how="left")
        .join(dimensions["stores"], on="store_id", how="left")
        .join(dimensions["suppliers"], on="supplier_id", how="left")
        .withColumn("order_date", F.to_date(F.col("order_date")))
        .withColumn("year", F.year(F.col("order_date")))
        .withColumn("month", F.month(F.col("order_date")))
        .withColumn(
            "customer_name",
            F.concat_ws(" ", F.col("first_name").cast("string"), F.col("last_name").cast("string")),
        )
        .withColumn(
            "supplier_name",
            F.concat_ws(" ", F.col("seller_first_name").cast("string"), F.col("seller_last_name").cast("string")),
        )
        .withColumn("review_count", F.when(F.col("review_id").isNull(), F.lit(0)).otherwise(F.lit(1)))
    )


def write_to_clickhouse(df: DataFrame, table: str, ch_config: Dict[str, str]) -> None:
    """Writes a DataFrame to ClickHouse table."""
    (
        df.write
        .format("jdbc")
        .options(**ch_config)
        .option("dbtable", table)
        .option("createTableOptions", "ENGINE = MergeTree() ORDER BY tuple()")
        .mode("overwrite")
        .save()
    )


def create_top_products_report(sales_df: DataFrame) -> DataFrame:
    """Creates top 10 products by sales count and revenue."""
    return (
        sales_df.groupBy("product_id", "product_name", "product_category")
        .agg(
            F.sum("price").alias("total_revenue"),
            F.count(F.lit(1)).alias("total_sales"),
        )
        .orderBy(F.col("total_sales").desc(), F.col("total_revenue").desc())
        .limit(10)
    )


def create_revenue_by_category_report(sales_df: DataFrame) -> DataFrame:
    """Creates revenue report by product category."""
    return (
        sales_df.groupBy("product_category")
        .agg(
            F.sum("price").alias("total_revenue"),
            F.count(F.lit(1)).alias("total_sales"),
        )
        .orderBy(F.col("total_revenue").desc())
    )


def create_product_rating_reviews_report(sales_df: DataFrame) -> DataFrame:
    """Creates product rating and reviews report."""
    return (
        sales_df.groupBy("product_id", "product_name", "product_category")
        .agg(
            F.avg("rating").alias("avg_rating"),
            F.sum("review_count").cast("long").alias("review_count"),
        )
        .orderBy(F.col("avg_rating").desc())
    )


def create_customer_analytics(sales_df: DataFrame) -> Dict[str, DataFrame]:
    """Creates various customer analytics reports."""
    customer_spend = (
        sales_df.groupBy("customer_id", "customer_name", "country")
        .agg(
            F.sum("price").alias("total_spent"),
            F.avg("price").alias("avg_check"),
            F.count(F.lit(1)).alias("orders_count"),
        )
    )
    
    return {
        "top_customers": (
            customer_spend.orderBy(F.col("total_spent").desc(), F.col("orders_count").desc())
            .limit(10)
        ),
        "customers_by_country": (
            sales_df.groupBy("country")
            .agg(F.countDistinct("customer_id").alias("customers_count"))
            .orderBy(F.col("customers_count").desc())
        ),
        "avg_check_by_customer": (
            customer_spend.select("customer_id", "customer_name", "country", "avg_check", "orders_count", "total_spent")
            .orderBy(F.col("avg_check").desc())
        )
    }


def create_time_analytics(sales_df: DataFrame) -> Dict[str, DataFrame]:
    """Creates various time-based analytics reports."""
    return {
        "monthly_trends": (
            sales_df.groupBy("year", "month")
            .agg(
                F.sum("price").alias("total_revenue"),
                F.count(F.lit(1)).alias("orders_count"),
            )
            .orderBy("year", "month")
        ),
        "yearly_trends": (
            sales_df.groupBy("year")
            .agg(
                F.sum("price").alias("total_revenue"),
                F.count(F.lit(1)).alias("orders_count"),
                F.avg("price").alias("avg_order_value"),
            )
            .orderBy("year")
        ),
        "avg_order_by_month": (
            sales_df.groupBy("year", "month")
            .agg(F.avg("price").alias("avg_order_value"))
            .orderBy("year", "month")
        )
    }


def create_store_analytics(sales_df: DataFrame) -> Dict[str, DataFrame]:
    """Creates various store analytics reports."""
    store_base = (
        sales_df.groupBy("store_id", "store_name", "store_city", "store_state")
        .agg(
            F.sum("price").alias("total_revenue"),
            F.avg("price").alias("avg_check"),
            F.count(F.lit(1)).alias("orders_count"),
        )
    )
    
    return {
        "top_stores": store_base.orderBy(F.col("total_revenue").desc()).limit(5),
        "sales_by_city_state": (
            store_base.groupBy("store_city", "store_state")
            .agg(
                F.sum("total_revenue").alias("total_revenue"),
                F.sum("orders_count").alias("orders_count"),
                F.avg("avg_check").alias("avg_check"),
            )
            .orderBy(F.col("total_revenue").desc())
        ),
        "avg_check_by_store": (
            store_base.select("store_id", "store_name", "store_city", "store_state", "avg_check", "orders_count", "total_revenue")
            .orderBy(F.col("avg_check").desc())
        )
    }


def create_supplier_analytics(sales_df: DataFrame) -> Dict[str, DataFrame]:
    """Creates various supplier analytics reports."""
    supplier_base = (
        sales_df.groupBy("supplier_id", "supplier_name", "seller_country")
        .agg(
            F.sum("price").alias("total_revenue"),
            F.avg("price").alias("avg_price"),
            F.count(F.lit(1)).alias("orders_count"),
        )
    )
    
    return {
        "top_suppliers": supplier_base.orderBy(F.col("total_revenue").desc()).limit(5),
        "avg_price_by_supplier": (
            supplier_base.select("supplier_id", "supplier_name", "seller_country", "avg_price", "orders_count", "total_revenue")
            .orderBy(F.col("avg_price").desc())
        ),
        "suppliers_by_country": (
            sales_df.groupBy("seller_country")
            .agg(
                F.sum("price").alias("total_revenue"),
                F.countDistinct("supplier_id").alias("suppliers_count"),
                F.count(F.lit(1)).alias("orders_count"),
            )
            .orderBy(F.col("total_revenue").desc())
        )
    }


def create_product_quality_analytics(sales_df: DataFrame) -> Dict[str, DataFrame]:
    """Creates product quality analytics reports."""
    product_quality = (
        sales_df.groupBy("product_id", "product_name", "product_category")
        .agg(
            F.avg("rating").alias("avg_rating"),
            F.count(F.lit(1)).alias("total_sales"),
            F.sum("review_count").cast("long").alias("review_count"),
        )
    )
    
    best_products = (
        product_quality.orderBy(F.col("avg_rating").desc(), F.col("total_sales").desc())
        .limit(10)
        .withColumn("segment", F.lit("best"))
    )
    worst_products = (
        product_quality.orderBy(F.col("avg_rating").asc(), F.col("total_sales").desc())
        .limit(10)
        .withColumn("segment", F.lit("worst"))
    )
    
    return {
        "best_worst_products": best_products.unionByName(worst_products),
        "rating_sales_corr": product_quality.select(F.corr("avg_rating", "total_sales").alias("rating_sales_correlation")),
        "most_reviews": product_quality.orderBy(F.col("review_count").desc(), F.col("total_sales").desc()).limit(10)
    }


def generate_all_reports(sales_df: DataFrame, ch_config: Dict[str, str]) -> None:
    """Generates and writes all analytics reports to ClickHouse."""
    # Product reports
    write_to_clickhouse(
        create_top_products_report(sales_df), 
        "analytics.report_products_top10", 
        ch_config
    )
    write_to_clickhouse(
        create_revenue_by_category_report(sales_df), 
        "analytics.report_products_revenue_by_category", 
        ch_config
    )
    write_to_clickhouse(
        create_product_rating_reviews_report(sales_df), 
        "analytics.report_products_rating_reviews", 
        ch_config
    )
    
    # Customer reports
    customer_reports = create_customer_analytics(sales_df)
    write_to_clickhouse(
        customer_reports["top_customers"], 
        "analytics.report_customers_top10", 
        ch_config
    )
    write_to_clickhouse(
        customer_reports["customers_by_country"], 
        "analytics.report_customers_by_country", 
        ch_config
    )
    write_to_clickhouse(
        customer_reports["avg_check_by_customer"], 
        "analytics.report_customers_avg_check", 
        ch_config
    )
    
    # Time-based reports
    time_reports = create_time_analytics(sales_df)
    write_to_clickhouse(
        time_reports["monthly_trends"], 
        "analytics.report_time_monthly_trends", 
        ch_config
    )
    write_to_clickhouse(
        time_reports["yearly_trends"], 
        "analytics.report_time_yearly_trends", 
        ch_config
    )
    write_to_clickhouse(
        time_reports["avg_order_by_month"], 
        "analytics.report_time_avg_order_by_month", 
        ch_config
    )
    
    # Store reports
    store_reports = create_store_analytics(sales_df)
    write_to_clickhouse(
        store_reports["top_stores"], 
        "analytics.report_stores_top5", 
        ch_config
    )
    write_to_clickhouse(
        store_reports["sales_by_city_state"], 
        "analytics.report_stores_by_city_state", 
        ch_config
    )
    write_to_clickhouse(
        store_reports["avg_check_by_store"], 
        "analytics.report_stores_avg_check", 
        ch_config
    )
    
    # Supplier reports
    supplier_reports = create_supplier_analytics(sales_df)
    write_to_clickhouse(
        supplier_reports["top_suppliers"], 
        "analytics.report_suppliers_top5", 
        ch_config
    )
    write_to_clickhouse(
        supplier_reports["avg_price_by_supplier"], 
        "analytics.report_suppliers_avg_price", 
        ch_config
    )
    write_to_clickhouse(
        supplier_reports["suppliers_by_country"], 
        "analytics.report_suppliers_by_country", 
        ch_config
    )
    
    # Product quality reports
    quality_reports = create_product_quality_analytics(sales_df)
    write_to_clickhouse(
        quality_reports["best_worst_products"], 
        "analytics.report_quality_best_worst_products", 
        ch_config
    )
    write_to_clickhouse(
        quality_reports["rating_sales_corr"], 
        "analytics.report_quality_rating_sales_correlation", 
        ch_config
    )
    write_to_clickhouse(
        quality_reports["most_reviews"], 
        "analytics.report_quality_most_reviews", 
        ch_config
    )


def main():
    """Main ETL pipeline execution."""
    spark = None
    try:
        spark = create_spark_session()
        
        pg_config = get_postgres_config()
        ch_config = get_clickhouse_config()
        
        dimensions = load_dimension_tables(spark, pg_config)
        sales_df = create_sales_dataframe(dimensions)
        
        generate_all_reports(sales_df, ch_config)
        
        print("ETL pipeline completed successfully! All reports generated.")
        
    except Exception as e:
        print(f"Error during ETL execution: {e}", file=sys.stderr)
        sys.exit(1)
        
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()