import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum as _sum, count, current_timestamp


def main():
    MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER")
    MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD")
    MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT")

    if not all([MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_ENDPOINT]):
        raise ValueError("Missing one or more required MinIO environment variables.")

    spark = (
        SparkSession.builder.appName("RetailProcessing")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .getOrCreate()
    )

    print("Starting Retail Processing Job...")

    print("Processing Silver Layer...")
    orders_df = spark.read.parquet("s3a://bronze/retail/orders/*.parquet")

    silver_orders = (
        orders_df.dropDuplicates(["order_id"])
        .withColumn("order_date", to_date(col("order_date")))
        .withColumn("amount", col("amount").cast("double"))
        .filter(col("amount") > 0)
        .withColumn("_processed_at", current_timestamp())
    )

    silver_orders.write.mode("overwrite").parquet("s3a://silver/retail/orders/")
    print(f"Silver Orders processed: {silver_orders.count()} rows")

    print("Processing Gold Layer...")
    daily_sales = (
        silver_orders.groupBy("order_date", "status")
        .agg(
            _sum("amount").alias("total_revenue"),
            count("order_id").alias("total_orders"),
        )
        .orderBy("order_date")
    )

    daily_sales.write.mode("overwrite").parquet("s3a://gold/retail/daily_sales/")
    print("Gold Daily Sales processed.")
    daily_sales.show()

    spark.stop()


if __name__ == "__main__":
    main()
