import os
import boto3
from botocore.client import Config
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    sum as _sum,
    count,
    current_timestamp,
    when,
)


def setup_buckets(endpoint, access_key, secret_key):
    """Ensures that the silver and gold buckets exist in MinIO."""
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )

    for bucket in ["silver", "gold"]:
        try:
            s3.head_bucket(Bucket=bucket)
        except Exception:
            try:
                s3.create_bucket(Bucket=bucket)
                print(f"Created missing bucket: {bucket}")
            except Exception as e:
                if "BucketAlready" not in str(e):
                    print(f"Warning creating bucket {bucket}: {e}")


def main():
    MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER")
    MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD")
    MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")

    POSTGRES_USER = os.environ.get("POSTGRES_USER")
    POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
    POSTGRES_DB = os.environ.get("POSTGRES_DB")
    POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")

    if not all([MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_ENDPOINT]):
        raise ValueError("Missing one or more required MinIO environment variables.")

    print("Checking MinIO destination buckets...")
    setup_buckets(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY)

    jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:5432/{POSTGRES_DB}"
    jdbc_properties = {
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
    }

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

    print("Processing Silver Layer (Orders)...")
    orders_df = spark.read.parquet("s3a://bronze/retail/orders/*.parquet")

    silver_orders = (
        orders_df.dropDuplicates(["order_id"])
        .withColumn("order_date", to_date(col("order_date")))
        .withColumn("amount", col("amount").cast("double"))
        .filter(col("amount") > 0)
        .withColumn("_processed_at", current_timestamp())
    )
    silver_orders.write.mode("overwrite").parquet("s3a://silver/retail/orders/")

    print("Processing Gold Layer (Daily Sales)...")
    daily_sales = (
        silver_orders.groupBy("order_date", "status")
        .agg(
            _sum("amount").alias("total_revenue"),
            count("order_id").alias("total_orders"),
        )
        .orderBy("order_date")
    )
    daily_sales.write.mode("overwrite").parquet("s3a://gold/retail/daily_sales/")

    print("Pushing Daily Sales to Postgres Data Warehouse...")
    daily_sales.write.jdbc(
        url=jdbc_url,
        table="gold_daily_sales",
        mode="overwrite",
        properties=jdbc_properties,
    )
    daily_sales.show(5)

    print("Processing Silver Layer (Events)...")
    try:
        events_df = spark.read.parquet("s3a://bronze/retail/events_stream/")

        silver_events = (
            events_df.dropDuplicates(["event_id"])
            .withColumn("event_date", to_date(col("timestamp")))
            .withColumn("_processed_at", current_timestamp())
        )
        silver_events.write.mode("overwrite").parquet("s3a://silver/retail/events/")

        print("Processing Gold Layer (Product Engagement)...")
        product_engagement = (
            silver_events.groupBy("product_id")
            .agg(
                count(when(col("action") == "view_item", True)).alias("views"),
                count(when(col("action") == "add_to_cart", True)).alias(
                    "added_to_cart"
                ),
                count(when(col("action") == "checkout", True)).alias("checkouts"),
            )
            .orderBy(col("views").desc())
        )
        product_engagement.write.mode("overwrite").parquet(
            "s3a://gold/retail/product_engagement/"
        )

        print("Pushing Product Engagement to Postgres Data Warehouse...")
        product_engagement.write.jdbc(
            url=jdbc_url,
            table="gold_product_engagement",
            mode="overwrite",
            properties=jdbc_properties,
        )
        product_engagement.show(5)

    except Exception as e:
        print(f"Skipped events processing (stream might be empty): {e}")

    spark.stop()


if __name__ == "__main__":
    main()
