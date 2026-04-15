import os
import boto3
from botocore.client import Config
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
)


def setup_bucket(endpoint, access_key, secret_key, bucket_name):
    """Ensures that the target bucket exists in MinIO before Spark writes to it."""
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )

    try:
        s3.head_bucket(Bucket=bucket_name)
    except Exception:
        try:
            s3.create_bucket(Bucket=bucket_name)
            print(f"Created missing bucket: {bucket_name}")
        except Exception as e:
            if "BucketAlready" not in str(e):
                print(f"Warning creating bucket {bucket_name}: {e}")


def main():
    MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER")
    MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD")
    MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:29092")

    if not all([MINIO_ACCESS_KEY, MINIO_SECRET_KEY]):
        raise ValueError("Missing MinIO credentials.")

    print("Checking MinIO destination bucket...")
    setup_bucket(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, "bronze")

    spark = (
        SparkSession.builder.appName("RetailEventStreaming")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    json_schema = StructType(
        [
            StructField("event_id", StringType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("action", StringType(), True),
            StructField("timestamp", TimestampType(), True),
        ]
    )

    print(f"Connecting to Kafka at {KAFKA_BROKER}...")

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", "retail_events")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = df.select(
        from_json(col("value").cast("string"), json_schema).alias("data")
    ).select("data.*")

    print("Starting stream to MinIO (s3a://bronze/retail/events_stream/)...")

    query = (
        parsed_df.writeStream.format("parquet")
        .option("checkpointLocation", "s3a://bronze/checkpoints/retail_events/")
        .option("path", "s3a://bronze/retail/events_stream/")
        .outputMode("append")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
