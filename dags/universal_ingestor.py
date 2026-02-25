import os
import yaml
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from io import BytesIO
import boto3
from botocore.client import Config

CONFIG_PATH = "/opt/airflow/configs"


def get_minio_client():
    minio_access_key = os.environ.get("MINIO_ROOT_USER")
    minio_secret_key = os.environ.get("MINIO_ROOT_PASSWORD")
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")

    if not minio_access_key or not minio_secret_key:
        raise ValueError("MinIO credentials are missing from environment variables.")

    return boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def ingest_table(table_name, destination_path, conn_id, **kwargs):
    print(f"Starting ingestion for table: {table_name}")

    pg_hook = PostgresHook(postgres_conn_id=conn_id)
    df = pg_hook.get_pandas_df(sql=f"SELECT * FROM {table_name}")
    print(f"Extracted {len(df)} rows from {table_name}")

    for col_name in df.select_dtypes(
        include=["datetime64[ns]", "datetime64[ns, UTC]"]
    ).columns:
        df[col_name] = df[col_name].astype("datetime64[us]")

    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)

    s3 = get_minio_client()
    bucket_name = "bronze"
    file_key = f"{destination_path}{table_name}.parquet"

    try:
        s3.head_bucket(Bucket=bucket_name)
    except Exception as e:
        try:
            s3.create_bucket(Bucket=bucket_name)
        except Exception as create_error:
            if "BucketAlreadyOwnedByYou" not in str(
                create_error
            ) and "BucketAlreadyExists" not in str(create_error):
                raise create_error

    s3.put_object(Bucket=bucket_name, Key=file_key, Body=parquet_buffer.getvalue())
    print(f"Successfully uploaded to s3://{bucket_name}/{file_key}")


def load_configs():
    """Reads all YAML files from the config folder"""
    configs = []
    if os.path.exists(CONFIG_PATH):
        for filename in os.listdir(CONFIG_PATH):
            if filename.endswith(".yaml"):
                file_path = os.path.join(CONFIG_PATH, filename)
                with open(file_path, "r") as f:
                    content = yaml.safe_load(f)
                    if content:
                        configs.append(content)
                    else:
                        print(f"Warning: Skipped empty config file: {filename}")
    return configs


for config in load_configs():
    domain = config.get("domain", "default_domain")

    with DAG(
        dag_id=f"universal_ingestor_{domain}",
        schedule_interval="@daily",
        start_date=days_ago(1),
        catchup=False,
        tags=["universal", domain],
    ) as dag:
        if "sources" in config:
            for source in config["sources"]:
                if source["type"] == "database":
                    conn_id = source["connection_id"]
                    for table in source["tables"]:
                        table_name = table["table_name"]
                        dest_path = table["destination_path"]

                        PythonOperator(
                            task_id=f"ingest_{table_name}",
                            python_callable=ingest_table,
                            op_kwargs={
                                "table_name": table_name,
                                "destination_path": dest_path,
                                "conn_id": conn_id,
                            },
                        )
