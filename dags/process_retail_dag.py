import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

SPARK_COMMAND = """
/home/airflow/.local/bin/spark-submit \
    --master spark://spark-master:7077 \
    --name "Retail_Silver_Gold_Processing" \
    --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    /opt/airflow/src/jobs/process_retail.py
"""

with DAG(
    dag_id="process_retail_data",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["retail", "processing", "spark"],
) as dag:
    submit_spark_job = BashOperator(
        task_id="run_spark_processing",
        bash_command=SPARK_COMMAND,
        env={
            "MINIO_ROOT_USER": os.environ.get("MINIO_ROOT_USER"),
            "MINIO_ROOT_PASSWORD": os.environ.get("MINIO_ROOT_PASSWORD"),
            "MINIO_ENDPOINT": os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        },
        append_env=True,
    )
