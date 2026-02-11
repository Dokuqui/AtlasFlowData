FROM apache/airflow:2.10.4-python3.10

USER root

RUN apt-get update -o Acquire::Check-Valid-Until=false -o Acquire::Check-Date=false && \
    apt-get install -y openjdk-17-jdk procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow-providers-apache-spark" \
    "pyspark==3.5.3" \
    "pandas" \
    "boto3" \
    "pyyaml" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.10.txt"