import random
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago


def generate_and_load_data():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = hook.get_sqlalchemy_engine()

    users_data = [
        {"user_id": i, "email": f"user_{i}@example.com", "signup_date": datetime.now()}
        for i in range(1, 101)
    ]
    users_df = pd.DataFrame(users_data)

    orders_data = []
    for i in range(1, 21):
        orders_data.append(
            {
                "order_id": i,
                "user_id": random.randint(1, 100),
                "product_id": random.randint(1, 50),
                "amount": round(random.uniform(10.0, 100.0), 2),
                "status": "completed" if random.random() < 0.8 else "failed",
                "order_date": datetime.now(),
            }
        )
    orders_df = pd.DataFrame(orders_data)

    users_df.to_sql("users", con=engine, if_exists="replace", index=False)
    orders_df.to_sql("orders", con=engine, if_exists="replace", index=False)

    print(f"Successfully inserted {len(users_df)} users and {len(orders_df)} orders.")


with DAG(
    dag_id="init_source_db",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["setup"],
) as dag:
    create_tables = PythonOperator(
        task_id="create_fake_data",
        python_callable=generate_and_load_data,
    )
