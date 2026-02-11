from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago

CREATE_TABLES_SQL = """
    CREATE TABLE IF NOT EXISTS users (
        user_id SERIAL PRIMARY KEY,
        email VARCHAR(100),
        signup_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS orders (
        order_id SERIAL PRIMARY KEY,
        user_id INT,
        product_id INT,
        amount DECIMAL(10, 2),
        status VARCHAR(20),
        order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Insert fake users
    INSERT INTO users (email)
    SELECT 
        'user_' || generate_series || '@example.com'
    FROM generate_series(1, 100)
    WHERE NOT EXISTS (SELECT 1 FROM users);

    -- Insert fake orders
    INSERT INTO orders (user_id, product_id, amount, status) 
    SELECT 
        floor(random() * 100 + 1)::int,
        floor(random() * 50 + 1)::int,
        (random() * 100)::decimal(10,2),
        CASE WHEN random() < 0.8 THEN 'completed' ELSE 'failed' END
    FROM generate_series(1, 20)
    WHERE NOT EXISTS (SELECT 1 FROM orders);
"""

with DAG(
    dag_id="init_source_db",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["setup"],
) as dag:
    create_tables = SQLExecuteQueryOperator(
        task_id="create_fake_data",
        conn_id="postgres_default",
        sql=CREATE_TABLES_SQL,
    )
