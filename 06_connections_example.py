from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# Hook из Postgres-провайдера
from airflow.providers.postgres.hooks.postgres import PostgresHook


def query_postgres():
    # postgres_conn_id = Conn Id из UI
    hook = PostgresHook(postgres_conn_id="pg_training")

    # Простой запрос — чтобы не зависеть от структуры БД
    # Можно заменить на свою таблицу
    records = hook.get_records("SELECT 1 AS num")

    print(f"Records from Postgres: {records}")


with DAG(
    dag_id="connections_example",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["training", "connections"],
) as dag:

    run_query = PythonOperator(
        task_id="query_postgres",
        python_callable=query_postgres,
    )
