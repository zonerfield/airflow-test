from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def get_number():
    return 7

def multiply(ti):  # ti = TaskInstance из контекста
    number = ti.xcom_pull(task_ids="get_number")
    result = number * 10
    print(f"Result is: {result}")

with DAG(
    dag_id="xcom_example",
    start_date=datetime(2024, 1, 1),
    schedule=None,          # <-- тут главное изменение
    catchup=False,
    tags=["training"],
) as dag:

    get_number_task = PythonOperator(
        task_id="get_number",
        python_callable=get_number,
    )

    multiply_task = PythonOperator(
        task_id="multiply",
        python_callable=multiply,
    )

    get_number_task >> multiply_task
