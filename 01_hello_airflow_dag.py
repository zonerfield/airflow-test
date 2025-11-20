from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="hello_airflow",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # запуск только вручную
    catchup=False,
    tags=["training"],
) as dag:

    hello = BashOperator(
        task_id="print_date",
        bash_command="echo 'Today is: ' && date",
    )
