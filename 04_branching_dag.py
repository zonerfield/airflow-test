from datetime import datetime, timedelta
import random

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "sla": timedelta(minutes=5),
}

def choose_path():
    path = random.choice(["path_a", "path_b"])
    print(f"Chosen path: {path}")
    return path

def run_path(name):
    print(f"Running {name}")

with DAG(
    dag_id="branching_example",
    start_date=datetime(2024, 1, 1),
    schedule=None,          # вместо schedule_interval
    catchup=False,
    default_args=default_args,
    tags=["training"],
) as dag:

    start = EmptyOperator(task_id="start")

    decide = BranchPythonOperator(
        task_id="decide",
        python_callable=choose_path,
    )

    path_a = PythonOperator(
        task_id="path_a",
        python_callable=lambda: run_path("A"),
    )

    path_b = PythonOperator(
        task_id="path_b",
        python_callable=lambda: run_path("B"),
    )

    join = EmptyOperator(
        task_id="join",
        trigger_rule="none_failed_mi_
