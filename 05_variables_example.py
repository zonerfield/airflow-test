from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator


def print_greeting():
    greeting = Variable.get(
        "training_greeting",
        default_var="Привет из default_var, переменная не найдена",
    )
    print(f"Greeting from Variable: {greeting}")


def show_config():
    config = Variable.get(
        "training_config",
        default_var='{"run_mode": "prod", "limit": 10}',
        deserialize_json=True,   # сразу парсим JSON в dict
    )
    run_mode = config.get("run_mode")
    limit = config.get("limit")
    print(f"Config from Variable: run_mode={run_mode}, limit={limit}")


with DAG(
    dag_id="variables_example",
    start_date=datetime(2024, 1, 1),
    schedule=None,      # Airflow 3.x: используем schedule, не schedule_interval
    catchup=False,
    tags=["training", "variables"],
) as dag:

    t1 = PythonOperator(
        task_id="print_greeting",
        python_callable=print_greeting,
    )

    t2 = PythonOperator(
        task_id="show_config",
        python_callable=show_config,
    )

    t1 >> t2
