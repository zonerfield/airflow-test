from datetime import datetime
import os
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator

BASE_PATH = "/tmp/airflow_training"
os.makedirs(BASE_PATH, exist_ok=True)

INPUT_FILE = os.path.join(BASE_PATH, "input.csv")
OUTPUT_FILE = os.path.join(BASE_PATH, "result.txt")


def create_file():
    df = pd.DataFrame(
        [
            {"id": 1, "value": "a"},
            {"id": 2, "value": "b"},
            {"id": 3, "value": "c"},
        ]
    )
    df.to_csv(INPUT_FILE, index=False)
    print(f"Created {INPUT_FILE}")


def process_file():
    df = pd.read_csv(INPUT_FILE)
    rows_count = len(df)
    with open(OUTPUT_FILE, "w") as f:
        f.write(f"Rows count: {rows_count}\n")
    print(f"Wrote {OUTPUT_FILE} with rows_count={rows_count}")


with DAG(
    dag_id="file_etl_example",
    start_date=datetime(2024, 1, 1),
    schedule=None,      # было schedule_interval=None
    catchup=False,
    tags=["training"],
) as dag:

    create = PythonOperator(
        task_id="create_file",
        python_callable=create_file,
    )

    process = PythonOperator(
        task_id="process_file",
        python_callable=process_file,
    )

    create >> process
