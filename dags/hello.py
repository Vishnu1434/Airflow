from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime


def hello_world():
    print("Hello World")


with DAG(
    dag_id="simple_hello_world_dag",
    start_date=datetime(2025, 8, 15),
    schedule_interval="@hourly",  # Run manually
    catchup=False,
    tags=["example"],
) as dag:

    start = DummyOperator(
        task_id="start"
    )

    python_hello = PythonOperator(
        task_id="python_hello",
        python_callable=hello_world
    )

    end = DummyOperator(
        task_id="end"
    )

    # Define DAG order: start -> python_hello -> end
    start >> python_hello >> end
