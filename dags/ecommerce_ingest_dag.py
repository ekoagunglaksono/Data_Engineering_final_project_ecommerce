from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ecommerce_ingest_dag",
    default_args=default_args,
    description="Generate dummy ecommerce data and load to BigQuery",
    start_date=datetime(2025, 9, 11),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    generate_data = BashOperator(
        task_id="generate_data",
        bash_command="python /opt/airflow/data_generator/generate_data.py ",
    )

    generate_data
