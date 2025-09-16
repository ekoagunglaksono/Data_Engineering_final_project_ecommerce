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
    tags=["ecommerce", "elt"],
) as dag:

    generate_data = BashOperator(
        task_id="generate_data",
        bash_command="python /opt/airflow/data_generator/generate_data.py ",
    )


    load_to_bigquery = BashOperator(
        task_id="load_to_bigquery",
        bash_command="python /opt/airflow/scripts/bq_loader.py",
    )


    run_dbt_staging = BashOperator(
        task_id="run_dbt_staging",
        bash_command=(
            "export PATH=$PATH:/home/airflow/.local/bin && "
            "export DBT_PROFILES_DIR=/opt/airflow/dbt_ecommerce && "
            "cd /opt/airflow/dbt_ecommerce && "
            "dbt run --select staging && "
            "dbt test --select staging"
        ),
        env={"GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/keys/purwadika-502e43f1636d.json"}
    )

    generate_data >> load_to_bigquery >> run_dbt_staging
