
FROM apache/airflow:2.7.1-python3.9

RUN pip install --no-cache-dir \
    "google-cloud-bigquery>=3.17.0" \
    "google-auth>=2.33.0" \
    pandas \
    pyarrow \
    faker \
    dbt-core==1.10.11 \
    dbt-bigquery==1.10.2
