import os
import pandas as pd
from google.cloud import bigquery

# Konfigurasi
PROJECT_ID = "purwadika"
DATASET_ID = "jcdeol005_finalproject_eko_raw"
DATA_DIR = "/opt/airflow/data"

# Mapping file CSV ke nama tabel di BigQuery
TABLES = {
    "suppliers.csv": "suppliers",
    "users.csv": "users",
    "products.csv": "products",
    "orders.csv": "orders",
    "order_items.csv": "order_items",
    "payments.csv": "payments",
    "shipments.csv": "shipments",
    "reviews.csv": "reviews",
}

def load_csv_to_bq():
    client = bigquery.Client(project=PROJECT_ID)

    for file_name, table_name in TABLES.items():
        file_path = os.path.join(DATA_DIR, file_name)

        if not os.path.exists(file_path):
            print(f"File {file_name} tidak ditemukan, skip.")
            continue

        print(f"Membaca {file_name} ...")
        df = pd.read_csv(file_path)

        # --- PATCH: normalisasi dtype ---
        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = pd.to_numeric(df[col], errors="ignore")

        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
        print(f"Upload {len(df)} baris ke {table_id}")

        # --- PATCH: job_config untuk konsistensi load ---
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE"  
        )

        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  

        print(f"Selesai upload {table_name}")

if __name__ == "__main__":
    print("Mulai load semua CSV ke BigQuery...")
    load_csv_to_bq()
    print("Semua data berhasil dimuat ke BigQuery!")
