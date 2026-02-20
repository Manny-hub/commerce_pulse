from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from datetime import timedelta
from utils import run_ingest

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="commerce_pulse_mongodb_ingest",
    default_args=default_args,
    description="Daily live ingest from CommercePulse Api to MongoDB",
    schedule="@daily",
    start_date=timezone.datetime(2026, 2, 20),
    catchup=False,
) as dag:

    ingest_task = PythonOperator(
        task_id="fetch_and_upsert_to_mongo",
        python_callable=run_ingest,
        provide_context=True
    )