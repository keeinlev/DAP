from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import timedelta, datetime

default_args = {
    'owner': 'dap',
    'depends_on_past': False,
    'email': ['k327lee@uwaterloo.ca'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    "copy_s3_events_to_snowflake",
    "Calls a COPY TO command in Snowflake to ingest the raw S3 events into the Analytics DB.",
    default_args=default_args,
    start_date=datetime(2026, 2, 26),
    schedule_interval=timedelta(minutes=5),
    catchup=False
) as dag:
    copy_s3_to_snowflake = PythonOperator()
