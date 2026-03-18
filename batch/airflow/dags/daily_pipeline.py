from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator 
from datetime import datetime, timedelta

default_args = {
    'owner': 'emma',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id = 'scottish_equity_daily_pipeline',
    default_args=default_args,
    description='Daily pipeline: fetch stock data, upload to S3, run dbt',
    schedule='0 18 * * 1-5',
    start_date=datetime(2026,1,1),
    catchup=False,
    tags=['equity', 'risk', 'daily'],
) as dag:

    fetch_stock_data = BashOperator(
        task_id='fetch_stock_data',
        bash_command='cd /opt/airflow && source .venv/bin/activate && python batch/ingestion/fetch_stock_data.py',
)

    upload_to_s3 = BashOperator(
        task_id='upload_to_s3',
        bash_command='cd /opt/airflow && source .venv/bin/activate && python batch/ingestion/upload_to_s3.py',)

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/batch/equity_risk && source ../../.venv/bin/activate && dbt run',
)
    fetch_stock_data >> upload_to_s3 >> dbt_run
