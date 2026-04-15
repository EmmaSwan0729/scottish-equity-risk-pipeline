from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator 
from datetime import datetime, timedelta
import logging

def on_failure_callback(context):
    ti = context['task_instance']
    duration = (datetime.utcnow() - ti.start_date).seconds if ti.start_date else None
    logging.error(
        f"[FAILURE] DAG: {ti.dag_id} | Task: {ti.task_id} |"
        f"Excecution: {context['execution_date']} |"
        f"Duration: {duration}s | Try: {ti.try_number}"
    )

def on_success_callback(context):
    ti = context['task_instance']
    duration = (ti.end_date - ti.start_date).seconds if ti.start_date and ti.end_date else None
    logging.info(
        f"[SUCCESS] DAG: {ti.dag_id} | Task: {ti.task_id} |"
        f"Execution: {context['execution_date']} |"
        f"Duration: {duration}s"
    )

def on_dag_success_callback(context):
    dag_id = context['dag'].dag_id
    execution_date = context['execution_date']
    logging.info(
        f"[PIPELINE COMPLETE] DAG: {dag_id} |"
        f"Execution: { execution_date} | Status: SUCCESS"
    )

default_args = {
    'owner': 'emma',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout':timedelta(hours=1),
    'on_failure_callback': on_failure_callback,
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
        bash_command='cd /opt/airflow && source .venv/bin/activate && python batch/ingestion/upload_to_s3.py',
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/batch/equity_risk && source ../../.venv/bin/activate && dbt run',
    )
    fetch_stock_data >> upload_to_s3 >> dbt_run
