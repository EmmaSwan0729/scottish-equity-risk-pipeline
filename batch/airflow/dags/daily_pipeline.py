from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator 
import snowflake.connector
from datetime import datetime, timedelta
import logging
import sys
import os

def on_failure_callback(context):
    ti = context['task_instance']
    duration = (datetime.utcnow() - ti.start_date).total_seconds() if ti.start_date else None
    context.get('logical_date') or context.get('execution_date', 'N/A')
    logging.error(
        f"[FAILURE] DAG: {ti.dag_id} | Task: {ti.task_id} |"
        f"Execution: {context['execution_date']} |"
        f"Duration: {duration}s | Try: {ti.try_number}"
    )

def on_success_callback(context):
    ti = context['task_instance']
    duration = (ti.end_date - ti.start_date).total_seconds() if ti.start_date and ti.end_date else None
    context.get('logical_date') or context.get('execution_date', 'N/A')
    logging.info(
        f"[SUCCESS] DAG: {ti.dag_id} | Task: {ti.task_id} |"
        f"Execution: {context['execution_date']} |"
        f"Duration: {duration}s"
    )

def on_dag_success_callback(context):
    dag_id = context['dag'].dag_id
    execution_date = context.get('logical_date') or context.get('execution_date', 'N/A')
    logging.info(
        f"[PIPELINE COMPLETE] DAG: {dag_id} |"
        f"Execution: { execution_date} | Status: SUCCESS"
    )

def delete_today(ds: str, **kwargs):
    import os
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database="EQUITY_DB",
        schema="RAW",
        warehouse="EQUITY_WH",
    )
    cursor = conn.cursor()
    cursor.execute(f"DELETE FROM raw_stock_prices WHERE date = '{ds}'")
    deleted = cursor.rowcount
    logging.info(f"[IDEMPOTENCY] Deleted {deleted} rows for date={ds}")
    cursor.close()
    conn.close()

def fetch_and_upload(ds: str, **kwargs):
    from batch.ingestion.fetch_stock_data import fetch_stock_data, load_tickers
    from batch.ingestion.upload_to_s3 import upload_df_to_s3
    import os

    tickers = load_tickers()
    start_date = (datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=365)).strftime("%Y-%m-%d")

    df = fetch_stock_data(
        tickers=tickers,
        start_date=start_date,
        end_date=ds,
    )

    s3_path = upload_df_to_s3(
        df=df,
        bucket=os.getenv("S3_BUCKET_NAME"),
        date=ds,
    )

    logging.info(f"fetch_and_upload complete | date={ds} | rows={len(df)} | s3={s3_path}")

default_args = {
    'owner': 'emma',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout':timedelta(hours=1),
    'on_failure_callback': on_failure_callback,
    'on_success_callback': on_success_callback
}

with DAG(
    dag_id = 'scottish_equity_daily_pipeline',
    default_args=default_args,
    description='Daily pipeline: fetch stock data, upload to S3, run dbt',
    schedule='0 18 * * 1-5',
    start_date=datetime(2026,1,1),
    catchup=False,
    tags=['equity', 'risk', 'daily'],
    on_success_callback=on_dag_success_callback,
) as dag:

    delete_today_task = PythonOperator(
        task_id='delete_today',
        python_callable=delete_today,
        op_kwargs={'ds': '{{ ds }}'},
    )

    fetch_and_upload_task = PythonOperator(
        task_id='fetch_and_upload',
        python_callable=fetch_and_upload,
        op_kwargs={'ds': '{{ ds }}'},
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd $DBT_PROJECT_DIR && $VENV_DIR/bin/dbt run',
        env={
        'DBT_PROJECT_DIR': os.path.join(os.path.dirname(__file__), '../../equity_risk'),
        'VENV_DIR': os.path.join(os.path.dirname(__file__), '../../../.venv'),
        },
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd $DBT_PROJECT_DIR && $VENV_DIR/bin/dbt test',
        env={
        'DBT_PROJECT_DIR': os.path.join(os.path.dirname(__file__), '../../equity_risk'),
        'VENV_DIR': os.path.join(os.path.dirname(__file__), '../../../.venv'),
        },
    )

    delete_today_task >> fetch_and_upload_task >> dbt_run >> dbt_test
