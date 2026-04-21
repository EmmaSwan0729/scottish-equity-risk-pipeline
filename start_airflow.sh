#!/bin/bash
cd /Users/moxingxing/Documents/CV_Project/Finance/scottish-equity-risk-pipeline

source .venv/bin/activate
source .env
export SNOWFLAKE_ACCOUNT SNOWFLAKE_USER SNOWFLAKE_PASSWORD
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_REGION S3_BUCKET_NAME
export PYTHONPATH=/Users/moxingxing/Documents/CV_Project/Finance/scottish-equity-risk-pipeline
export no_proxy='*'
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

airflow api-server -p 8080 &
sleep 5
airflow dag-processor &
sleep 3
airflow scheduler &

echo "Airflow started! UI at http://localhost:8080"
