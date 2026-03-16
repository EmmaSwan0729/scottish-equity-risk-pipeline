import boto3
import pandas as pd
from io import StringIO, BytesIO
from datetime import datetime
import logging
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

S3_BUCKET = os.getenv("S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")

def upload_df_to_s3(
        df: pd.DataFrame,
        bucket: str,
        date: str,
) -> str:
    s3_key = f"raw/stock_prices/dt={date}/stock_prices.parquet"

    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)

    s3_client = boto3.client("s3", region_name=AWS_REGION)

    s3_client.put_object(
        Bucket = bucket,
        Key = s3_key,
        Body = parquet_buffer.getvalue(),
        ContentType = "application/octet-stream",
    )

    s3_path = f"s3://{bucket}/{s3_key}"
    logger.info(f"Upload {len(df)} rows to {s3_path}")
    return s3_path

if __name__ == "__main__":
    from fetch_stock_data import fetch_stock_data, SCOTTISH_TICKERS
    from datetime import timedelta

    end = datetime.today().strftime("%Y-%m-%d")
    start = (datetime.today() - timedelta(days=365)).strftime("%Y-%m-%d")

    df = fetch_stock_data(
        tickers = SCOTTISH_TICKERS,
        start_date = start,
        end_date = end,
    )

    s3_path = upload_df_to_s3(
        df=df,
        bucket=S3_BUCKET,
        date=end
    )

    print(f"\nUploaded to {s3_path}")
