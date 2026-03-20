import json
import uuid
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, stddev, avg, max as spark_max, min as spark_min
)
from pyspark.sql.types import StructType, StructField, StringType, FloatType,LongType
import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

# Snowflake connection config — credentials loaded from .env
SNOWFLAKE_CONFIG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "database": "EQUITY_DB",
    "schema": "ALERTS",
    "warehouse": "EQUITY_WH",
}

# Alert thresholds — tune these to adjust sensitivity
VOLATILITY_THRESHOLD = 0.02
DROP_THRESHOLD = 0.03
ZSCORE_THRESHOLD = 2.0

#Schema for incoming Kafka messages (matches kafka_producer.py output)
message_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("timestamp", LongType(), True),
])

def write_to_snowflake(symbol, alert_type, metric_value, threshold, price):
    """Write a single risk alert to Snowflake ALERTS.risk_alerts table."""
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO risk_alerts
                (ALERT_ID, SYMBOL, ALERT_TYPE, METRIC_VALUE, THRESHOLD_VALUE, PRICE_AT_ALERT, TRIGGERED_AT)
            VALUES(%s, %s, %s, %s, %s, %s, %s)
        """, (
            str(uuid.uuid4()),
            symbol,
            alert_type,
            float(metric_value),
            float(threshold),
            float(price),
            datetime.utcnow()
        ))
        conn.commit()
        cursor.close()
        conn.close()
        print(f"[ALERT] {symbol} | {alert_type} | value={metric_value:.4f} | threshold={threshold}")
    except Exception as e:
        print(f"[error] Snowflake write failed: {e}")

def process_batch(batch_df, batch_id):
    """Process each micro-batch: compute risk metrics and trigger alerts."""
    if batch_df.isEmpty():
        return
    
    metrics = (
        batch_df.groupBy("symbol")
        .agg(
            stddev("price").alias("volatility"),
            avg("price").alias("avg_price"),
            spark_max("price").alias("max_price"),
            spark_min("price").alias("min_price"),
        )
    )

    rows = metrics.collect()

    for row in rows:
        symbol = row["symbol"]
        volatility = row["volatility"] or 0.0
        avg_price = row["avg_price"] or 0.0
        max_price = row["max_price"] or 0.0
        min_price = row["min_price"] or 0.0

        latest = (
            batch_df.filter(col("symbol") == symbol)
            .orderBy(col("timestamp").desc())
            .first()
        )
        current_price = latest["price"] if latest else avg_price

        # ① Volatility alert — triggered if rolling volatility exceeds threshold
        volatility_pct = volatility /avg_price if avg_price > 0 else 0
        if volatility_pct > VOLATILITY_THRESHOLD:
            write_to_snowflake(
                symbol, "HIGH_VOLATILITY",
                volatility_pct, VOLATILITY_THRESHOLD, current_price
            )
        
        # ② Price drop alert — triggered if price falls more than 3% from window high
        drop_pct = (max_price - current_price) / max_price if max_price >0 else 0
        if drop_pct > DROP_THRESHOLD:
            write_to_snowflake(
                symbol,"PRICE_DROP",
                drop_pct, DROP_THRESHOLD, current_price
            )

        # ③ Price anomaly alert — triggered if price deviates more than 2 std devs from mean
        if volatility > 0:
            zscore = abs(current_price - avg_price) / volatility
            if zscore > ZSCORE_THRESHOLD:
                write_to_snowflake(
                    symbol, "PRICE_ANOMALY",
                    zscore, ZSCORE_THRESHOLD, current_price
                )

def main():
    # Initialise Spark session with Kafka connector package
    spark = (
        SparkSession.builder.appName("EquityRiskStreaming")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
                .config("spark.sql.streaming.checkpointLocation", "/tmp/spark_checkpoint")
                .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Read stream from Kafka topic
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "stock_prices")
        .option("startingOffsets", "latest")
        .load()
    )

    # Parse JSON messages and filter out nulls
    parsed = (
        raw_stream
        .select(from_json(col("value").cast("string"), message_schema).alias("data"))
        .select("data.symbol", "data.price", "data.timestamp")
        .filter(col("symbol").isNotNull())
    )

    # Start streaming query — process each micro-batch with foreachBatch
    query = (
        parsed.writeStream
        .foreachBatch(process_batch)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("Spark Streaming started — listening on Kafka topic: stock_prices")
    print("Processing every 10s, alerts written to Snowflake ALERTS.risk_alerts")
    print("Press Ctrl+C to stop\n")

    query.awaitTermination()

if __name__ == "__main__":
    main()
