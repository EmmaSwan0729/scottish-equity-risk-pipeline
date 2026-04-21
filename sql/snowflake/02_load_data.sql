-- ============================================================
-- Scottish Equity Risk Pipeline — Initial Data Load
-- Run this script to load historical data from S3 into
-- Snowflake. Required after setting up a new account or
-- when manually reloading data.
-- Prerequisites: 01_setup.sql must be executed first.
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE EQUITY_WH;
USE DATABASE EQUITY_DB;
USE SCHEMA RAW;

COPY INTO raw_stock_prices (date, open, high, low, close, volume, symbol, ingested_at)
FROM (
    SELECT
        TO_DATE(TO_TIMESTAMP($1:date::BIGINT / 1000000000)),
        $1:open::FLOAT,
        $1:high::FLOAT,
        $1:low::FLOAT,
        $1:close::FLOAT,
        $1:volume::BIGINT,
        $1:symbol::VARCHAR(20),
        $1:ingested_at::TIMESTAMP_NTZ
    FROM @raw_stock_stage
)
FILE_FORMAT = (FORMAT_NAME = 'parquet_format')
PATTERN = '.*\.parquet';

-- Verify load
SELECT COUNT(*), MIN(date), MAX(date), COUNT(DISTINCT symbol)
FROM raw_stock_prices;