
-- Scottish Equity Risk Pipeline — Snowflake Setup
-- Run this script once when setting up a new Snowflake account.
-- Execute each section in order.

-- SECTION_1: Warehouse, Database, Schemas

USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS EQUITY_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

CREATE DATABASE IF NOT EXISTS EQUITY_DB;

USE WAREHOUSE EQUITY_WH;
USE DATABASE EQUITY_DB;

CREATE SCHEMA IF NOT EXISTS EQUITY_DB.RAW;
CREATE SCHEMA IF NOT EXISTS EQUITY_DB.STAGING;
CREATE SCHEMA IF NOT EXISTS EQUITY_DB.MARTS;
CREATE SCHEMA IF NOT EXISTS EQUITY_DB.ALERTS;


-- SECTION_2: Raw Tables
CREATE OR REPLACE TABLE EQUITY_DB.RAW.raw_stock_prices (
    date         DATE,
    open         FLOAT,
    high         FLOAT,
    low          FLOAT,
    close        FLOAT,
    volume       BIGINT,
    symbol       VARCHAR(20),
    ingested_at  TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS EQUITY_DB.ALERTS.risk_alerts (
    alert_id        VARCHAR(36),
    symbol          VARCHAR(10),
    alert_type      VARCHAR(50),
    metric_value    FLOAT,
    threshold_value FLOAT,
    price_at_alert  FLOAT,
    triggered_at    TIMESTAMP_NTZ
);


-- SECTION_3: S3 Storage Integration
-- Note: After running DESC INTEGRATION below, copy
-- STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID,
-- then update the IAM Role trust policy in AWS Console.
-- See README for detailed instructions.
USE SCHEMA EQUITY_DB.RAW;

CREATE STORAGE INTEGRATION IF NOT EXISTS S3_EQUITY_INTEGRATION
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::526860034407:role/snowflake-equity-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://scottish-equity-risk-pipeline-526860034407-eu-west-2-an/raw/stock_prices/');

DESC INTEGRATION S3_EQUITY_INTEGRATION;

-- SECTION 4: File Format and Stage
-- Run this after updating the IAM Role trust policy.
CREATE OR REPLACE FILE FORMAT EQUITY_DB.RAW.parquet_format
    TYPE = 'PARQUET'
    SNAPPY_COMPRESSION = TRUE;

CREATE OR REPLACE STAGE EQUITY_DB.RAW.raw_stock_stage
    URL = 's3://scottish-equity-risk-pipeline-526860034407-eu-west-2-an/raw/stock_prices/'
    STORAGE_INTEGRATION = S3_EQUITY_INTEGRATION
    FILE_FORMAT = parquet_format;

-- Verify S3 connection (should list parquet files)
LIST @EQUITY_DB.RAW.raw_stock_stage;