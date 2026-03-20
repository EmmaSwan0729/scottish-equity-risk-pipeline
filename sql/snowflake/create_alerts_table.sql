USE DATABASE EQUITY_DB;
USE SCHEMA ALERTS;
CREATE TABLE IF NOT EXISTS risk_alerts (
    alert_if VARCHAR(36) DEFAULT UUID_STRING(),
    symbol VARCHAR(10),
    alert_type VARCHAR(50),
    metric_value FLOAT,
    threshold_value FLOAT,
    price_at_alert FLOAT,
    triggered_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);