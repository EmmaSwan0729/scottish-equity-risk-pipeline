USE DATABASE EQUITY_DB;
USE SCHEMA ALERTS;
CREATE TABLE risk_alerts (
    alert_id        VARCHAR(36),
    symbol          VARCHAR(10),
    alert_type      VARCHAR(50),
    metric_value    FLOAT,
    threshold_value FLOAT,
    price_at_alert  FLOAT,
    triggered_at    TIMESTAMP_NTZ
);