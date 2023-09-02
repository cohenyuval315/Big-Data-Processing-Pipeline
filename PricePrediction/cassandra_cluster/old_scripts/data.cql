DROP TABLE IF EXISTS app_data.predictions;

CREATE TABLE IF NOT EXISTS app_data.predictions (
    timestamp BIGINT,
    interval VARCHAR,
    klineCloseTime BIGINT,
    closePrice DOUBLE,
    PRIMARY KEY (timestamp)
);

TRUNCATE TABLE IF EXISTS app_data.predictions;