DROP TABLE IF EXISTS app_data.predictions;

CREATE TABLE IF NOT EXISTS app_data.predictions (
    timestamp BIGINT,
    interval VARCHAR,
    closePrice DOUBLE,
    predictedClosePrice DOUBLE,

    PRIMARY KEY (timestamp)
);

TRUNCATE TABLE IF EXISTS app_data.predictions;