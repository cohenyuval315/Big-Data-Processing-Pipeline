CREATE TABLE IF NOT EXISTS moving_avg (
    symbol TEXT,
    timestamp TIMESTAMP,
    moving_avg DOUBLE,
    PRIMARY KEY (symbol, timestamp)
);