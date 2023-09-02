CREATE TABLE IF NOT EXISTS closed_prices (
    symbol TEXT,
    timestamp TIMESTAMP,
    closeprice DOUBLE,
    PRIMARY KEY (symbol, timestamp)
);