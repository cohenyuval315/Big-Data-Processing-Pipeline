DROP TABLE IF EXISTS app_data.predictions;

CREATE TABLE IF NOT EXISTS app_data.predictions (
    timestamp BIGINT,
    key VARCHAR, 
    topic VARCHAR, 
    offset BIGINT,
    partition BIGINT,
    klineStartTime BIGINT,
    klineCloseTime BIGINT,
    symbol VARCHAR,
    interval VARCHAR,
    firstTradeID BIGINT,
    lastTradeID BIGINT,
    openPrice DOUBLE,
    closePrice DOUBLE,
    highPrice DOUBLE,
    lowPrice DOUBLE,
    baseAssetVolume DOUBLE,
    numOfTrades INT,
    klineClosed BOOLEAN,
    quoteAssetVolume DOUBLE,
    takerBuyBaseAssetVolume DOUBLE,
    takerBuyQuoteAssetVolume DOUBLE,
    ignore VARCHAR,
    eventTime BIGINT,
    eventType VARCHAR,
    PRIMARY KEY (timestamp,klineStartTime, symbol)
);

TRUNCATE TABLE IF EXISTS app_data.predictions;