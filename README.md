# Price-Prediction-Pipeline


## main flow:
1. 
- from api to kafka: real time data , history data
2. 
- from kafka to hadoop: history data 
- from kafka to hadoop: real time data 
3. 
- from kafka to kafka: transformation to data(currently transformations in spark)
4. 
- from kafka to cassandra: transformed real time data 
- from kafka to spark: real time data : processing predictions.[x]
- from hadoop to spark : history data
- from spark to kafka: predictions
5. 
- from kafka to cassnadra: predictions 
6. 
- from cassandra to web server by request 
- from web server to web client: visulizing data
7. periodic:
    1. 
    - from cassandra to hadoop: old real time data 
    - from cassnadra to delete: old real time data
    - from cassandra to hadoop: old predictions data
    - from cassnadra to delete: old predictions data
    - from cassnadra to hadoop: aggregation data analytics
    - from cassnadra to delete: aggregation data analytics
    2. 
    - from spark to hadoop: ml processing historical data
    - from spark to hadoop: historical aggregation data analytics


# General:
- docker-compose []
- organizing clusters configurations []
- k8 []

# Data:
- open-low-high-close candlesticks


# Data Source:
- using binance python lib: depth 2 order book
- using binance python lib: all binance btcusdt history, files/kafka[x]
- using binance python lib: real time streaming binance btcusdt data 1m intervals[x]

# data ingestion
- history/real time data of btcusdt to kafka[x]

# Data etl:
- kafka streams scala , transform data to kafka[] 
- real time data to hadoop []
- history data to hadoop []

# data retention:
- scala - remove real time from cassandra every interval[]
- scala - remove real time from cassandra every interval[]
- scala - aggregated data to hadoop every interval[]

# spark structred processing
- pyspark processing[x]
- machine learning example sklearn  SGBRegressor model[x]
- randomForestRegressor(1d) + lstm(1m)
- testing models[]
- decoupling cassandra []
- predictions back to kafka instead of cassandra []
- docker []
- ...


# cassandraDB
- real time data to cassandra[x]
- predictions data to cassandra[x]
- scripts[]
- docker[x]

# hadoop(warehouse and datalake)
- aggregated data []
- btcusdt history data []
- predictions data []
- docker []


# web server:
- server aiohttp [x]
- endpoints [x]
- connection to cassandra[x]
- docker []

# web application:
- react with charts []
- docker []


