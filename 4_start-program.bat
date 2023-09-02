@echo off
start "DataSource" cmd /k "D: & cd D:\Users\yuval\Desktop\PySparkBigData\PricePrediction\data_sources\binance\ & python async_real_time_data_kafka.py"
start "Processing" cmd /k "D: & cd D:\Users\yuval\Desktop\PySparkBigData\PricePrediction\processing\ & python main_simple.py"
