from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.types import *
from pyspark.sql.types import LongType,StringType,DoubleType,IntegerType,BooleanType
from pyspark.sql.functions import *
from pyspark.sql.streaming import *
from kafka.kafka_source import KafkaSource
from cass import CassandraSink
from pyspark.ml import Transformer,Pipeline
from pyspark.ml.feature import VectorAssembler, OneHotEncoder,MinMaxScaler
from pyspark.sql import Window
from pyspark.ml import Pipeline
from pyspark.sql.types import *
from pyspark.sql import types as T
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.ml import Transformer
from pyspark.ml.regression import LinearRegression
from pyspark.ml.param.shared import HasInputCol, HasOutputCol
from transforms.date_transformer import TimeStampTransformer
#df.withColumn("row_id", monotonically_increasing_id())
from keras import optimizers
from file_source.historical_data_source import HistoricalDataSource
from keras.models import Sequential
from keras.layers import Dense, LSTM, Dropout, GRU
from pyspark.ml.evaluation import RegressionEvaluator
import pandas as pd
import pyspark.pandas as ps

def main():
    from spark_builder import spark    
    # cass_sink_one_second = CassandraSink() 
    # cass_sink_one_minute = CassandraSink() 
    # ks = KafkaSource()
    # kafka_df = ks.read(spark) read time data
    # kafka_df.printSchema()
    target_column_name = "1m_close" 
    feature_columns = [
        "1m_open",
        "1m_high",
        "1m_low",
        "1m_close",
        "1m_quote_asset_volume",
        "1m_number_of_trades",
        "1m_taker_buy_base_asset_volume",
        "1m_taker_buy_quote_asset_volume",
        "1m_volume",
        "1m_open_time_day",
        "1m_open_time_month",
        "1m_open_time_year",
        "1m_open_time_hour",
        "1m_open_time_minute",
        "1m_close_time_day",
        "1m_close_time_month",
        "1m_close_time_year",
        "1m_close_time_hour",
        "1m_close_time_minute"]
    
    vector_assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

    hds = HistoricalDataSource('../data/raw/BTCUSDT/1m/')
    historical_data_df = hds.read(spark)
    windowed_df = historical_data_df \
        .withWatermark("spark_timestamp", "1 minute") \
        .groupBy(window("spark_timestamp", "1 minute").alias("minute_window")) \
        .agg(F.avg("close").alias("avg_close_in_"))


    assembled_data = vector_assembler.transform(historical_data_df)
    train_data, test_data = assembled_data.randomSplit([0.8, 0.2], seed=1234)
    lr = LinearRegression(featuresCol="features", labelCol=target_column_name)
    lr_model = lr.fit(train_data)
    predictions = lr_model.transform(test_data)
    evaluator = RegressionEvaluator(labelCol=target_column_name, predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    print("Root Mean Squared Error (RMSE):", rmse)

    daily_aggregates = historical_data_df.groupBy(F.date_trunc('day', F.col('timestamp')).alias('date')) \
        .agg(F.avg('close').alias('daily_avg_close'))

    
    # session_window = session_window(events.timestamp, \
    # F.when(events.userId == "user1", "5 seconds") \
    # .when(events.userId == "user2", "20 seconds").otherwise("5 minutes"))


    #LinearRegression()

    historical_data_df.writeStream \
        .format("console") \
        .start()\

    spark.streams.awaitAnyTermination()




if __name__ == "__main__":
    main()