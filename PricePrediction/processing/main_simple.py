from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.streaming import *
from pyspark.sql import functions as F
from pyspark.sql import Window
from kafka.kafka_source import KafkaSource
from cass import CassandraSink

def main():
    from spark_builder import spark  
    expr = [
        "timestamp",
        "topic",
        "symbol",
        "interval",
        "CAST (klineStartTime AS TIMESTAMP)",
        "CAST (klineCloseTime AS TIMESTAMP)",
        "CAST (eventTime AS TIMESTAMP)",
        "openPrice",
        "closePrice",
        "highPrice",
        "lowPrice",
        "numOfTrades",
        "klineClosed",
    ]
    ks = KafkaSource()
    cass_db = CassandraSink(checkpoint_location="checkpoints/prices/")
    kafka_df:StreamingQuery = ks.read(spark,["closed_candles","raw_data"])
    full_df = kafka_df.select("timestamp","topic","kline_data.*")
    df_expr = full_df.selectExpr(*expr)
    df1 = df_expr.dropna(subset="klineStartTime")
    columns_to_rename = {
        "openPrice": "open", 
        "highPrice": "high", 
        "lowPrice": "low",
        "klineStartTime":"opentimestamp",
        "klineCloseTime":"closetimestamp",
        "eventTime":"eventtime",
        "closePrice":"close"
        }

    for old_name, new_name in columns_to_rename.items():
        df1 = df1.withColumnRenamed(old_name, new_name)

    parsed_df = df1.select("timestamp","opentimestamp","closetimestamp","eventtime","symbol","open","close","high","low")
    
    window_spec = window(parsed_df["timestamp"], "10 minutes","5 minutes")

    rolling_avg_df = parsed_df\
        .withWatermark("timestamp", "10 minutes")\
        .groupBy(window_spec,"closetimestamp","symbol").agg(
         col("symbol"),
         col("window.start").alias("window_start"),
         col("window.end").alias("window_end"),
         (min(col("closetimestamp"))).alias("closetimestart"),
         (max(col("closetimestamp"))).alias("closetimeend"),
         (avg(col("close"))).alias("moving_avg"),
         (min(col("close"))).alias("min_close"),
         (max(col("close"))).alias("max_close"),
         (count(col("close"))).alias("count_closes"),
    ).select("symbol","window_start", "window_end", "moving_avg","min_close","max_close","count_closes","closetimestart","closetimeend")

    closed_prices_df = parsed_df\
        .withWatermark("closetimestamp", "10 minutes")\
        .withColumnRenamed("close", "closeprice")\
        .select(
            col("symbol"),
            col("closeprice"),
            col("closetimestamp").alias("timestamp")
        )
    rolling_df = rolling_avg_df\
        .withWatermark("closetimeend", "10 minutes")\
        .select(
            col("symbol"),
            col("moving_avg"),
            col("closetimeend").alias("timestamp")
        )
    cass_db.write_stream(closed_prices_df,"app_data","closed_prices",processingTime="10 seconds",mode="append",batchmode="append",checkpoint="checkpoints/closed_prices")
    cass_db.write_stream(rolling_df,"app_data","moving_avg",processingTime="5 seconds",mode="update",batchmode="append",checkpoint="checkpoints/rolling_avg")

    
    


    spark.streams.awaitAnyTermination()
if __name__ == "__main__":
    main()