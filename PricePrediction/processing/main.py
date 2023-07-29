from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.streaming import *
from kafka.kafka_source import KafkaSource
from cass import CassandraSink
from pyspark.ml.regression import RandomForestRegressor,GBTRegressor,GBTRegressionModel
from pyspark.ml.feature import VectorAssembler
from transforms.date_transformer import DateTransformer
from datetime import timedelta
from pyspark.sql import functions as F
import os
import numpy as np
from structures.kline.predication_kline_schema import klinePredictionSchema
from transforms.timestamp_transformer import TimestampTransformer
from models.model import sgd_regressor
from pyspark.sql.functions import udf
from pyspark.sql.functions import pandas_udf
import warnings
from pyspark.sql.functions import unix_timestamp
from pyspark.streaming import StreamingContext

from pyspark.sql.streaming import StreamingQueryListener

class CustomStreamingQueryListener(StreamingQueryListener):
    def on_query_started(self, query_info, query_details):
        # Add your logic for query start event here
        print("Query started with ID:", query_info.id)

    def on_query_progress(self, query_info, progress):
        # Add your logic for query progress event here
        print("Query in progress with ID:", query_info.id)

    def on_query_terminated(self, query_info):
        # Add your logic for query termination event here
        print("Query terminated with ID:", query_info.id)


def model_predict(model,vector_col):
    output = model.value.predict(vector_col)
    return output

def predict_and_write_cassandra(spark,batch_df,batch_id,model,feature_cols,input_features_col,target_col,metadata_cols,keyspace="app_data",table="predictions"):
    predict_udf = F.udf(lambda vector_col: model_predict(model, vector_col), DoubleType())
    batch_with_prediction_df = batch_df.withColumn("predictedcloseprice", predict_udf(F.col(input_features_col)))

    close_price_df = batch_with_prediction_df\
                        .where(col("klineClosed") == True)\
                        .groupBy("opentimestamp")\
                        .agg(avg("close").alias("closeprice"))

    pred_df_with_label = batch_with_prediction_df\
                            .join(close_price_df, "opentimestamp","left")

    pred_df = pred_df_with_label\
                .select("opentimestamp","closetimestamp","symbol","interval","predictedcloseprice","closeprice")\
                .where(col("klineClosed") == False)\
                .groupBy("opentimestamp","closetimestamp","symbol","interval","closeprice")\
                .agg(F.avg("predictedcloseprice").alias("predictedcloseprice"))

    ready_preds_df_1 = pred_df.withColumn('timestamp', unix_timestamp(F.current_timestamp()))
    ready_preds_df_2 = ready_preds_df_1.withColumn('opentimestamp', unix_timestamp('opentimestamp'))
    ready_preds_df = ready_preds_df_2.withColumn('closetimestamp', unix_timestamp('closetimestamp'))
    complete_df = ready_preds_df.dropna()
    complete_df.coalesce(1)
    complete_df.printSchema()
    complete_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table, keyspace=keyspace) \
        .mode("append") \
        .save()

def main():
    model_path = "./models/data/model/model.pkl"
    if not os.path.exists(model_path):
        print("MODEL DOES NOT EXISTS")
        return
    from spark_builder import spark  
    spark.sparkContext.setLogLevel("ERROR")
    # custom_listener = CustomStreamingQueryListener()
    # spark.streams.addListener(custom_listener)

    cass_sink_real_time = CassandraSink(keyspace="app_data") 
    cass_sink_real_time.create_real_time_data("real_time_data",drop=True,truncate=True)
    cass_sink_predictions = CassandraSink(keyspace="app_data")
    cass_sink_predictions.create_predictions_table("predictions",drop=True,truncate=True)
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
    kafka_df:StreamingQuery = ks.read(spark,"raw_data")
    full_df = kafka_df.select("timestamp","topic","kline_data.*")
    df_expr = full_df.selectExpr(*expr)
    df1 = df_expr.dropna(subset="klineStartTime")
    tt1 = TimestampTransformer("eventTime","eventTimeDate")
    tt2 = TimestampTransformer("klineStartTime","klineStartTimeDate")
    tt3 = TimestampTransformer("klineCloseTime","klineCloseTimeDate")
    event_time_dtt = DateTransformer("eventTimeDate","event")
    start_time_dtt = DateTransformer("klineStartTimeDate","open")
    close_time_dtt = DateTransformer("klineCloseTimeDate","close")
    df2 = tt1._transform(df1)
    df3 = tt2._transform(df2)
    df4 = tt3._transform(df3)
    df5 = event_time_dtt.transform(df4)
    df6 = start_time_dtt.transform(df5)
    df7 = close_time_dtt.transform(df6)
    df8 = df7.withColumn("is_quarter_end", F.when((F.col("open_month") % 3 == 0), 1).otherwise(0))

    
    columns_to_rename = {
        "openPrice": "open", 
        "highPrice": "high", 
        "lowPrice": "low",
        "klineStartTime":"opentimestamp",
        "klineCloseTime":"closetimestamp",
        "closePrice":"close"
        }

    for old_name, new_name in columns_to_rename.items():
        df8 = df8.withColumnRenamed(old_name, new_name)

    
        

    #df11.printSchema()
    feature_cols = ["open", "high", "low", "is_quarter_end"]
    metadata_cols = ["timestamp","symbol","interval"]



    #cass_sink_predictions.write_stream(prediction_df,keyspace="app_data",table="predictions")
    
    # real_time_data_df = kafka_df.select(
    #     "timestamp", "timestampType", "key", "topic", "offset", "partition","kline_data.*"
    # )
    # cass_sink_real_time.write_stream(real_time_data_df,keyspace="app_data",table="real_time_data")
    features_cols = ["open","low","high","is_quarter_end"]
    target_col = "close"
    input_features_col = "input_features"
    assembler = VectorAssembler(inputCols=features_cols, outputCol=input_features_col)

    df_with_features = assembler.transform(df8)
    reg = sgd_regressor(model_path)
    sc = spark.sparkContext
    model = sc.broadcast(reg)

    df_with_features.writeStream\
        .foreachBatch(lambda batch_df, batch_id: predict_and_write_cassandra(spark,batch_df, batch_id,model,feature_cols,input_features_col,target_col,metadata_cols)) \
        .trigger(processingTime="30 seconds")\
        .outputMode("update") \
        .start()

    spark.streams.awaitAnyTermination()




if __name__ == "__main__":

    warning_filter = ("ignore", "23/07/29 20:08:11 WARN KafkaDataConsumer: KafkaDataConsumer is not running in UninterruptibleThread. It may hang when KafkaDataConsumer's methods are interrupted because of KAFKA-1894")
    warnings.filters.append(warning_filter)

    main()