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
from pyspark.ml.regression import LinearRegression
from transforms.timestamp_transformer import TimestampTransformer
from transforms.date_transformer import DateTransformer
from transforms.technical_indicators import TechnicalIndicatorsTransformer
from file_source.historical_data_source import HistoricalDataSource
from pyspark.ml.evaluation import RegressionEvaluator
import pandas as pd
import pyspark.pandas as ps
from pyspark.ml import  Pipeline
from models.model import sgd_regressor
from models.utils import save_sklearn_model
import numpy as np

def predict(x,model_path):
    model = sgd_regressor(model_path)
    return model.predict(x)

def train(batch_df,batch_id,model_path,features_col,target_col):
    y = np.array(batch_df.select(target_col).collect())
    x = np.array(batch_df.select(features_col).collect())
    model = sgd_regressor(model_path)
    model.partial_fit(x,y)
    save_sklearn_model(model,model_path)
    print(predict(x,model_path))



def main():
    from spark_builder import spark    
    model_path = "./models/data/model/model.pkl"
    hds_1m = HistoricalDataSource('../data/_raw/BTCUSDT/1m/')
    # hds = HistoricalDataSource('../data/_raw/BTCUSDT/1d/')
    historical_data_df = hds_1m.read(spark)
    historical_data_df_no_nulls = historical_data_df.dropna(subset="timestamp")

    tt1 = TimestampTransformer("timestamp","open_datetime")
    tt2 = TimestampTransformer("close_time","close_datetime")
    dtt1 = DateTransformer("open_datetime","open")
    dtt2 = DateTransformer("close_datetime","close")

    # pipeline_stages = [tt1,tt2,dtt1,dtt2]
    # pipeline = Pipeline(stages=pipeline_stages)
    df1 = tt1._transform(historical_data_df_no_nulls)
    df2 = tt2._transform(df1)
    df3 = dtt1._transform(df2)
    df4 = dtt2._transform(df3)
    df5 = df4.withColumn("is_quarter_end", F.when((F.col("open_month") % 3 == 0), 1).otherwise(0))
    df6 = df5.withColumn("open-close", F.col("open") - F.col("close"))
    df7 = df6.withColumn("low-high", F.col("low") - F.col("high"))

    features_cols = ["open","low","high","is_quarter_end"]
    features_cols.extend(dtt1.get_columns())
    target_col = "close"
    df7.writeStream \
        .outputMode("update") \
        .trigger(processingTime="5 seconds") \
        .format("console") \
        .foreachBatch(lambda batch_df, batch_id:train(batch_df, batch_id,model_path,features_cols,target_col)) \
        .start()\

    # techTransformer = TechnicalIndicatorsTransformer()
    # df7.writeStream \
    #     .outputMode("update") \
    #     .trigger(processingTime="5 seconds") \
    #     .format("console") \
    #     .foreachBatch(lambda batch_df, batchId:techTransformer._transform(batch_df)) \
    #     .start()\


    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()