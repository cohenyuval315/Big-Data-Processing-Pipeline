from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.types import *
from pyspark.sql.types import LongType,StringType,DoubleType,IntegerType,BooleanType
from pyspark.sql.functions import *
from pyspark.sql.streaming import *
from kafka.kafka_source import KafkaSource
from cass import CassandraSink
from pyspark.ml.regression import RandomForestRegressionModel


def main():
    from spark_builder import spark    
    cass_sink_one_second = CassandraSink() 
    cass_sink_one_minute = CassandraSink() 
    ks = KafkaSource()
    aggregated_df = ks.read(spark)
    aggregated_df.printSchema()

    aggregated_df.writeStream \
        .format("console") \
        .start()\
        
    
    one_second_df = aggregated_df.filter((aggregated_df.topic == "btcusdt_1s")) #"topic = 'btcusdt_1s'"
    one_minute_df = aggregated_df.filter((aggregated_df.topic == "btcusdt_1m"))
    
    cass_sink_one_second.write_stream(df=one_second_df,table="one_second",keyspace="btcusdt")
    cass_sink_one_minute.write_stream(df=one_minute_df,table="one_minute",keyspace="btcusdt")

    spark.streams.awaitAnyTermination()







if __name__ == "__main__":
    main()