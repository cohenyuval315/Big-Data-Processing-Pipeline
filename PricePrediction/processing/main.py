from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.types import *
from pyspark.sql.types import LongType,StringType,DoubleType,IntegerType,BooleanType
from pyspark.sql.functions import *
from pyspark.sql.streaming import *
import dataclasses
import time
import findspark
import pyspark
import os 
import datetime
import pandas as pd
from models.kline import klineSchema,KlineData
from cass import CassandraSink
findspark.init()

os.environ['HADOOP_HOME'] = r'D:\hadoop\winutils'

hadoop_home = r'D:\hadoop\winutils'
bin_path = os.path.join(hadoop_home, 'bin')
path = os.environ.get('PATH', '')
new_path = f'{bin_path};{path}'
os.environ['PATH'] = new_path
os.environ['SCALA_HOME'] =  r'D:\Program Files (x86)\scala'
os.environ['JAVA_HOME'] = r'C:\Program Files\Microsoft\jdk-11.0.16.101-hotspot'
os.environ['SPARK_HOME'] = r'D:\spark\spark-3.4.1-bin-hadoop3'
os.environ['spark.local.dir'] = r'D:\spark\temp'

apache_spark = "org.apache.spark"
apache_cassandra = "org.apache.cassandra"

CASSANDRA_VERSION = '3.11.5'
SCALA_VERSION = '2.12'
SPARK_VERISON = '3.4.1'
CONNECTOR = '3.3.0'

spark_lib = f'{apache_spark}:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERISON}'
cassandra = f'{apache_cassandra}:apache-cassandra:{CASSANDRA_VERSION}'
spark_cassandra_connector =f'com.datastax.spark:spark-cassandra-connector_{SCALA_VERSION}:3.3.0'
packages = f'--packages {spark_lib},{cassandra},{spark_cassandra_connector} pyspark-shell'
os.environ['PYSPARK_SUBMIT_ARGS'] = packages


def main():
    spark = SparkSession.builder \
        .appName("app") \
        .config('spark.local.dir',r'D:\spark\temp') \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "false") \
        .config("spark.sql.shuffle.partitions", "3") \
        .config("spark.cassandra.connection.host", "localhost") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
        .getOrCreate()
    
    cass_sink = CassandraSink() 
    
    # sampleDataframe = KafkaSource().read()
    
    sampleDataframe = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "btcusdt_1s,btcusdt_1m") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss","false") \
    .load()


    #base_df = sampleDataframe.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)","timestamp")
    base_df = sampleDataframe.selectExpr(
        "CAST(key AS STRING)", "CAST(value AS STRING)",
        "CAST(topic AS STRING)", "CAST(offset AS STRING)", "CAST(partition AS INTEGER)", "timestamp"
    )


    base_df.printSchema()

    #extracted_df = base_df.select("key", *[get_json_object("value", f"$.{col_name}").alias(col_name) for col_name in klineSchema.names])
    extracted_df = base_df.select(
        "timestamp","key", "topic", "offset", "partition", 
        *[get_json_object("value", f"$.{col_name}").alias(col_name.lower()) for col_name in klineSchema.names]
    )
    # query = extracted_df.writeStream \
    #     .format("console") \
    #     .option("checkpointLocation", "./checkpoints") \
    #     .start()
    
    one_second_df = extracted_df.filter((extracted_df.topic == "btcusdt_1s")) #"topic = 'btcusdt_1s'"
    extracted_df.cop
    one_minute_df = extracted_df.filter("topic = 'btcusdt_1m'")
    
    cass_sink.write_stream(df=one_second_df,table="one_second",keyspace="btcusdt")
    cass_sink.write_stream(df=one_minute_df,table="one_minute",keyspace="btcusdt")



    # query.awaitTermination()
    # new_line_cassandra.awaitTermination()

    # query = base_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    #     .writeStream \
    #     .format("console") \
    #     .option("checkpointLocation", "./checkpoints") \
    #     .start()
    
    # json_df = base_df.select("key", from_json("value", klineSchema).alias("data"))


    # data = query.selectExpr()



    # query.awaitTermination()

    # # query = base_df.writeStream \
    # #     .format("console") \
    # #     .start()

    # # query.awaitTermination()
    # console_query = base_df.writeStream \
    #     .format("console") \
    #     .start()

    # # Wait for the query to finish
    # console_query.awaitTermination()



if __name__ == "__main__":
    main()