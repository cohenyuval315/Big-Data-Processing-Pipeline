from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from stream_config import app_config,spark_config

# spark = SparkSession.builder \
#     .appName("app") \
#     .config('spark.local.dir',r'D:\spark\temp') \
#     .config("spark.sql.adaptive.enabled", "false") \
#     .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "false") \
#     .config("spark.sql.shuffle.partitions", "2") \
#     .config("spark.cassandra.connection.host", "localhost") \
#     .config("spark.cassandra.connection.port", "9042") \
#     .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
#     .getOrCreate()
    

# def getSparkSession():
#     session =  SparkSession.builder.getOrCreate()
#     return session