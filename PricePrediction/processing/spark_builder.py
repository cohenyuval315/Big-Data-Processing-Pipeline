from pyspark.sql import SparkSession
from pyspark import SparkConf,SparkContext
import findspark
import os
from utils import read_yaml,read_config

findspark.init()

spark_master_url = "spark://127.0.0.1:7078"

CONFIG_PATH = "./config.yaml"
apache_spark = "org.apache.spark"
apache_cassandra = "org.apache.cassandra"
CASSANDRA_VERSION = '3.11.5'
SCALA_VERSION = '2.12'
SPARK_VERISON = '3.4.1'
CONNECTOR = '3.3.0'
scala_home = r'D:\Program Files (x86)\scala'
hadoop_home = r'D:\hadoop\winutils'
java_home = r'C:\Program Files\Microsoft\jdk-11.0.16.101-hotspot'
spark_home = r'D:\spark\spark-3.4.1-bin-hadoop3'
spark_local_dir = r'D:\spark\temp'
spark_lib = f'{apache_spark}:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERISON}'
cassandra = f'{apache_cassandra}:apache-cassandra:{CASSANDRA_VERSION}'
spark_cassandra_connector =f'com.datastax.spark:spark-cassandra-connector_{SCALA_VERSION}:3.3.0'


packages = f'--packages {spark_lib},{cassandra},{spark_cassandra_connector} pyspark-shell'

bin_path = os.path.join(hadoop_home, 'bin')
path = os.environ.get('PATH', '')
new_path = f'{bin_path};{path}'


os.environ['HADOOP_HOME'] = hadoop_home
os.environ['PATH'] = new_path
os.environ['SCALA_HOME'] =  scala_home
os.environ['JAVA_HOME'] = java_home
os.environ['SPARK_HOME'] = spark_home
os.environ['spark.local.dir'] = spark_local_dir
os.environ['PYSPARK_SUBMIT_ARGS'] = packages

conf = SparkConf()
configurations = read_config(CONFIG_PATH,"sparkConfig")
conf.setAll(configurations)

spark = SparkSession.builder \
    .master(spark_master_url)\
    .appName("app") \
    .config(conf=conf)\
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")


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
    

