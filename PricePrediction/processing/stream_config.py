from pyspark import SparkConf
import os
os.environ['HADOOP_HOME'] = 'D:\hadoop'
os.environ['"spark.local.dir"'] = 'D:\spark\temp'


class Config:
    app_name = "MySparkApplication"

class KafkaConfig:
    kafka_topic = "my_topic"
    kafka_boostrap_servers = "localhost:9092"

class DevConfig(Config):
    pass


spark_config = SparkConf()
spark_config.set("spark.local.dir", r"D:\spark\temp")

kafka_config = KafkaConfig()

app_config = DevConfig()

