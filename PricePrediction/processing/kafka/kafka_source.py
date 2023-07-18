from spark_stream import getSparkSession
from .kafka_service import KafkaService
import logging

class KafkaSource(KafkaService):
    def __init__(self) -> None:
        self.boostrap_servers = "localhost:9092"
        self.default_topic = "my_topic"
        self.spark = getSparkSession()

    def read(self,topics=None,key=None):
        t = topics if topics else self.default_topic
        df = self.spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "my_topic") \
        .option("startingOffsets", "earliest") \
        .load()

