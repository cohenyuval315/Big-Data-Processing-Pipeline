from .kafka_service import KafkaService
import logging
from pyspark.sql.types import StringType
from pyspark.sql.functions import column
from structures.kline import klineSchema
from pyspark.sql.functions import get_json_object,from_json

class KafkaSource(KafkaService):
    def __init__(self) -> None:
        self.boostrap_servers = "localhost:9092"
        self.default_topic = "raw_data_test"

    def read(self,spark,topics:list=["raw_data"]):
        df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", ",".join(topics)) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss","false") \
        .load()
        #".option(key.deserializer,)"
        #".option(value.deserializer,)"
        #".option(enable.auto.commit,)"
        #".option(max.poll.records,)"
        #".option(auto.offset.reset,)"

        kafka_df = df.withColumn("value", df["value"].cast(StringType()))
        parsed_df = kafka_df.withColumn("kline_data", from_json(column("value"), klineSchema))
        ready_kafka_df = parsed_df.selectExpr(
            "CAST(timestamp AS TIMESTAMP)",
            "CAST(timestampType AS INTEGER)",
            "CAST(key AS STRING)",
            "CAST(topic AS STRING)", 
            "CAST(offset AS INTEGER)", 
            "CAST(partition AS INTEGER)",
            "kline_data",
        )
        return ready_kafka_df
    
    def debug(self,spark,topics:list=["raw_data"]):
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", ",".join(topics)) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss","false") \
            .load()
        df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
            .writeStream \
            .format("console") \
            .outputMode("append") \
            .start()
        return df
