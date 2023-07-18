from .kafka_service import KafkaService
import logging
from models.kline import klineSchema
from pyspark.sql.functions import get_json_object

class KafkaSource(KafkaService):
    def __init__(self) -> None:
        self.boostrap_servers = "localhost:9092"
        self.default_topic = "my_topic"

    def read(self,spark):
        df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "btcusdt_1s,btcusdt_1m") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss","false") \
        .load()
        ".option(key.deserializer,)"
        ".option(value.deserializer,)"
        ".option(enable.auto.commit,)"
        ".option(max.poll.records,)"
        ".option(auto.offset.reset,)"

        kafka_df = df.selectExpr(
            "CAST(key AS STRING)", "CAST(value AS STRING)",
            "CAST(topic AS STRING)", "CAST(offset AS STRING)", "CAST(partition AS INTEGER)", "timestamp"
        )
        aggregated_df = kafka_df.select(
            "timestamp","key", "topic", "offset", "partition", 
            *[get_json_object("value", f"$.{col_name}").alias(col_name.lower()) for col_name in klineSchema.names]
        )
        return aggregated_df

