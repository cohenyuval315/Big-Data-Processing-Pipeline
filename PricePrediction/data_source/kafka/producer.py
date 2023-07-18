from confluent_kafka import Producer
import json

class MyProducer():
    def __init__(self,bootstrap_servers="localhost:9092") -> None:
        self.bootstrap_servers = bootstrap_servers
        self.producer = Producer({'bootstrap.servers': self.bootstrap_servers })

    def produce(self,data,topic,key=None):
        try:
            self.producer.produce(topic,key=key, value=data)
        except Exception as e:
            print(e)
        return True

    async def flush(self):
        self.producer.flush()
