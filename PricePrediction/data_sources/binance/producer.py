from confluent_kafka import Producer
import json
import time

class MyProducer():
    def __init__(self,bootstrap_servers="localhost:9092",config=None) -> None:
        self.bootstrap_servers = bootstrap_servers
        cfg = config if config else {
            'bootstrap.servers': self.bootstrap_servers ,
            # 'broker.address.family': 'v4'
            }
        self.producer = establish_kafka_connection(cfg)

    def produce(self,data,topic,key=None):
        try:
            if self.producer:
                self.producer.produce(topic,key=key, value=data)
            else:
                return False
        except Exception as e:
            print(e)
            return False
        return True

    async def flush(self):
        if self.producer:
            self.producer.flush()

    def create_topic(self,topic):
        pass

def establish_kafka_connection(cfg):
    retries = 5
    delay = 1  # Initial delay in seconds
    max_delay = 16  # Maximum delay in seconds
    
    while retries > 0:
        try:
            kafka_producer = Producer(cfg)
            return kafka_producer
        except Exception as e:
            print(f"Connection attempt failed: {e}")
            retries -= 1
            time.sleep(delay)
            delay = min(delay * 2, max_delay)  # Exponential backoff

    print("Unable to establish Kafka connection")
    return None