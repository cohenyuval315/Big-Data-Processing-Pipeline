from confluent_kafka import Consumer,KafkaException
import json 
import asyncio

class MyConsumer():
    def __init__(self,topic,bootstrap_servers = "localhost:9092") -> None:
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.group_id = 1
        self.consumer = Consumer({
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': "1",
            'enable.auto.commit': False,
            #'auto.offset.reset': 'earliest'
        })
        self.consumer.subscribe([self.topic])
    
    # def init_consumer(self):
    #     consumer_config = {
    #         'bootstrap.servers': self.bootstrap_servers,
    #         'group.id': self.group_id,
    #         'auto.offset.reset': 'earliest',
    #         'key.deserializer': str.decode,
    #         'socket.keepalive.enable': True,
    #         'value.deserializer': lambda x: json.loads(x.decode('utf-8'))
    #     }
    #     consumer = Consumer(consumer_config)
    #     consumer.subscribe([self.topic])
    #     return consumer

    async def handle_kafka_messages(self):
        try:
            while True:
                message = self.consumer.poll(1.0)
                if message is None:
                    await asyncio.sleep(1)
                    print("none")
                    continue
                if message.error():
                    if message.error().code() == KafkaException._PARTITION_EOF:
                        # Reached the end of a partition, continue polling
                        continue
                    else:
                        print(f"Consumer error: {message.error()}")
                        continue
                print("CONSUMER message from Kafka:", message.value().decode('utf-8'))

                # # Manually commit the offsets to control the position
                self.consumer.commit(message)
        except Exception as e:
            print(e)
        finally:
            self.consumer.close()

    async def process_kafka_message(self,message):
        print("Received message from Kafka:", message)