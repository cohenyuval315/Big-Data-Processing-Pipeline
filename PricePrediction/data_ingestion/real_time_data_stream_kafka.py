
import os
from binance import ThreadedWebsocketManager,BinanceSocketManager,AsyncClient
import json
import time
from kafka import MyProducer
import asyncio

from confluent_kafka.admin import AdminClient, NewTopic
from utils import load_config





def get_topic_kline_stream(msg):
    stream_name = msg['stream']
    data = msg['data']
    kline_data = data['k']
    interval = kline_data['i']
    symbol = kline_data['s']
    symbol = symbol.lower()
    topic = f"{symbol}_{interval}"
    return topic

    
def get_kline_key(msg):
    return None


def handle_binance_socket_message(msg,to_print=False,producer=None,key=None,topic=None,delay=0.1):
    if to_print:
        print(msg)
    if not producer:
        return
    msg_topic = topic if topic else get_topic_kline_stream(msg)
    msg_key = key if key else get_kline_key(key)
    producer.produce(json.dumps(msg),key=msg_key,topic=msg_topic)
    time.sleep(delay)

def create_kafka_topic(topic_name):
    admin_client = AdminClient({
        "bootstrap.servers": "localhost:9092"
    })
    topic_list = []
    topic_list.append(NewTopic(topic_name, 1, 1))
    admin_client.create_topics(topic_list)

def binance_socket_stream_data_source(config,producer=None):
    binance_api = config['binance_api']
    binance_api = binance_api['real_time_stream']
    RUN = bool(binance_api['run'])
    if RUN is False:
        return
    
    API_KEY = os.environ.get('BINANCE_API_KEY')
    SECRET_KEY = os.environ.get('BINANCE_SECRET_KEY')

    STREAMS = binance_api['streams']
    STOP_ON_EXCEPTION = bool(binance_api['stop_on_exception'])
    DEFAULT_TOPIC = binance_api['default_topic']
    
    TO_PRINT= bool(binance_api['print'])
    TO_KAFKA = binance_api['to_kafka']
    BINANCE_SLEEP_DELAY = int(binance_api['sleep_delay'])

    KEY = None
    async_client = AsyncClient(api_key=API_KEY,api_secret=SECRET_KEY)
    bsm = BinanceSocketManager(async_client)
    twm = ThreadedWebsocketManager(API_KEY,SECRET_KEY)
    my_producer = producer if TO_KAFKA is True else None
    
    callback = lambda msg: handle_binance_socket_message(msg,TO_PRINT,my_producer,KEY,DEFAULT_TOPIC,BINANCE_SLEEP_DELAY)

    start_time = time.time()
    while True:
        try:
            print(f"starting binance socket streams:{STREAMS}")
            twm.start()
            twm.start_multiplex_socket(callback=callback, streams=STREAMS)
            twm.join()
        except Exception as e:
            end_time = time.time()
            run_time = end_time - start_time
            print(f"\nbinance_api_runtime:{run_time}\nexception:{e}\n")
            if STOP_ON_EXCEPTION:
                break


async def run_sources(config,producer=None):
    # await asyncio.gather(*[
    #     binance_socket_stream_data_source(config,producer)
    #                        ])
    binance_socket_stream_data_source(config,producer)

def main():
    CONFIG_FILE_PATH = 'data_ingestion_config.yaml'
    config = load_config(CONFIG_FILE_PATH)
    kafka_config = config['kafka']
    producer_config = kafka_config['producer_config']
    TO_KAFKA = bool(config['to_kafka'])
    BOOTSTRAP_SERVERS = producer_config['bootstrap_servers']
    data_sources_config = config['data_sources']
    producer = MyProducer(bootstrap_servers=BOOTSTRAP_SERVERS,config=None)
    if TO_KAFKA is False:
        producer = None
    # await run_sources(data_sources_config,producer)
    binance_socket_stream_data_source(data_sources_config,producer)


if __name__ == "__main__":
    main()