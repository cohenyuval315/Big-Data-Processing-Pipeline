
import os
from binance import ThreadedWebsocketManager,BinanceSocketManager,AsyncClient
import json
import time
from kafka import MyProducer
import asyncio
import pandas as pd
from confluent_kafka.admin import AdminClient, NewTopic
from utils import load_config

async def normalize_kline(msg):
    m = msg['data'].copy()
    normal_msg = m['k'].copy()
    del m['k']
    del m['s']
    normal_msg['E'] = m['E']
    normal_msg['e'] = m['e'] 
    return normal_msg

async def normalize_kline_json(json):
    # df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    data = {
    #   "klineStartTimeDate": str(pd.to_datetime(json["t"], unit='ms')),
    #   "klineCloseTimeDate": str(pd.to_datetime(json["T"], unit='ms')),
    #   "eventTimeDate": str(pd.to_datetime(json["E"], unit='ms')),
      "eventType": str(json['e']),
      "eventTime": int(json["E"]),
      "symbol": str(json["s"]),
      "klineStartTime": int(json["t"]),
      "klineCloseTime": int(json["T"]),
      "interval": str(json["i"]), 
      "firstTradeID": int(json["f"]), 
      "lastTradeID": int(json["L"]), 
      "openPrice": float(json["o"]), 
      "closePrice": float(json["c"]), 
      "highPrice": float(json["h"]), 
      "lowPrice": float(json["l"]), 
      "baseAssetVolume": float(json["v"]), 
      "numOfTrades": int(json["n"]), 
      "klineClosed": bool(json["x"]),
      "quoteAssetVolume": float(json["q"]), 
      "takerBuyBaseAssetVolume": float(json["V"]), 
      "takerBuyQuoteAssetVolume": float(json["Q"]), 
      "ignore": str(json["B"]),
    }
    return data

def get_topic_kline_stream(msg):
    stream_name = msg['stream']
    data = msg['data']
    kline_data = data['k']
    interval = kline_data['i']
    symbol = kline_data['s']
    symbol = symbol.lower()
    topic = f"{symbol}_{interval}"
    return topic

def create_kafka_topic(topic_name):
    admin_client = AdminClient({
        "bootstrap.servers": "localhost:9092"
    })
    topic_list = []
    topic_list.append(NewTopic(topic_name, 1, 1))
    admin_client.create_topics(topic_list)

def get_kline_key(msg):
    return None


async def handle_binance_socket_message(idx,msg,**kwargs):
    params = kwargs['kwargs']
    to_print = params['to_print']
    producer = params['producer']
    key = params['key']
    topic = params['topic']
    delay = params['delay']
    sanity_check = params['sanity_check']
    if not producer:
        return
    msg_topic = topic if topic else get_topic_kline_stream(msg)
    msg_key = key if key else get_kline_key(key)
    msg = await normalize_kline(msg)
    msg = await normalize_kline_json(msg)
    if to_print:
        print(msg)
    p = producer.produce(json.dumps(msg),key=msg_key,topic=msg_topic)
    if sanity_check:
        print(f"{idx} recv msg; producer kafka: {p}")
    await asyncio.sleep(delay)



async def run_stream(bsm,streams,**kwargs):
    ms = bsm.multiplex_socket(streams=streams)
    idx = 0
    if ms is False:
        return False
    async with ms as mscm:
        while True:
            res = await mscm.recv()
            asyncio.create_task(handle_binance_socket_message(idx,res,**kwargs))
            idx += 1
            
    

async def binance_socket_stream_data_source(config,producer=None):
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
    create_kafka_topic(DEFAULT_TOPIC)
    USER_TIMEOUT = bool(binance_api['to_timeout'])
    user_timeout = None
    if USER_TIMEOUT is True:
        user_timeout = int(binance_api['timeout'])
    #create_kafka_topic(DEFAULT_TOPIC)
    TO_PRINT= bool(binance_api['print'])
    SANITY_CHECK= bool(binance_api['sanity_check'])
    
    TO_KAFKA = binance_api['to_kafka']
    BINANCE_SLEEP_DELAY = int(binance_api['sleep_delay'])
    KEY = None
    async_client = await AsyncClient.create(api_key=API_KEY,api_secret=SECRET_KEY)
    bsm = BinanceSocketManager(async_client,user_timeout=user_timeout)
    my_producer = producer if TO_KAFKA is True else None

    start_time = time.time()
    tries = 0
    while True:
        try:
            print(f"starting binance socket streams:{STREAMS}")
            check = await run_stream(bsm=bsm,streams=STREAMS,kwargs={
                "to_print":TO_PRINT,
                "producer":my_producer,
                "key":KEY,
                "topic":DEFAULT_TOPIC,
                "delay":BINANCE_SLEEP_DELAY,
                "sanity_check":SANITY_CHECK,
            })
            if check is False:
                if tries == 5:
                    print("FAIL TO CONNECT")
                    break
                tries += 1
                print(f"fail to connect to binance client trying agian : {tries} ...")
                

        except KeyboardInterrupt as e:
            end_time = time.time()
            run_time = end_time - start_time
            print(f"\nKeyboard Stop: binance_api_runtime:{run_time}\nexception:{e}\n")
            break

        except Exception as e:
            end_time = time.time()
            run_time = end_time - start_time
            print(f"\nbinance_api_runtime:{run_time}\nexception:{e}\n")
            if STOP_ON_EXCEPTION:
                break
    await async_client.close_connection()


async def run_sources(config,producer=None):
    await asyncio.gather(*[
        binance_socket_stream_data_source(config,producer)
                           ])


async def run():
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
    await run_sources(data_sources_config,producer)

def main():
    asyncio.run(run())


if __name__ == "__main__":
    main()
    