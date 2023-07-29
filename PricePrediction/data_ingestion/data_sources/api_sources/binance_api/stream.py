
import os
from binance import ThreadedWebsocketManager,AsyncClient
from binance.enums import *
from binance.exceptions import BinanceAPIException,BinanceWebsocketUnableToConnect,BinanceRequestException,UnknownDateFormat
import json
from confluent_kafka import Producer
import json

class MyProducer():
    def __init__(self,bootstrap_servers="localhost:9092",config=None) -> None:
        self.bootstrap_servers = bootstrap_servers
        cfg = config if config else {'bootstrap.servers': self.bootstrap_servers }
        self.producer = Producer(cfg)

    def produce(self,data,topic,key=None):
        try:
            self.producer.produce(topic,key=key, value=data)
        except Exception as e:
            print(e)
        return True

    async def flush(self):
        self.producer.flush()

def normalize_kline(msg):
    m = msg.copy()
    normal_msg = m['k'].copy()
    del m['k']
    del m['s']
    normal_msg['E'] = m['E']
    normal_msg['e'] = m['e'] 
    return normal_msg

def normalize_kline_json(json):
    data = {
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

def main():
    API_KEY = os.environ.get('BINANCE_API_KEY')
    SECRET_KEY = os.environ.get('BINANCE_SECRET_KEY')
    BOOTSRAP_SERVERS = "localhost:9092"
    streams = ['btcusdt@kline_1m']
    producer = MyProducer(BOOTSRAP_SERVERS)

    twm = ThreadedWebsocketManager(API_KEY,SECRET_KEY)
    def handle_socket_message(msg):
        stream_name = msg['stream']
        data = msg['data']
        kline_data = data['k']
        interval = kline_data['i']
        symbol = kline_data['s']
        symbol = symbol.lower()
        topic = f"{symbol}_{interval}"
        topic = f"raw_data_test"
        ready_msg = normalize_kline_json(normalize_kline(data))
        producer.produce(json.dumps(ready_msg),key=stream_name,topic=topic)
    try:
        twm.start()
        twm.start_multiplex_socket(callback=handle_socket_message, streams=streams)
        twm.join()
    except Exception as e:
        print(e)
        twm.stop()



if __name__ == "__main__":
    main()