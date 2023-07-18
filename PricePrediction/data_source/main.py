
import os
from binance import ThreadedWebsocketManager,AsyncClient
from binance.enums import *
from binance.exceptions import BinanceAPIException,BinanceWebsocketUnableToConnect,BinanceRequestException,UnknownDateFormat
import json
from kafka import MyProducer

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
    streams = ['btcusdt@kline_1m', 'btcusdt@kline_1s']
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
        ready_msg = normalize_kline_json(normalize_kline(data))
        producer.produce(json.dumps(ready_msg),key=stream_name,topic=topic,verbose=True)
    try:
        twm.start()
        twm.start_multiplex_socket(callback=handle_socket_message, streams=streams)
        twm.join()
    except Exception as e:
        print(e)
        twm.stop()



if __name__ == "__main__":
    main()