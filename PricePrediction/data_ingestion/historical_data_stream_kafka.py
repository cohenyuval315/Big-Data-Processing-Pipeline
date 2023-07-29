import os
from binance import Client
from datetime import timedelta,datetime
import pandas as pd
from kafka.producer import MyProducer
import json
import asyncio


class TimestampPartitioner(object):
    def __init__(self,seperator = '|') -> None:
        self.seperator = seperator
        
    def __call__(self, topic, key, partitions, *args, **kwargs):
        timestamp_key = key.decode('utf-8')
        timestamp = int(timestamp_key.split(self.seperator)[0])
        num_partitions = len(partitions)
        return timestamp % num_partitions
    
def create_csv_file(base_dir,symbol,interval,file_idx,data,override=False,file_prefix="RAW"): 
    file_name = f"{file_prefix}_{symbol}_{interval}_{file_idx}.csv"
    file_path = os.path.join(base_dir,symbol,interval,file_name)
    if not os.path.exists(file_path) or (override is True and os.path.exists(file_path)):
        df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'])
        # df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.to_csv(file_path, index=False)
        
def make_topic(symbol:str,interval:str,prefix:str=""):
    topic = f'{prefix}{symbol.lower()}_{interval.lower()}'
    return topic


async def data_to_csv(base_dir,data,symbol,interval,num_rows_per_file,overwrite,file_prefix):
    os.makedirs(os.path.join(base_dir), exist_ok=True)
    os.makedirs(os.path.join(base_dir,symbol), exist_ok=True)
    os.makedirs(os.path.join(base_dir,symbol,interval), exist_ok=True) 
    file_rows = []
    file_num = 0
    for idx,item in enumerate(data):
        file_rows.append(item)
        if idx != 0 and idx % num_rows_per_file == 0:
            create_csv_file(base_dir=base_dir,symbol=symbol,interval=interval,file_idx=file_num,data=file_rows,override=overwrite,file_prefix=file_prefix)
            file_rows = []
            file_num += 1
        await asyncio.sleep(0.1)

    if len(file_rows) > 0:
        create_csv_file(base_dir=base_dir,symbol=symbol,interval=interval,file_idx=file_num,data=file_rows,override=overwrite,file_prefix=file_prefix)

async def get_kline_timestamp(item):
    timestamp = item["E"]
    return timestamp

async def get_kline_timestamp_partioner_key(item,seperator,symbol,interval,idx):
    timestamp = await get_kline_timestamp(item)
    item_key = f"{timestamp}{seperator}{symbol}_{interval}_{idx}"
    key = item_key.encode('utf-8')
    return key

async def handle_msg(idx,producer,item,key,topic,delay):
    producer.produce(json.dumps(item),key=key,topic=topic)
    await asyncio.sleep(delay)

async def data_to_kafka(producer,to_print,topic, data,symbol,interval,prefix,seperator,delay):
    for idx,item in enumerate(data):
        if to_print:
            print(f'{idx}-{item}')        
        if not producer:
            continue
        data_key = await get_kline_timestamp_partioner_key(item,seperator,symbol,interval,idx) if seperator else None
        data_topic = topic if topic else make_topic(symbol,interval,prefix)
        await handle_msg(idx,producer,item,data_key,data_topic,delay)

def binance_historical_data(config):
    binance_api = config['binance_api']
    binance_api = binance_api['historical_data']
    RUN = bool(binance_api['run'])
    if RUN is False:
        return
    TO_KAFKA = bool(binance_api['to_kafka'])
    producer_config = {
        'bootstrap.servers': "localhost:9092",
    }
    USE_TIMESTAMP_PARTITIONER = bool(binance_api['timestamp_partitioner'])
    if USE_TIMESTAMP_PARTITIONER:
        TIMESTAMP_PARTITIONER_SEPERATOR = binance_api['timestamp_partitioner_seperator']
        custom_partitioner = TimestampPartitioner(TIMESTAMP_PARTITIONER_SEPERATOR)
        partitioner = {'partitioner': custom_partitioner}
        producer_config.update(partitioner)

    API_KEY = os.environ.get('BINANCE_API_KEY')
    SECRET_KEY = os.environ.get('BINANCE_SECRET_KEY')
    START_DATE = binance_api['start_date']
    END_DATE = datetime.today().strftime("%d %b %Y")
    SLEEP_DELAY = int(binance_api['sleep_delay'])
    TO_PRINT = bool(binance_api['print'])
    DEFAULT_TOPIC = binance_api['default_topic']
    print(END_DATE)
    symbols = binance_api['symbols']
    intervals = binance_api['intervals']
    TO_CSV = bool(binance_api['to_csv'])
    if TO_CSV:
        csv_config = binance_api['csv_config']
        BASE_DIR= csv_config['base_dir']
        OVERWRITE= bool(csv_config['overwrite'])
        MAX_BATCH_SIZE = int(csv_config['max_batch_size'])
        NUM_BATCHS_IN_FILE= int(csv_config['num_batches_in_file'])
        ROWS_PER_FILE = NUM_BATCHS_IN_FILE * MAX_BATCH_SIZE
        CSV_FILE_PREFIX = csv_config['csv_file_prefix']

    client = Client(api_key=API_KEY,api_secret=SECRET_KEY)
    producer = MyProducer(config=producer_config)
    for symbol in symbols:
        for interval in intervals:
            _interval = interval[0]
            data = client.get_historical_klines_generator(
                symbol=symbol,
                interval=_interval,
                start_str=START_DATE,
                end_str=END_DATE,
            )
            if TO_CSV:
                print("csv - interval: ", _interval)
                asyncio.create_task(data_to_csv(base_dir=BASE_DIR,data=data,symbol=symbol,interval=_interval,num_rows_per_file=ROWS_PER_FILE,overwrite=OVERWRITE,file_prefix=CSV_FILE_PREFIX))

            if TO_KAFKA:
                if USE_TIMESTAMP_PARTITIONER:
                    print("kafka - interval: ", _interval)
                    asyncio.create_task(data_to_kafka(producer=producer,data=data,symbol=symbol,interval=_interval,prefix="",seperator=TIMESTAMP_PARTITIONER_SEPERATOR,delay=SLEEP_DELAY,to_print=TO_PRINT,topic=DEFAULT_TOPIC))
