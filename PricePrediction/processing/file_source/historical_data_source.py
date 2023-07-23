import logging
from structures.kline import historicalKlineSchema 
from pyspark.sql.functions import get_json_object
import os
class HistoricalDataSource():
    def __init__(self,base_dir,filename=None) -> None:
        self.base_dir = base_dir
        self.data_path = base_dir if filename is None else os.path.join(base_dir,filename)
        self.file_type = "csv"

    def read(self,spark):
        df = spark \
        .readStream \
        .schema(historicalKlineSchema) \
        .option("maxFilesPerTrigger",1)\
        .option("checkpointLocation", "checkpoints/historical/") \
        .option("spark.sql.streaming.forceDeleteTempCheckpointLocation",True)\
        .format(self.file_type) \
        .load(self.base_dir)
        return df

