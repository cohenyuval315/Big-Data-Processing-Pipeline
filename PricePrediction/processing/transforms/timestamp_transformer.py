
from pyspark.ml import Transformer
from pyspark.sql import functions as F
import pandas as pd
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import udf

class TimestampTransformer(Transformer):
    def __init__(self,inputCol="timestamp",outputCol="date") -> None:
        self.inputCol = inputCol
        self.outputCol = outputCol

    def _transform(self,df):
        to_datetime_udf = udf(lambda ts: pd.to_datetime(ts, unit='ms'), TimestampType())
        new_df = df.withColumn(self.outputCol, to_datetime_udf(df[self.inputCol]))
        return new_df