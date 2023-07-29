from pyspark.ml import Transformer
from pyspark.sql import functions as F
import pandas_ta as ta
from pyspark.sql import Window

class TechnicalIndicatorsTransformer(Transformer):
    INDICATORS = [
        "RSI"
    ]
    def __init__(self,inputCol="timestamp",indicatorCol="close",outputColPrefix="",period = 14, watermark_duration = "10 seconds",trigger_interval = "5 seconds") -> None:
        self.period = period
        self.watermark_duration = watermark_duration
        self.trigger_interval = trigger_interval
        self.inputCol = inputCol
        self.indicatorCol = indicatorCol
        self.outputColPrefix =outputColPrefix
    
    def calculate_rsi(self,df,period):
        window_spec = Window.orderBy(self.inputCol).rowsBetween(-period + 1, Window.currentRow)
        
        # Calculate the moving average
        moving_avg = F.avg(self.indicatorCol).over(window_spec)

        return df.withColumn(f"{self.outputColPrefix}MovingAverage_{period}", moving_avg)

    def calculate_indicators(self,batch_df,period):
        batch = self.calculate_rsi(batch_df,period)
        return batch

    def _transform(self,df):
        batch = self.calculate_indicators(df,self.period)
        return batch
    
        
        