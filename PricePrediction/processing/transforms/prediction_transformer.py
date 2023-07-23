from pyspark.ml import Transformer
from pyspark.sql import functions as F

class PredictionTransformer(Transformer):
    def __init__(self,inputCol,prefix=None) -> None:
        self.inputCol = inputCol
        self.prefix = prefix  if prefix else ""
    def check_input_type(self, schema):
        return
    
    def _transform(self, df):
        self.check_input_type(df.schema)
        self.transform(df)

    def transform(self,df):
        return df