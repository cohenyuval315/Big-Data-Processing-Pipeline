import pandas as pd
import dataclasses
from .kline_data import KlineData

@dataclasses.dataclass
class KlineAggregationKafka:
    topic:str 
    partition: int
    offset: int
    timestamp: pd.Timestamp
    klineOutput: KlineData