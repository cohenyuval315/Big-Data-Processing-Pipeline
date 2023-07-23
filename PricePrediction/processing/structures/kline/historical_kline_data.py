import dataclasses

@dataclasses.dataclass
class HistoricalKlineData:
    eventType:str
    eventTime:int
    symbol:str
    klineStartTime:int
    klineCloseTime:int
    interval:str
    firstTradeID:int
    lastTradeID:int
    openPrice:float
    closePrice:float
    highPrice:float
    lowPrice:float
    baseAssetVolume:float
    numOfTrades:int
    klineClosed:bool
    quoteAssetVolume:float
    takerBuyBaseAssetVolume:float
    takerBuyQuoteAssetVolume:float
    ignore:str
