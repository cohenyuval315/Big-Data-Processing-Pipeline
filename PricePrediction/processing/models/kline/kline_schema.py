from pyspark.sql.types import LongType,StringType,DoubleType,IntegerType,BooleanType,StructType,StructField

klineSchema = StructType([\
    StructField("klineStartTime", LongType(),True),
    StructField("klineCloseTime", LongType(),True),
    StructField("symbol", StringType(),True),
    StructField("interval", StringType(),True),
    StructField("firstTradeID", LongType(),True),
    StructField("lastTradeID", LongType(),True),
    StructField("openPrice", DoubleType(),True),
    StructField("closePrice", DoubleType(),True),
    StructField("highPrice", DoubleType(),True),
    StructField("lowPrice", DoubleType(),True),
    StructField("baseAssetVolume", DoubleType(),True),
    StructField("numOfTrades", IntegerType(),True),
    StructField("klineClosed", BooleanType(),True),
    StructField("quoteAssetVolume", DoubleType(),True),
    StructField("takerBuyBaseAssetVolume", DoubleType(),True),
    StructField("takerBuyQuoteAssetVolume", DoubleType(),True),
    StructField("ignore", StringType(),True),
    StructField("eventTime", LongType(),True),
    StructField("eventType", StringType(),True)
])