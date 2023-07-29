from pyspark.sql.types import LongType,StringType,DoubleType,IntegerType,BooleanType,StructType,StructField,DateType,TimestampType

klinePredictionSchema = StructType([\
    StructField("timestamp", TimestampType(),True),
    StructField("opentimestamp", TimestampType(),True),
    StructField("closetimestamp", TimestampType(),True),
    StructField("symbol", StringType(),True),
    StructField("interval", StringType(),True),
    StructField("closeprice", DoubleType(),True),
    StructField("predictedcloseprice", DoubleType(),True),
])
