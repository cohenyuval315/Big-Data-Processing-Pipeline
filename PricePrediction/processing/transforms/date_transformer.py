from pyspark.ml import Transformer
from pyspark.sql.types import TimestampType
from pyspark.sql import functions as F

class DateTransformer(Transformer):
    DATE = "date"
    WEEK_DAY = "week_day"
    MONTH_DAY = "month_day"
    YEAR_DAY = "year_day"
    MONTH = "month"
    YEAR = "year"
    HOUR = "hour"
    MINUTE = "minute"
    SECOND = "second"
    COLUMNS = [
        # DATE,
        # WEEK_DAY,
        # MONTH_DAY,
        # YEAR_DAY,
        MONTH,
        YEAR,
        HOUR,
        MINUTE,
        SECOND
    ]
    def __init__(self,inputCol,prefix=None) -> None:
        self.inputCol = inputCol
        self.prefix = prefix + "_" if prefix else ""
        self.date = self.prefix + self.DATE
        self.week_day = self.prefix + self.WEEK_DAY
        self.month_day = self.prefix + self.MONTH_DAY
        self.year_day = self.prefix + self.YEAR_DAY
        self.month = self.prefix + self.MONTH
        self.year = self.prefix + self.YEAR
        self.hour = self.prefix + self.HOUR
        self.minute = self.prefix + self.MINUTE
        self.second = self.prefix + self.SECOND
        self.columns = [
            # self.date,
            # self.week_day,
            # self.month_day,
            # self.year_day,
            self.month,
            self.year,
            self.hour,
            self.minute, 
            self.second,
        ]
    def get_columns(self):
        return self.columns

    def check_input_type(self, schema):
        field = schema[self.inputCol]
        if (field.dataType != TimestampType()):
            raise Exception('Input type %s did not match input type TimestampType' % field.dataType)     
        
    def _transform(self, df):
        self.check_input_type(df.schema)
        return self.transform(df)
        
    def transform(self,df):
        new_df = df.withColumns({
                # self.date:F.to_date(df[self.inputCol],"yyyy-MM-dd HH:mm:ss.SSSS"),
                # self.week_day:F.dayofweek(df[self.inputCol]),
                # self.month_day:F.dayofmonth(df[self.inputCol]),
                # self.year_day:F.dayofyear(df[self.inputCol]),
                self.month:F.month(df[self.inputCol]),
                self.year:F.year(df[self.inputCol]),
                self.hour:F.hour(df[self.inputCol]),
                self.minute:F.minute(df[self.inputCol]),
                self.second:F.second(df[self.inputCol]),
        })
        return new_df


    

    

    
