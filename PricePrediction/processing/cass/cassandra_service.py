from cassandra.cluster import Cluster

class CassandraService:
    def __init__(self,addresses = ["127.0.0.1"],port=9042, keyspace:str=None) -> None:
        self.cluster = Cluster(
            addresses,
            port=port
        )
        self.port = port
        self.keyspace = keyspace
        if self.keyspace:
            self.create_keyspace_if_not_exists(self.keyspace)

    def get_session(self,keyspace=None):
        if keyspace:
            session = self.cluster.connect(keyspace)
        else:
            session = self.cluster.connect()
        return session


    def create_keyspace_if_not_exists(self,keyspace):
        schema = f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspace}
            WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }}
        """
        session = self.get_session()
        session.execute(schema)
        session.shutdown()

    def create_predictions_table(self,table_name,drop=False,truncate=False):
        if not self.keyspace:
            return
        table_schema = f"""
            CREATE TABLE IF NOT EXISTS {self.keyspace}.{table_name} (
                opentimestamp BIGINT,
                closetimestamp BIGINT,
                symbol VARCHAR,
                interval VARCHAR,
                closeprice DOUBLE,
                predictedcloseprice DOUBLE,
                timestamp BIGINT,
                PRIMARY KEY (timestamp,opentimestamp)
            ) WITH CLUSTERING ORDER BY (opentimestamp DESC);
        """
        self._create_table_if_not_exists(table_name,table_schema,drop,truncate)

    def create_kafkameta_table(self):
        pass

    def create_real_time_data(self,table_name,drop=False,truncate=False):
        if not self.keyspace:
            return
        table_schema = f"""
            CREATE TABLE IF NOT EXISTS {self.keyspace}.{table_name} (
                timestamp BIGINT,
                key VARCHAR, 
                topic VARCHAR, 
                offset BIGINT,
                partition BIGINT,
                klineStartTime BIGINT,
                klineCloseTime BIGINT,
                symbol VARCHAR,
                interval VARCHAR,
                firstTradeID BIGINT,
                lastTradeID BIGINT,
                openPrice DOUBLE,
                closePrice DOUBLE,
                highPrice DOUBLE,
                lowPrice DOUBLE,
                baseAssetVolume DOUBLE,
                numOfTrades INT,
                klineClosed BOOLEAN,
                quoteAssetVolume DOUBLE,
                takerBuyBaseAssetVolume DOUBLE,
                takerBuyQuoteAssetVolume DOUBLE,
                ignore VARCHAR,
                eventTime BIGINT,
                eventType VARCHAR,
                PRIMARY KEY (timestamp,eventtime)
            ) WITH CLUSTERING ORDER BY (eventtime DESC);
        """
        self._create_table_if_not_exists(table_name,table_schema,drop,truncate)



    def _create_table_if_not_exists(self,table,table_schema,drop=False,truncate=False):
        session = self.get_session()

        if drop:
            drop_table = f"DROP TABLE IF EXISTS {self.keyspace}.{table}"
            session.execute(drop_table)

        # table_schema = f"""
        #     CREATE TABLE IF NOT EXISTS {self.keyspace}.{table} (
        #         timestamp BIGINT,
        #         key VARCHAR, 
        #         topic VARCHAR, 
        #         offset BIGINT,
        #         partition BIGINT,
        #         klineStartTime BIGINT,
        #         klineCloseTime BIGINT,
        #         symbol VARCHAR,
        #         interval VARCHAR,
        #         firstTradeID BIGINT,
        #         lastTradeID BIGINT,
        #         openPrice DOUBLE,
        #         closePrice DOUBLE,
        #         highPrice DOUBLE,
        #         lowPrice DOUBLE,
        #         baseAssetVolume DOUBLE,
        #         numOfTrades INT,
        #         klineClosed BOOLEAN,
        #         quoteAssetVolume DOUBLE,
        #         takerBuyBaseAssetVolume DOUBLE,
        #         takerBuyQuoteAssetVolume DOUBLE,
        #         ignore VARCHAR,
        #         eventTime BIGINT,
        #         eventType VARCHAR,
        #         PRIMARY KEY (timestamp,klineStartTime, symbol)
        #     )
        # """
        session.execute(table_schema)

        if truncate:
            drop_table = f"TRUNCATE {self.keyspace}.{table}"
            session.execute(drop_table)

        session.shutdown()




    


    
    