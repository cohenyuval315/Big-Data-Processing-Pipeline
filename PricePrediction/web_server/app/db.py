from cassandra.cluster import Cluster
from cassandra import connection

class CassandraService:
    def __init__(self,addresses,port) -> None:
        self.cluster = Cluster(
            addresses,
            port=port
        )
        self.port = port

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
 
    def create_table_if_not_exists(self,keyspace,table):
        drop_table = f"DROP TABLE IF EXISTS {keyspace}.{table}"
        table_schema = f"""
            CREATE TABLE IF NOT EXISTS {keyspace}.{table} (
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
                PRIMARY KEY (timestamp,klineStartTime, symbol)
            )
        """
        session = self.get_session()
        session.execute(drop_table)
        session.execute(table_schema)
        session.shutdown()




    


    
    