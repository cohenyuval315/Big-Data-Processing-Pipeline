from cassandra.cluster import Cluster

class Database:
    pass

class CassandraConnection(Database):
    def __init__(self,addresses = ["cassandra"],port=9042) -> None:
        super().__init__()
        self.addresses = addresses
        self.port = port
        self.cluster = None
        
    def init(self):
        self.cluster = Cluster(
            self.addresses,
            port=self.port
        )
    def _get_session(self,keyspace):
        if self.cluster:
            if keyspace:
                session = self.cluster.connect(keyspace)    
            else:
                session = self.cluster.connect()
            return session
        else:
            raise Exception()
        
    def _request(self,query,keyspace):
        session = self._get_session(keyspace)
        res = session.execute(query)
        session.shutdown()
        return res
    

class CassandraDB:

    KEYSPACE = "app_data"
    TABLE_MOVING_AVG = "moving_avg"
    TABLE_CLOSE_PRICES = "closed_prices"

    def __init__(self,db:CassandraConnection) -> None:
        self.db = db

    def get_close_prices(self):
        query = f"""
            SELECT symbol,toDate(timestamp),closeprice
            FROM {self.TABLE_CLOSE_PRICES};
        """
        res = self.db._request(query,self.KEYSPACE)
        data = []
        for item in res:
            data.append(item)
        return data


    def get_moving_avg(self):
        query = f"""
            SELECT symbol,toDate(timestamp),moving_avg
            FROM {self.TABLE_MOVING_AVG};
        """
        res = self.db._request(query,self.KEYSPACE)
        data = []
        for item in res:
            data.append(item)
        return data

    
def init_db(config):
    cc = CassandraConnection()
    cc.init()
    db = CassandraDB(cc)
    return db