from cassandra.cluster import Cluster

class CassandraDB:
    def __init__(self,addresses,port) -> None:
        self.cluster = Cluster(
            addresses,
            port=port
        )
        self.contact_points = addresses
        self.port = port
        self.keyspace = "app_data"
        self.table = "predictions"
        self.timestamp_column = "timestamp"

    def get_cluster(self):
        cluster = Cluster(
            contact_points = self.contact_points,
            port=self.port,
            protocol_version=65
        )
        return cluster

    def get_session(self,keyspace=None):
        if keyspace:
            session = self.cluster.connect(keyspace)
        else:
            session = self.cluster.connect()
        return session

    def create_keyspace_if_not_exists(self):
        schema = f"""
            CREATE KEYSPACE IF NOT EXISTS {self.keyspace}
            WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }}
        """
        session = self.get_session()
        session.execute(schema)
        session.shutdown()


    def create_table_if_not_exists(self,truncate=False,drop=False):
        pass




    def get_all_data(self,limit=100):
        query = "SELECT eventtime,klinestarttime,klineclosetime,closeprice FROM real_time_data where klineclosed = True ALLOW FILTERING"
        session = self.get_session(self.keyspace)
        # prepared_statement = session.prepare(query)
        # bound_statement = prepared_statement.bind([limit])
        rows = session.execute(query)
        session.shutdown()
        if not rows:
            return []
        rows = [row for row in rows]
        return rows

    def get_all_predictions(self,limit=100):
        query = "SELECT klinestarttime,klineclosetime,groundtruthcloseprice,predictioncloseprice FROM predictions;"
        session = self.get_session(self.keyspace)
        # prepared_statement = session.prepare(query)
        # bound_statement = prepared_statement.bind([limit])
        rows = session.execute(query)

        session.shutdown()
        if not rows:
            return []
        rows = [row for row in rows]
        return rows
    
    def get_data(self, last_klinestarttime):
        query = "SELECT klinestarttime, klineclosetime, closeprice FROM real_time_data WHERE klinestarttime > ? ALLOW FILTERING ORDER BY token(klineStartTime) DESC;"
        session = self.get_session(self.keyspace)
        # prepared_statement = session.prepare(query)
        # bound_statement = prepared_statement.bind([last_klinestarttime])
        rows = session.execute(query)
        rows = [row for row in rows if row[0] > last_klinestarttime]
        session.shutdown()
        return rows
    
    def get_predications(self,last_klinestarttime):
        query = f"SELECT klinestarttime,klineclosetime,groundtruthcloseprice,predictioncloseprice FROM predictions WHERE klinestarttime > ? ALLOW FILTERING  ORDER BY token(klineStartTime) DESC;"
        session = self.get_session(self.keyspace)
        # prepared_statement = session.prepare(query)
        # bound_statement = prepared_statement.bind([last_klinestarttime])
        rows = session.execute(query)
        rows = [row for row in rows if row[0] > last_klinestarttime]
        session.shutdown()
        return rows


    


    
    