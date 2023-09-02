from cassandra.cluster import Cluster


def create():

    addresses = ["0.0.0.0"]
    port=9042
    keyspace = "app_data"
    table = "moving_avg"
    table2 = "closed_candles"
    truncate = False
    cluster = Cluster(
        addresses,
        port=port
    )
    keyspace_schema = f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }}
    """
    table_schema = f"""
    CREATE TABLE IF NOT EXISTS {keyspace}.{table} (
        symbol TEXT,
        timestamp TIMESTAMP,
        moving_avg DOUBLE,
        PRIMARY KEY (symbol, timestamp)
    );
    """
    table_2_schema = f"""
    CREATE TABLE IF NOT EXISTS {keyspace}.{table2} (
        symbol TEXT,
        timestamp TIMESTAMP,
        close DOUBLE,
        PRIMARY KEY (symbol, timestamp)
    );
    """
    session = cluster.connect()
    session.execute(keyspace_schema)
    session = cluster.connect(keyspace)
    session.execute(table_schema)
    session.execute(table_2_schema)
    if truncate:
        drop_table = f"TRUNCATE {keyspace}.{table}"
        session.execute(drop_table)
        drop_table = f"TRUNCATE {keyspace}.{table2}"
        session.execute(drop_table)



if __name__ == "__main__":
    create()




    


    
    