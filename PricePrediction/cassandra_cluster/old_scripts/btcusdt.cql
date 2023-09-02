CREATE KEYSPACE IF NOT EXISTS btcusdt
WITH REPLICATION = {
     'class' : 'SimpleStrategy', 'replication_factor' : 1 };
