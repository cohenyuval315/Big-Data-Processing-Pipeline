CREATE KEYSPACE IF NOT EXISTS app_data
WITH REPLICATION = {
     'class' : 'SimpleStrategy', 'replication_factor' : 1 };
