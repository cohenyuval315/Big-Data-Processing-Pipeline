import os

class Config:
    CASSANDRA_HOSTS = ["127.0.0.1"]
    CASSANDRA_PORT = 9042
    WEB_HOST = "127.0.0.1"
    WEB_PORT = 9001
    TRUNCATE_TABLE = False
    DROP_TABLE= False    

class DevConfig(Config):
    TRUNCATE_TABLE = False
    DROP_TABLE= False

class TestConfig(Config):
    TRUNCATE_TABLE = True
    DROP_TABLE=True


app_config = DevConfig()