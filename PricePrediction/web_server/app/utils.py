import os
from hashlib import md5
import pytz
import yaml
from aiohttp import web
from dateutil.parser import parse
from db import CassandraDB
from config import Config


def load_config(fname):
    with open(fname, 'rt') as f:
        data = yaml.safe_load(f)
    # TODO: add config validation
    return data


def init_cassandra(conf:Config):
    cs = CassandraDB(addresses=conf.CASSANDRA_HOSTS,port=conf.CASSANDRA_PORT)
    cs.create_keyspace_if_not_exists()
    cs.create_table_if_not_exists(truncate=conf.TRUNCATE_TABLE,drop=conf.DROP_TABLE)
    return cs

