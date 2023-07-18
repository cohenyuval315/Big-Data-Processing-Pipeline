import os
from hashlib import md5
import pytz
import yaml
from aiohttp import web
from dateutil.parser import parse
from db import CassandraService


def load_config(fname):
    with open(fname, 'rt') as f:
        data = yaml.safe_load(f)
    # TODO: add config validation
    return data


async def init_cassandra(conf):
    cassandra_config = conf['cassandra'] 
    hosts = cassandra_config['hosts']
    port = cassandra_config['port']
    keyspaces = cassandra_config['keyspaces']
    tables = cassandra_config['tables']
    cs = CassandraService(addresses=hosts,port=port)
    for keyspace in keyspaces:
        cs.create_keyspace_if_not_exists(keyspace)
        for table in tables:
            cs.create_table_if_not_exists(keyspace,table)
    return cs

