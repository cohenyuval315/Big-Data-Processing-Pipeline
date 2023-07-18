import datetime

import aiohttp_jinja2
from aiohttp import web
import os

class SiteHandler:

    def __init__(self, cassandra=None):
        self._cassandra = cassandra

    @property
    def cassandra(self):
        return self._cassandra

    async def data(self, request):
        # starttime = request.match_info['starttime']
        # endtime = request.match_info['endttime']
        # interval = request.match_info['interval']
        data= {
            "history":[]
        }
        return web.json_response(data=data)
        

    async def new_data(self, request):
        # timestamp = request.match_info['last_timestamp']
        data= {
            "new_data":[]
        }
        return web.json_response(data=data)


