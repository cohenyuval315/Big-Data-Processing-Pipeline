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

    async def history_data(self, request):
        candlesticks = 
        return web.Response()
        

    async def new_data(self, request):
        username = request.match_info['username']
