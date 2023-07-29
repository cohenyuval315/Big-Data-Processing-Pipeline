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

    async def history_data(self,request):
        data = request.app['db'].get_all_data()
        if not data:
            return web.json_response(status=404)
        return web.json_response(data=data)
    
    async def history_predictions(self,request):
        data = request.app['db'].get_all_predictions()
        if not data:
            return web.json_response(status=404)
        return web.json_response(data=data)
    
    async def data(self, request):
        last_timestamp = request.query.get('last_timestamp')
        data = request.app['db'].get_data(int(last_timestamp))
        if not data:
            return web.json_response(status=404)
        return web.json_response(data=data)
    
    async def predictions(self,request):
        last_timestamp = request.query.get('last_timestamp')
        data = request.app['db'].get_predications(int(last_timestamp))
        if not data:
            return web.json_response(status=404)
        return web.json_response(data=data)


