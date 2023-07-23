import asyncio
import logging
import pathlib
from aiohttp import web
from routes import setup_routes
# from utils import init_cassandra, load_config
from views import SiteHandler
import os


PROJ_ROOT = pathlib.Path(__file__).parent.parent

# async def setup_cassandra(app, conf, loop):
#     cassandra = await init_cassandra(conf)
#     return cassandra




async def init(loop):
    # conf = load_config(PROJ_ROOT / 'config.yml')
    app = web.Application()
    # cassandra = await setup_cassandra(app, conf, loop)
    handler = SiteHandler()
    setup_routes(app, handler, PROJ_ROOT)
    host = os.environ.get("WEB_HOST")
    port = os.environ.get("WEB_PORT")
    db_host = os.environ.get("DB_HOSTS")
    db_ports = os.environ.get("DB_PORT")
    print(db_host,db_ports)
    # host, port = conf['host'], conf['port']
    return app, host, port


async def get_app():
    import aiohttp_debugtoolbar
    app, _, _ = await init(asyncio.get_event_loop())
    aiohttp_debugtoolbar.setup(app)
    return app


def main():
    logging.basicConfig(level=logging.DEBUG)
    loop = asyncio.get_event_loop()
    app, host, port = loop.run_until_complete(init(loop))
    web.run_app(app, host=host, port=port)


if __name__ == '__main__':
    main()