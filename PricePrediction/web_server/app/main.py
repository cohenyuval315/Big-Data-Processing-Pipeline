import asyncio
import logging
import pathlib
from aiohttp import web
from routes import setup_routes
from utils import init_cassandra
from views import SiteHandler
from config import app_config


PROJ_ROOT = pathlib.Path(__file__).parent.parent
# conf = load_config(PROJ_ROOT / 'config.yml')

async def init(loop):
    app = web.Application()
    cassandra = init_cassandra(conf=app_config)
    handler = SiteHandler(cassandra)
    app['db'] = cassandra
    setup_routes(app, handler, PROJ_ROOT)
    return app, app_config.WEB_HOST, app_config.WEB_PORT


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