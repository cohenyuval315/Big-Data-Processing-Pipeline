from aiohttp import web
from conn import init_db
import json
import os
import logging
access_log = logging.getLogger('aiohttp.access')
class Config():
    pass

config = None

async def moving_avg(request:web.Request):
    res = request.app['db'].get_moving_avg()
    data = [
        {
            "moving_avg": row.moving_avg,
            "symbol": row.symbol,
            "system_todate_timestamp": str(row.system_todate_timestamp)
        }
        for row in res
    ]
    return web.json_response(data=data)
    

async def close_prices(request):
    res = request.app['db'].get_close_prices()
    data = [
        {
            "closeprice": row.closeprice,
            "symbol": row.symbol,
            "system_todate_timestamp": str(row.system_todate_timestamp)
        }
        for row in res
    ]
    return web.json_response(data=data)


def get_app():
    app = web.Application()
    app.add_routes([web.get('/moving_avg', moving_avg),
                    web.get('/close_prices', close_prices)
                    ])
    
    app['db'] = init_db(config)
    return app


def main():
    app = get_app()
    port = os.environ.get("SERVER_PORT")
    if port is None:
        print("port is None")
        port = 7676
    else:
        port = int(port)

    host = os.environ.get("SERVER_HOST")
    if host is None:
        print("host is None")
        host = "0.0.0.0"
    web.run_app(app,host=host,port=port, access_log=access_log)


if __name__ == "__main__":
    main()
