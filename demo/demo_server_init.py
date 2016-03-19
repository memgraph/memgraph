#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
The demo server init script. Environment could be configured
via the MEMGRAPH_DEMO environtment variable. Available environments
are: debug, prod.
'''

import logging

from util import get_env
from simulation.web_server import SimulationWebServer

environment = get_env('MEMGRAPH_DEMO', 'debug')
wsgi = get_env('MEMGRAPH_DEMO_WSGI', 'werkzeug')


def _init():
    '''
    Initialzies logging level and server.
    '''
    if environment == 'prod':
        logging.basicConfig(level=logging.WARNING)
    elif environment == 'test':
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(level=logging.DEBUG)

    return SimulationWebServer().server


app = _init()


if __name__ == '__main__':
    if wsgi == 'gevent':
        from gevent.wsgi import WSGIServer
        http_server = WSGIServer(('', 8080), app)
        http_server.serve_forever()
    else:
        app.run(host="0.0.0.0", port=8080)
