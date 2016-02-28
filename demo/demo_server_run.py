#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import logging

from simulation.web_server import SimulationWebServer


def main():
    '''
    The frontend run script. Environment could be configured
    via the MEMGRAPH_DEMO environtment variable. Available environments
    are: debug, prod.
    '''
    if 'MEMGRAPH_DEMO' in os.environ:
        environment = os.environ['MEMGRAPH_DEMO']
    else:
        environment = "debug"

    frontend_server = SimulationWebServer()

    if environment == 'prod':
        logging.basicConfig(level=logging.WARNING)
        frontend_server.run("0.0.0.0", 8080, False)
    elif environment == 'debug':
        logging.basicConfig(level=logging.INFO)
        frontend_server.run("0.0.0.0", 8080, True)

if __name__ == '__main__':
    main()
