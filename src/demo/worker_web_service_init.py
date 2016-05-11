# -*- coding: utf-8 -*-

import logging

from config import config
from worker.worker_web_service import WorkerWebService


def _init():
    '''
    Defines log level.
    '''
    logging.basicConfig(level=config.log_level)

    return WorkerWebService().server

app = _init()

if __name__ == '__main__':
    app.run(host=config.host, port=config.port)
