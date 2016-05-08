# -*- coding: utf-8 -*-

import logging
import threading
from os import path
from flask import request, jsonify

from web_service import WebService
# from .client import wrapped_client
from .client import subprocess_client

log = logging.getLogger(__name__)


class WorkerWebService(WebService):
    '''
    Memgraph worker web server. For now it wraps the flask server.
    '''

    def __init__(self):
        '''
        Instantiates the flask web server.
        '''
        super().__init__(__name__)
        self.params_data = {}
        self.is_simulation_running = False
        self.stats_data = None
        self.setup_routes()
        self.counter = None

    def setup_routes(self):
        '''
        Setup all routes.
        '''
        super().setup_routes()
        self.add_route('/', self.index, 'GET')
        self.add_route('/<path:path>', self.static, 'GET')
        self.add_route('/start', self.start, 'POST')
        self.add_route('/stop', self.stop, 'POST')
        self.add_route('/stats', self.stats, 'GET')
        self.add_route('/params', self.params_get, 'GET')
        self.add_route('/params', self.params_set, 'POST')

    def index(self):
        '''
        Serves demo.html on the index path.
        '''
        print('index')
        return self.server.send_static_file('demo.html')

    def static(self, path):
        '''
        Serves other static files.
        '''
        return self.server.send_static_file(path)

    def run_simulation(self):
        '''
        If flag is_simulation_running flag is up (True) the executor
        epoch will be executed. Epochs will be executed until somebody
        set is_simulation_running flag to Flase.
        '''
        log.info('new simulation run')

        while self.is_simulation_running:
            #   cython call TODO relase the GIL
            # params = [
            #     self.params_data['host'].encode('utf-8'),
            #     self.params_data['port'].encode('utf-8'),
            #     self.params_data['connections'],
            #     self.params_data['duration'],
            #     list(map(lambda x: x.encode('utf-8'),
            #              self.params_data['queries']))
            # ]
            # data = wrapped_client(*params)

            #   subprocess call
            if not self.counter:
                self.counter = 0
            params = [
                str(self.params_data['host']),
                str(self.params_data['port']),
                str(self.params_data['connections']),
                str(self.params_data['duration']),
                str(self.counter)
            ] + list(map(lambda x: str(x), self.params_data['queries']))
            exe = path.join(path.dirname(path.abspath(__file__)),
                            "benchmark_json.out")
            self.stats_data = subprocess_client([exe] + params)
            self.counter = self.stats_data['counter']

    def start(self):
        '''
        Starts run in a separate thread.
        '''
        self.counter = 0
        self.is_simulation_running = True
        t = threading.Thread(target=self.run_simulation, daemon=True)
        t.start()
        return ('', 204)

    def stop(self):
        '''
        Stops the worker run.
        '''
        self.params_data = {}
        self.stats_data = None
        self.is_simulation_running = False
        return ('', 204)

    def stats(self):
        '''
        Returns the worker stats. Queries per second.
        '''
        if not self.stats_data:
            return ('', 204)

        return jsonify(self.stats_data)

    def params_get(self):
        '''
        Returns worker parameters.
        '''
        return jsonify(self.params_data)

    def params_set(self):
        '''
        Sets worker parameters.
        '''
        data = request.get_json()

        param_names = ['host', 'port', 'connections', 'duration', 'queries']

        for param in param_names:
            if param in data:
                self.params_data[param] = data[param]

        return self.params_get()
