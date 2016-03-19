# -*- coding: utf-8 -*-

import logging
import threading
from flask import request, jsonify

from .. import WebService
from . import wrapped_client

log = logging.getLogger(__name__)


class WorkerWebServer(WebService):
    '''
    Memgraph worker web server. For now it wraps the flask server.
    '''

    def __init__(self):
        '''
        Instantiates the flask web server.
        '''
        super().__init__()
        self.params_data = None
        self.is_simulation_running = False
        self.stats_data = None
        self.setup_routes()

    def setup_routes(self):
        '''
        Setup all routes.
        '''
        super().__init__()
        self.add_route('/start', self.start, 'POST')
        self.add_route('/stop', self.stop, 'POST')
        self.add_route('/stats', self.stats, 'GET')
        self.add_route('/params', self.params_get, 'GET')
        self.add_route('/params', self.params_set, 'POST')

    def run_simulation(self):
        '''
        If flag is_simulation_running flag is up (True) the executor
        epoch will be executed. Epochs will be executed until somebody
        set is_simulation_running flag to Flase.
        '''
        log.info('new simulation run')

        while self.is_simulation_running:
            self.stats_data = wrapped_client(*self.params_data)

    def start(self):
        '''
        '''
        self.is_simulation_running = True
        t = threading.Thread(target=self.run_simulation, daemon=True)
        t.start()
        return ('', 204)

    def stop(self):
        '''
        '''
        self.is_simulation_running = False
        return ('', 204)

    def stats(self):
        '''
        Returns the simulation stats. Queries per second.
        '''
        if not self.stats_data:
            return ('', 204)

        return jsonify(self.stats_data)

    def params_get(self):
        '''
        Returns simulation parameters.
        '''
        return jsonify(self.simulation_params.json_data())

    def params_set(self):
        '''
        Sets simulation parameters.
        '''
        data = request.get_json()

        param_names = ['host', 'port', 'connections', 'duration', 'queries']

        for param in param_names:
            if param in data:
                setattr(self.params_data, param, data[param])

        return self.params_get()
