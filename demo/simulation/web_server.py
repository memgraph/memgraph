# -*- coding: utf-8 -*-

import logging
import threading
from flask import Flask, request, jsonify

from .executor import SimulationExecutor
from .params import SimulationParams
from .task import SimulationTask

log = logging.getLogger(__name__)


class SimulationWebServer():
    '''
    Memgraph demo fontend server. For now it wraps the flask server.
    '''

    def __init__(self):
        '''
        Instantiates the flask web server.
        '''
        self.server = Flask(__name__)
        self.is_simulation_running = False
        self.simulation_stats = None
        self.simulation_params = SimulationParams()
        self.simulation_executor = \
            SimulationExecutor().setup(self.simulation_params)

    def setup_routes(self):
        '''
        Setup all available rutes:
            GET  /ping
            POST /tasks
            POST /start
            POST /stop
            GET  /stats
            GET  /params
            POST /params
        '''
        self.server.add_url_rule('/ping', 'ping', self.ping)
        self.server.add_url_rule('/tasks', 'tasks', self.tasks,
                                 methods=['POST'])
        self.server.add_url_rule('/start', 'start', self.start,
                                 methods=['POST'])
        self.server.add_url_rule('/stop', 'stop', self.stop,
                                 methods=['POST'])
        self.server.add_url_rule('/stats', 'stats', self.stats,
                                 methods=['GET'])
        self.server.add_url_rule('/params', 'params_get', self.params_get,
                                 methods=['GET'])
        self.server.add_url_rule('/params', 'params_set', self.params_set,
                                 methods=['POST'])

    def run(self, host="127.0.0.1", port=8080, debug=False):
        '''
        Runs the server. Before run, routes are initialized.
        '''
        self.setup_routes()
        self.server.run(host=host, port=port, debug=debug)

    def ping(self):
        '''
        Ping endpoint. Returns 204 HTTP status code.
        '''
        return ('', 204)

    def tasks(self):
        '''
        Register tasks. Task is object that encapsulates single query data.
        '''
        data = request.get_json()['data']

        self.simulation_params.tasks = \
            [SimulationTask(item['id'], item['query'])
             for item in data]

        return ('', 200)

    def run_simulation(self):
        '''
        If flag is_simulation_running flag is up (True) the executor
        epoch will be executed. Epochs will be executed until somebody
        set is_simulation_running flag to Flase.
        '''
        log.info('new simulation run')

        while self.is_simulation_running:
            self.simulation_stats = self.simulation_executor.epoch()

    def start(self):
        '''
        Starts new executor epoch in separate thread.
        '''
        self.is_simulation_running = True
        t = threading.Thread(target=self.run_simulation, daemon=True)
        t.start()
        return ('', 204)

    def stop(self):
        '''
        On POST /stop, stops the executor. The is not immediately, first
        the is_simulation_running flag is set to False value, and next
        epoc of executor won't be executed.
        '''
        self.is_simulation_running = False
        return ('', 204)

    def stats(self):
        '''
        Returns the simulation stats. Queries per second.
        '''
        if not self.simulation_stats:
            return ('', 204)

        return jsonify(self.simulation_stats.json_data())

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

        param_names = ['protocol', 'host', 'port', 'username', 'password',
                       'period_time', 'queries_per_second',
                       'workers_per_query']

        for param in param_names:
            if param in data:
                setattr(self.simulation_params, param, data[param])

        return self.params_get()
