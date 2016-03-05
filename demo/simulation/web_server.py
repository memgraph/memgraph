# -*- coding: utf-8 -*-

import json
import logging
import threading
from flask import Flask, request, jsonify

from .executor import SimulationExecutor
from .params import SimulationParams
from .task import SimulationTask

log = logging.getLogger(__name__)


class SimulationWebServer(object):
    '''
    Memgraph demo fontend server. For now it wraps the flask server.
    '''

    def __init__(self):
        '''
        Instantiates the flask web server.
        '''
        self.is_simulation_running = False
        self.simulation_stats = None
        self.simulation_params = SimulationParams()
        self.simulation_executor = SimulationExecutor()
        self.server = Flask(__name__)
        self.setup_routes()
        self.server.before_first_request(self.before_first_request)

    def setup_routes(self):
        '''
        Setup all routes.
        '''
        self.add_route('/ping', self.ping, 'GET')
        self.add_route('/tasks', self.tasks_get, 'GET')
        self.add_route('/tasks', self.tasks_set, 'POST')
        self.add_route('/start', self.start, 'POST')
        self.add_route('/stop', self.stop, 'POST')
        self.add_route('/stats', self.stats, 'GET')
        self.add_route('/params', self.params_get, 'GET')
        self.add_route('/params', self.params_set, 'POST')

    def before_first_request(self):
        '''
        Initializes simulation executor before first request.
        '''
        log.info('before first request')
        self.simulation_executor.setup(self.simulation_params)

    def add_route(self, route, code_method, http_method):
        '''
        Registers URL rule

        :param route: str, route string
        :param object_method: object method responsible for the
                              request handling
        :param http_method: name of http method
        '''
        self.server.add_url_rule(route, '%s_%s' % (route, http_method),
                                 code_method, methods=[http_method])

    def ping(self):
        '''
        Ping endpoint. Returns 204 HTTP status code.
        '''
        return ('', 204)

    def tasks_get(self):
        '''
        Retutns all defined tasks.
        '''
        return json.dumps(
            [task.json_data() for task in self.simulation_params.tasks]
        )

    def tasks_set(self):
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
