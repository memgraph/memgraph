# -*- coding: utf-8 -*-

from flask import Flask


class WebService(object):

    def __init__(self, name):
        '''
        '''
        self.server = Flask(__name__)

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

    def setup_routes(self):
        '''
        '''
        self.add_route('/ping', self.ping, 'GET')

    def ping(self):
        '''
        Ping endpoint. Returns 204 HTTP status code.
        '''
        return ('', 204)
