# -*- coding: utf-8 -*-

import base64
import logging

log = logging.getLogger(__name__)


class SimulationParams(object):
    '''
    Encapsulates the simulation params.
    '''

    def __init__(self):
        '''
        Setup default params values.
        '''
        self.protocol = 'http'
        self.host = 'localhost'
        self.port = 7474
        self.username = ''
        self.password = ''

        self.period_time = 0.5
        self.workers_per_query = 1
        self.queries_per_second = 15000
        self.recalculate_qpp()

        self.tasks = []

    def json_data(self):
        '''
        :returns: dict with all param values
        '''
        return {
            "protocol": self.protocol,
            "host": self.host,
            "port": self.port,
            "username": self.username,
            "password": self.password,
            "period_time": self.period_time,
            "workers_per_query": self.workers_per_query,
            "queries_per_period": self.queries_per_period,
            "queries_per_second": self.queries_per_second
        }

    # protocol
    @property
    def protocol(self):
        return self._protocol

    @protocol.setter
    def protocol(self, value):
        self._protocol = value

    # host
    @property
    def host(self):
        return self._host

    @host.setter
    def host(self, value):
        self._host = value

    # port
    @property
    def port(self):
        return self._port

    @port.setter
    def port(self, value):
        self._port = value

    # username
    @property
    def username(self):
        return self._username

    @username.setter
    def username(self, value):
        self._username = value
        log.info("Username is now: %s" % self._username)
        self.http_basic()

    # password
    @property
    def password(self):
        return self._password

    @password.setter
    def password(self, value):
        self._password = value
        log.info("Password is now: %s" % self._password)
        self.http_basic()

    def http_basic(self):
        '''
        Recalculates http authorization header.
        '''
        try:
            encoded = base64.b64encode(
                str.encode(self.username + ":" + self.password))
            self.authorization = "Basic " + encoded.decode()
            log.info("Authorization is now: %s" % self.authorization)
        except AttributeError:
            log.debug("Username or password isn't defined.")
        except Exception as e:
            log.exception(e)

    # authorization
    @property
    def authorization(self):
        return self._authorization

    @authorization.setter
    def authorization(self, value):
        self._authorization = value

    # workers per query
    @property
    def workers_per_query(self):
        return self._workers_per_query

    @workers_per_query.setter
    def workers_per_query(self, value):
        self._workers_per_query = value

    # queries per second
    @property
    def queries_per_second(self):
        return self._queries_per_second

    @queries_per_second.setter
    def queries_per_second(self, value):
        self._queries_per_second = value
        self.recalculate_qpp()

    def recalculate_qpp(self):
        try:
            self.queries_per_period = \
                int(self.queries_per_second * self.period_time)
        except:
            pass

    # queries per period
    @property
    def queries_per_period(self):
        return self._queries_per_period

    @queries_per_period.setter
    def queries_per_period(self, value):
        self._queries_per_period = value

    # period time
    @property
    def period_time(self):
        return self._period_time

    @period_time.setter
    def period_time(self, value):
        self._period_time = value
        self.recalculate_qpp()
