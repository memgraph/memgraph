# -*- coding: utf-8 -*-


class SimulationParams(object):
    '''
    '''

    def __init__(self):
        '''
        '''
        self.protocol = 'http'
        self.host = 'localhost'
        self.port = 7474

        self.workers_number = 16
        self.period_time = 0.5
        self.workers_per_query = 1
        self.queries_per_second = 15000
        self.recalculate_qps()

        self.tasks = []

    def json_data(self):
        '''
        '''
        return {
            "protocol": self.protocol,
            "host": self.host,
            "port": self.port,
            "workers_number": self.workers_number,
            "period_time": self.period_time,
            "workers_per_query": self.workers_per_query,
            "queries_per_second": self.queries_per_second,
            "queries_per_period": self.queries_per_period
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

    # workers number
    @property
    def workers_number(self):
        return self._workers_number

    @workers_number.setter
    def workers_number(self, value):
        self._workers_number = value

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

    def recalculate_qps(self):
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
        self.recalculate_qps()

    # period time
    @property
    def period_time(self):
        return self._period_time

    @period_time.setter
    def period_time(self, value):
        self._period_time = value
        self.recalculate_qps()
