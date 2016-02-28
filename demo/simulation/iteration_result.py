# -*- coding: utf-8 -*-


class SimulationIterationResult(object):
    '''
    '''

    def __init__(self, id, count, delta_t):
        '''
        '''
        self.id = id
        self.count = count
        self.delta_t = delta_t
        self.queries_per_second = self.count / self.delta_t

    def json_data(self):
        '''
        '''
        return {
            "id": self.id,
            "queries_per_second": self.queries_per_second
        }
