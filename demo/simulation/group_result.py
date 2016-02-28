# -*- coding: utf-8 -*-


class SimulationGrupeResult(object):
    '''
    '''

    def __init__(self, id, queries_per_period):
        '''
        '''
        self.id = id
        self.queries_per_second = queries_per_period

    def json_data(self):
        '''
        '''
        return {
            'id': self.id,
            'queries_per_second': self.queries_per_second
        }
