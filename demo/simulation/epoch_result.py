# -*- coding: utf-8 -*-


class SimulationEpochResult(object):
    '''
    '''

    def __init__(self, per_query, for_all):
        '''
        '''
        self.per_query = per_query
        self.for_all = for_all

    def json_data(self):
        '''
        '''
        return {
            "per_query": [item.json_data() for item in self.per_query],
            "for_all": self.for_all
        }
