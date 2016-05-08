# -*- coding: utf-8 -*-


class SimulationEpochResult(object):
    '''
    Encapsulates single epoch result.
    '''

    def __init__(self, per_query, for_all):
        '''
        Sets per_query and for_all results.

        :param per_query: list of SimulationGroupResult objects
        :param for_all: float, queries per second
        '''
        self.per_query = per_query
        self.for_all = for_all

    def json_data(self):
        '''
        :returns: dict, epoch results
        '''
        return {
            "per_query": [item.json_data() for item in self.per_query],
            "for_all": self.for_all
        }
