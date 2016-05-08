# -*- coding: utf-8 -*-


class SimulationGroupResult(object):
    '''
    Encapsulates query per seconds information for qroup of workers
    (all workers that execute the same query).
    '''

    def __init__(self, id, queries_per_second):
        '''
        :param id: str, query id
        :param queries_per_second: float, queries per second
        '''
        self.id = id
        self.queries_per_second = queries_per_second

    def json_data(self):
        '''
        :returns: dict, {query_id(str):queries_per_second(float)}
        '''
        return {
            'id': self.id,
            'queries_per_second': self.queries_per_second
        }
