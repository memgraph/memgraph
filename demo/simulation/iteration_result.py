# -*- coding: utf-8 -*-


class SimulationIterationResult(object):
    '''
    Encapsulates single worker result.
    '''

    def __init__(self, id, count, delta_t):
        '''
        :param id: str, query id
        :param count: int, number of the query exection
        :param delta_t: time of execution
        '''
        self.id = id
        self.count = count
        self.delta_t = delta_t
        self.queries_per_second = self.count / self.delta_t

    def json_data(self):
        '''
        :returns: dict {query_id(str):queries_per_second(float)}
        '''
        return {
            "id": self.id,
            "queries_per_second": self.queries_per_second
        }
