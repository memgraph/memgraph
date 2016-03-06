# -*- coding: utf-8 -*-


class SimulationIterationResult(object):
    '''
    Encapsulates single worker result.
    '''

    def __init__(self, id, count, start_time, end_time):
        '''
        :param id: str, query id
        :param count: int, number of the query exection
        :param delta_t: time of execution
        '''
        self.id = id
        self.count = count
        self.start_time = start_time
        self.end_time = end_time
        self.delta_t = end_time - start_time
        self.queries_per_second = self.count / self.delta_t

    def json_data(self):
        '''
        :returns: dict {query_id(str):queries_per_second(float)}
        '''
        return {
            "id": self.id,
            "queries_per_second": self.queries_per_second
        }
