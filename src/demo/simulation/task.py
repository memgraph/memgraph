# -*- coding: utf-8 -*-


class SimulationTask(object):
    '''
    Encapsulates query data.
    '''

    def __init__(self, id, query):
        '''
        :param id: query id
        :param query: str, query string
        '''
        self.id = id
        self.query = query

    def json_data(self):
        '''
        :returns: dict with all elements
        '''
        return {
            "id": self.id,
            "query": self.query
        }
