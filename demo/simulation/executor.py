# -*- coding: utf-8 -*-

import time
import logging
import itertools
import requests
from concurrent.futures import ProcessPoolExecutor

from .epoch_result import SimulationEpochResult
from .group_result import SimulationGrupeResult
from .iteration_result import SimulationIterationResult

log = logging.getLogger(__name__)


def calculate_qps(results):
    '''
    '''
    max_delta_t = max([result.delta_t for result in results])
    qps = sum([result.count * (max_delta_t / result.delta_t)
               for result in results]) / max_delta_t
    return qps


def send(query):
    '''
    '''
    requests.post('http://localhost:7474/db/data/transaction/commit',
                  json={
                      "statements": [
                          {"statement": query}
                      ]
                  },
                  headers={'Authorization': 'Basic bmVvNGo6cGFzcw=='})


class SimulationExecutor(object):
    '''
    '''

    def __init__(self):
        '''
        '''
        pass

    def setup(self, params):
        '''
        '''
        self.params = params
        return self

    def iteration(self, task):
        '''
        '''
        count = 0
        start_time = time.time()

        for i in range(self.params.queries_per_period):
            send(task.query)

            end_time = time.time()
            count = count + 1
            delta_t = end_time - start_time

            if delta_t > self.params.period_time:
                break

        return SimulationIterationResult(task.id, count, delta_t)

    def epoch(self):
        '''
        '''
        log.info('epoch')

        pool = ProcessPoolExecutor
        with pool(max_workers=self.params.workers_number) as executor:

            # execute all tasks
            futures = [executor.submit(self.iteration, task)
                       for task in self.params.tasks
                       for i in range(self.params.workers_per_query)]
            results = [future.result() for future in futures]

            # per query calculation
            grouped = itertools.groupby(results, lambda x: x.id)
            per_query = [SimulationGrupeResult(id, calculate_qps(list(tasks)))
                         for id, tasks in grouped]

            # for all calculation
            for_all = calculate_qps(results)

            return SimulationEpochResult(per_query, for_all)

    def execute(self):
        '''
        '''
        return self.epoch()
