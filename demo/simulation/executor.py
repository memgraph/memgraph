# -*- coding: utf-8 -*-

import time
import logging
import itertools
import urllib.request
from concurrent.futures import ProcessPoolExecutor

from .epoch_result import SimulationEpochResult
from .group_result import SimulationGroupResult
from .iteration_result import SimulationIterationResult

log = logging.getLogger(__name__)


def calculate_qps(results):
    '''
    Calculates queris per second for the results list. The main idea
    is to scale up results to the result with the biggest execution time.

    Example:
        Let say that 2 workers execute the same query. First worker
        executes 100 queries in 1s, second worker executes 10 queries in 10s.
        In that case first query result has to be scaled up.
        Here we are goint to make aproximation, if first worker
        execution time was 10s, it would execute 1000 queries.
        So, the total speed is (1000q + 10q) / 10s = 101 qps. In that case
        101 would be returned from this function.

    :param results: list of SimulationIterationResult objects
    :returns: queries per second result calculated on the input list
    '''
    max_delta_t = max([result.delta_t for result in results])
    qps = sum([result.count * (max_delta_t / result.delta_t)
               for result in results]) / max_delta_t
    return qps


class SimulationExecutor(object):
    '''
    The main executor object. Every period the instance of this class
    will execute all queries, collect the results and calculate speed of
    queries execution.
    '''

    def setup(self, params):
        '''
        Setup params and initialize the workers pool.

        :param params: SimulationParams object
        '''
        self.params = params
        self.pool = ProcessPoolExecutor
        return self

    def send(self, query):
        '''
        Sends the query to the graph database.

        TODO: replace random arguments

        :param query: str, query string
        '''
        # requests.post('http://localhost:7474/db/data/transaction/commit',
        #               json={
        #                   "statements": [
        #                       {"statement": query}
        #                   ]
        #               },
        #               headers={'Authorization': 'Basic bmVvNGo6cGFzcw=='})
        # requests.get('http://localhost:7474/db/data/ping')
        response = urllib.request.urlopen('http://localhost:7474/db/data/ping')
        response.read()

    def iteration(self, task):
        '''
        Executes the task. Task encapsulates the informations about query.
        The task is smallest piece of work and this method will try to execute
        queries (one query, more times) from the task as fastest as possible.
        Execution time of this method is constrained with the period_time time.

        :param task: instance of SimulationTask class.
        :returns: SimulationIterationResult
        '''
        count = 0
        delta_t = 0

        for i in range(self.params.queries_per_period):

            # send the query on execution
            start_time = time.time()
            self.send(task.query)
            end_time = time.time()

            # calculate delta time
            delta_t = delta_t + (end_time - start_time)

            count = count + 1

            if delta_t > self.params.period_time:
                break

        return SimulationIterationResult(task.id, count, delta_t)

    def epoch(self):
        '''
        Single simulation epoc. All workers are going to execute
        their queries in the period that is period_time seconds length.
        '''
        log.info('epoch')

        with self.pool() as executor:

            log.info('pool iter')

            # execute all tasks
            futures = [executor.submit(self.iteration, task)
                       for task in self.params.tasks
                       for i in range(self.params.workers_per_query)]
            results = [future.result() for future in futures]

            # per query calculation
            grouped = itertools.groupby(results, lambda x: x.id)
            per_query = [SimulationGroupResult(id, calculate_qps(list(tasks)))
                         for id, tasks in grouped]

            # for all calculation
            for_all = calculate_qps(results)

            return SimulationEpochResult(per_query, for_all)
