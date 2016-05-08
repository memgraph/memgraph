# -*- coding: utf-8 -*-

import os
import json
import time
import logging
import itertools
import http
from concurrent.futures import ProcessPoolExecutor

from .substitutor import substitute
from .epoch_result import SimulationEpochResult
from .group_result import SimulationGroupResult
from .iteration_result import SimulationIterationResult

log = logging.getLogger(__name__)


def calculate_qps(results, delta_t=None):
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
    min_start_time = min([result.start_time for result in results])
    max_end_time = max([result.end_time for result in results])
    delta_t = max_end_time - min_start_time
    qps = sum([result.count for result in results]) / delta_t
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

    def send(self, connection, query):
        '''
        Sends the query to the graph database.

        :param connection: http.client.HTTPConnection
        :param query: str, query string
        '''
        body = json.dumps({'statements': [{'statement': substitute(query)}]})
        headers = {
            'Authorization': self.params.authorization,
            'Content-Type': 'application/json'
        }
        connection.request('POST', '/db/data/transaction/commit',
                           body=body, headers=headers)
        response = connection.getresponse()
        log.debug('New response: %s' % response.read())

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

        log.debug("New worker with PID: %s" % os.getpid())

        connection = http.client.HTTPConnection(
            self.params.host, self.params.port)
        connection.connect()

        start_time = time.time()

        for i in range(self.params.queries_per_period):

            # send the query on execution
            self.send(connection, task.query)

            # calculate delta time
            end_time = time.time()
            delta_t = end_time - start_time

            count = count + 1

            if delta_t > self.params.period_time:
                break

        connection.close()

        return SimulationIterationResult(task.id, count, start_time, end_time)

    def epoch(self):
        '''
        Single simulation epoc. All workers are going to execute
        their queries in the period that is period_time seconds length.
        '''
        log.debug('epoch')

        max_workers = self.params.workers_per_query * len(self.params.tasks)

        with self.pool(max_workers=max_workers) as executor:

            log.debug('pool iter')

            # execute all tasks
            start_time = time.time()
            futures = [executor.submit(self.iteration, task)
                       for task in self.params.tasks
                       for i in range(self.params.workers_per_query)]
            results = [future.result() for future in futures]
            end_time = time.time()
            epoch_time = end_time - start_time
            log.info("Total epoch time: %s" % epoch_time)

            # per query calculation
            grouped = itertools.groupby(results, lambda x: x.id)
            per_query = [SimulationGroupResult(id, calculate_qps(list(tasks)))
                         for id, tasks in grouped]

            # for all calculation
            for_all = calculate_qps(results)

            log.info('Queries per period: %s' % sum([r.count
                                                     for r in results]))

            return SimulationEpochResult(per_query, for_all)
