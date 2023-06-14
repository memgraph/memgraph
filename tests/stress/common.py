# Copyright 2021 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

# -*- coding: utf-8 -*-

'''
Common methods for writing graph database
integration tests in python.

Only Bolt communication protocol is supported.
'''

import contextlib
import os
from threading import Thread
from time import sleep

from argparse import ArgumentParser
from neo4j import GraphDatabase, TRUST_ALL_CERTIFICATES


class OutputData:
    '''
    Encapsulates results and info about the tests.
    '''

    def __init__(self):
        # data in time format (name, time, unit)
        self._measurements = []
        # name, string data
        self._statuses = []

    def add_measurement(self, name, time, unit="s"):
        '''
        Stores measurement.

        :param name: str, name of measurement
        :param time: float, time value
        :param unit: str, time unit
        '''
        self._measurements.append((name, time, unit))

    def add_status(self, name, status):
        '''
        Stores status data point.

        :param name: str, name of data point
        :param status: printable value
        '''
        self._statuses.append((name, status))

    def dump(self, print_f=print):
        '''
        Dumps output using the given ouput function.

        Args:
            print_f - the function that consumes ouptput. Defaults to
            the 'print' function.
        '''
        print_f("Output data:")
        for name, status in self._statuses:
            print_f("    %s: %s" % (name, status))
        for name, time, unit in self._measurements:
            print_f("    %s: %s%s" % (name, time, unit))


def execute_till_success(session, query, max_retries=1000):
    '''
    Executes a query within Bolt session until the query is
    successfully executed against the database.

    Args:
        session - the bolt session to execute the query with
        query - str, the query to execute
        max_retries - int, maximum allowed number of attempts

    :param session: active Bolt session
    :param query: query to execute

    :return: tuple (results_data_list, number_of_failures, result_summary)
    '''
    no_failures = 0
    while True:
        try:
            result = session.run(query)
            data = result.data()
            summary = result.consume()
            return data, no_failures, summary
        except Exception:
            no_failures += 1
            if no_failures >= max_retries:
                raise Exception("Query '%s' failed %d times, aborting" %
                                (query, max_retries))


def batch(input, batch_size):
    """ Batches the given input (must be iterable).
    Supports input generators. Returns a generator.
    All is lazy. The last batch can contain less elements
    then `batch_size`, but is for sure more then zero.

    Args:
        input - iterable of elements
        batch_size - number of elements in the batch
    Return:
        a generator that yields batches of elements.
    """
    assert batch_size > 1, "Batch size must be greater then zero"

    batch = []
    for element in input:
        batch.append(element)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if len(batch):
        yield batch


def render(template, iterable_arguments):
    """
    Calls template.format() for each given argument.
    """
    for arguments in iterable_arguments:
        yield template.format(arguments)


def assert_equal(expected, actual, message):
    '''
    Compares expected and actual values. If values are not the same terminate
    the execution.

    :param expected: expected value
    :param actual: actual value
    :param message: str, message in case that the values are not equal, must
                    contain two placeholders (%s) to print the values.
    '''
    assert expected == actual, message % (expected, actual)


def connection_argument_parser():
    '''
    Parses arguments related to establishing database connection like
    host, port, username, etc.

    :return: An instance of ArgumentParser
    '''
    parser = ArgumentParser(description=__doc__)

    parser.add_argument('--endpoint', type=str, default='127.0.0.1:7687',
                        help='DBMS instance endpoint. '
                             'Bolt protocol is the only option.')
    parser.add_argument('--username', type=str, default='neo4j',
                        help='DBMS instance username.')
    parser.add_argument('--password', type=int, default='1234',
                        help='DBMS instance password.')
    parser.add_argument('--use-ssl', action='store_true',
                        help="Is SSL enabled?")
    return parser


@contextlib.contextmanager
def bolt_session(url, auth, ssl=False):
    '''
    with wrapper around Bolt session.

    :param url: str, e.g. "bolt://127.0.0.1:7687"
    :param auth: auth method, goes directly to the Bolt driver constructor
    :param ssl: bool, is ssl enabled
    '''
    driver = GraphDatabase.driver(
        url,
        auth=auth,
        encrypted=ssl,
        trust=TRUST_ALL_CERTIFICATES)
    session = driver.session()
    try:
        yield session
    finally:
        session.close()
        driver.close()


# If you are using session with multiprocessing take a look at SesssionCache
# in bipartite for an idea how to reuse sessions.
def argument_session(args):
    '''
    :return: Bolt session context manager based on program arguments
    '''
    return bolt_session('bolt://' + args.endpoint,
                        (args.username, str(args.password)),
                        args.use_ssl)


def argument_driver(args):
    return GraphDatabase.driver(
        'bolt://' + args.endpoint,
        auth=(args.username, str(args.password)),
        encrypted=args.use_ssl, trust=TRUST_ALL_CERTIFICATES)

# This class is used to create and cache sessions. Session is cached by args
# used to create it and process' pid in which it was created. This makes it
# easy to reuse session with python multiprocessing primitives like pmap.


class SessionCache:
    cache = {}

    @staticmethod
    def argument_session(args):
        key = tuple(vars(args).items()) + (os.getpid(),)
        if key in SessionCache.cache:
            return SessionCache.cache[key][1]
        driver = argument_driver(args)   # |
        session = driver.session()       # V
        SessionCache.cache[key] = (driver, session)
        return session

    @staticmethod
    def cleanup():
        for _, (driver, session) in SessionCache.cache.items():
            session.close()
            driver.close()


def periodically_execute(callable, args, interval, daemon=True):
    """
    Periodically calls the given callable.

    Args:
        callable - the callable to call
        args - arguments to pass to callable
        interval - time (in seconds) between two calls
        daemon - if the execution thread should be a daemon
    """
    def periodic_call():
        while True:
            sleep(interval)
            callable()

    Thread(target=periodic_call, args=args, daemon=daemon).start()
