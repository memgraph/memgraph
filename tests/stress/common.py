#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Common methods for writing graph database
integration tests in python.

Only Bolt communication protocol is supported.
'''

import contextlib

from argparse import ArgumentParser
from neo4j.v1 import GraphDatabase, basic_auth


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

    def console_dump(self):
        '''
        Dumps output data on the console output.
        '''
        print("Output data:")
        for name, status in self._statuses:
            print("    %s: %s" % (name, status))
        for name, time, unit in self._measurements:
            print("    %s: %s%s" % (name, time, unit))


def execute_till_success(session, query):
    '''
    Executes a query within Bolt session until the query is
    successfully executed against the database.

    :param session: active Bolt session
    :param query: query to execute

    :return: int, number of failuers
    '''
    no_failures = 0
    try_again = True
    while try_again:
        try:
            session.run(query).consume()
            try_again = False
        except Exception:
            no_failures += 1
    return no_failures


def batch_rendered_strings(t, dps, bs=1):
    '''
    Batches rendered strings based on template and data points. Template is
    populated from a single data point and than more rendered strings
    are batched into a single string.

    :param t: str, template for the rendered string (for one data point)
    :param dps, list or iterator with data points to populate the template
    :param bs: int, batch size

    :returns: (str, batched dps (might be useful for further rendering))
              e.g. if t = "test %s", dps = range(1, 6), bs = 2
                        yields are going to be:
                            "test 1 test 2", [1, 2]
                            "test 3 test 4", [3, 4]
                            "test 5" [5]
    '''
    no_dps = len(dps)
    for ndx in range(0, no_dps, bs):
        yield (' '.join([t % dp for dp in dps[ndx:min(ndx + bs, no_dps)]]),
               dps[ndx:min(ndx + bs, no_dps)])


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


def parse_connection_arguments():
    '''
    Parses arguments related to establishing database connection like
    host, port, username, etc.

    :return: An instance of ArgumentParser
    '''
    parser = ArgumentParser(description=__doc__)

    parser.add_argument('--endpoint', type=str, default='localhost:7687',
                        help='DBMS instance endpoint. '
                             'Bolt protocol is the only option.')
    parser.add_argument('--username', type=str, default='neo4j',
                        help='DBMS instance username.')
    parser.add_argument('--password', type=int, default='1234',
                        help='DBMS instance password.')
    parser.add_argument('--ssl-enabled', action='store_false',
                        help="Is SSL enabled?")

    parser.parse_known_args()

    return parser


@contextlib.contextmanager
def bolt_session(url, auth, ssl=False):
    '''
    with wrapper around Bolt session.

    :param url: str, e.g. "bolt://localhost:7687"
    :param auth: auth method, goes directly to the Bolt driver constructor
    :param ssl: bool, is ssl enabled
    '''
    driver = GraphDatabase.driver(url, auth=auth, encrypted=ssl)
    session = driver.session()
    try:
        yield session
    finally:
        session.close()
        driver.close()


def argument_session(args):
    '''
    :return: Bolt session based on program arguments
    '''
    return bolt_session('bolt://' + args.endpoint,
                        basic_auth(args.username, args.password))
