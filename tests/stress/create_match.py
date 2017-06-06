#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Large scale stress test. Tests only node creation.

The idea is to run this test on machines with huge amount of memory e.g. 2TB.
'''

import logging
import multiprocessing
import random
import time

from collections import defaultdict
from common import parse_connection_arguments, argument_session


def parse_args():
    '''
    Parses user arguments

    :return: parsed arguments
    '''
    parser = parse_connection_arguments()

    # specific
    parser.add_argument('--no-workers', type=int,
                        default=multiprocessing.cpu_count(),
                        help='Number of concurrent workers.')
    parser.add_argument('--no-vertices', type=int, default=100,
                        help='Number of created vertices.')
    parser.add_argument('--max-property-value', type=int, default=1000,
                        help='Maximum value of property - 1. A created node '
                        'will have a property with random value from 0 to '
                        'max_property_value - 1.')
    parser.add_argument('--create-pack-size', type=int, default=1,
                        help='Number of CREATE clauses in a query')
    return parser.parse_args()


log = logging.getLogger(__name__)
args = parse_args()


def create_worker(worker_id):
    '''
    Creates nodes and checks that all nodes were created.

    :param worker_id: worker id

    :return: tuple (worker_id, create execution time, time unit)
    '''
    assert args.no_vertices > 0, 'Number of vertices has to be positive int'

    generated_xs = defaultdict(int)
    create_query = ''
    with argument_session(args) as session:
        # create vertices
        start_time = time.time()
        for i in range(0, args.no_vertices):
            random_number = random.randint(0, args.max_property_value - 1)
            generated_xs[random_number] += 1
            create_query += 'CREATE (:Label_T%s {x: %s}) ' % \
                            (worker_id, random_number)
            # if full back or last item -> execute query
            if (i + 1) % args.create_pack_size == 0 or \
                    i == args.no_vertices - 1:
                session.run(create_query).consume()
                create_query = ''
        create_time = time.time()
        # check total count
        result_set = session.run('MATCH (n:Label_T%s) RETURN count(n) AS cnt' %
                                 worker_id).data()[0]
        assert result_set['cnt'] == args.no_vertices, \
            'Create vertices Expected: %s Created: %s' % \
            (args.no_vertices, result_set['cnt'])
        # check count per property value
        for i, size in generated_xs.items():
            result_set = session.run('MATCH (n:Label_T%s {x: %s}) '
                                     'RETURN count(n) AS cnt'
                                     % (worker_id, i)).data()[0]
            assert result_set['cnt'] == size, "Per x count isn't good " \
                "(Label: Label_T%s, prop x: %s" % (worker_id, i)
        return (worker_id, create_time - start_time, "s")


def create_handler():
    '''
    Initializes processes and starts the execution.
    '''
    # instance cleanup
    with argument_session(args) as session:
        session.run("MATCH (n) DETACH DELETE n").consume()

        # concurrent create execution & tests
        with multiprocessing.Pool(args.no_workers) as p:
            for worker_id, create_time, time_unit in \
                    p.map(create_worker, [i for i in range(args.no_workers)]):
                log.info('Worker ID: %s; Create time: %s%s' %
                         (worker_id, create_time, time_unit))

        # check total count
        expected_total_count = args.no_workers * args.no_vertices
        total_count = session.run(
            'MATCH (n) RETURN count(n) AS cnt').data()[0]['cnt']
        assert total_count == expected_total_count, \
            'Total vertex number: %s Expected: %s' % \
            (total_count, expected_total_count)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    create_handler()
