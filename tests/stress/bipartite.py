#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Large bipartite graph stress test.
'''

import logging
import multiprocessing
import time

from common import parse_connection_arguments, argument_session, \
                   assert_equal, batch_rendered_strings, \
                   OutputData, execute_till_success


def parse_args():
    '''
    Parses user arguments

    :return: parsed arguments
    '''
    parser = parse_connection_arguments()

    parser.add_argument('--no-workers', type=int,
                        default=multiprocessing.cpu_count(),
                        help='Number of concurrent workers.')
    parser.add_argument('--no-u', type=int, default=100,
                        help='Size of U set in the bipartite graph.')
    parser.add_argument('--no-v', type=int, default=100,
                        help='Size of V set in the bipartite graph.')
    parser.add_argument('--vertex-batch-size', type=int, default=100,
                        help="Create vertices in batches of this size.")
    parser.add_argument('--edge-batching', action='store_true',
                        help='Create edges in batches.')
    parser.add_argument('--edge-batch-size', type=int, default=100,
                        help='Number of edges in a batch when edges '
                             'are created in batches.')

    return parser.parse_args()


log = logging.getLogger(__name__)
args = parse_args()
output_data = OutputData()


def create_u_v_edges(u):
    '''
    Creates nodes and checks that all nodes were created.
    create edges from one vertex in U set to all vertex of V set

    :param worker_id: worker id

    :return: tuple (worker_id, create execution time, time unit)
    '''
    start_time = time.time()
    with argument_session(args) as session:
        no_failures = 0
        match_u_query = 'MATCH (u:U {id: %s}) ' % u
        if args.edge_batching:
            # TODO: try to randomize execution, the execution time should
            # be smaller, add randomize flag
            for batchm, dps in batch_rendered_strings(
                    'MATCH (v%s:V {id: %s})',
                    [(i, i) for i in range(args.no_v)],
                    args.edge_batch_size):
                for batchc, _ in batch_rendered_strings(
                        'CREATE (u)-[:R]->(v%s)',
                        [dpi for dpi, _ in dps],
                        args.edge_batch_size):
                    no_failures += execute_till_success(
                        session, match_u_query + batchm + batchc)
        else:
            no_failures += execute_till_success(
                session, match_u_query + 'MATCH (v:V) CREATE (u)-[:R]->(v)')

    end_time = time.time()
    return u, end_time - start_time, "s", no_failures


def traverse_from_u_worker(u):
    '''
    Traverses edges starting from an element of U set.
    Traversed labels are: :U -> :V -> :U.
    '''
    with argument_session(args) as session:
        start_time = time.time()
        assert_equal(
            args.no_u * args.no_v - args.no_v,  # cypher morphism
            session.run("MATCH (u1:U {id: %s})-[e1]->(v:V)<-[e2]-(u2:U) "
                        "RETURN count(v) AS cnt" % u).data()[0]['cnt'],
            "Number of traversed edges started "
            "from U(id:%s) is wrong!. " % u +
            "Expected: %s Actual: %s")
        end_time = time.time()
    return u, end_time - start_time, 's'


def traverse_from_v_worker(v):
    '''
    Traverses edges starting from an element of V set.
    Traversed labels are: :V -> :U -> :V.
    '''
    with argument_session(args) as session:
        start_time = time.time()
        assert_equal(
           args.no_u * args.no_v - args.no_u,  # cypher morphism
           session.run("MATCH (v1:V {id: %s})<-[e1]-(u:U)-[e2]->(v2:V) "
                       "RETURN count(u) AS cnt" % v).data()[0]['cnt'],
           "Number of traversed edges started "
           "from V(id:%s) is wrong!. " % v +
           "Expected: %s Actual: %s")
        end_time = time.time()
    return v, end_time - start_time, 's'


def execution_handler():
    '''
    Initializes client processes, database and starts the execution.
    '''
    # instance cleanup
    with argument_session(args) as session:
        start_time = time.time()

        # clean existing database
        session.run('MATCH (n) DETACH DELETE n').consume()
        cleanup_end_time = time.time()
        output_data.add_measurement("cleanup_time",
                                    cleanup_end_time - start_time)

        # create vertices
        # create U vertices
        for vertex_batch, _ in batch_rendered_strings('CREATE (:U {id: %s})',
                                                      range(args.no_u),
                                                      args.vertex_batch_size):
            session.run(vertex_batch).consume()
        # create V vertices
        for vertex_batch, _ in batch_rendered_strings('CREATE (:V {id: %s})',
                                                      range(args.no_v),
                                                      args.vertex_batch_size):
            session.run(vertex_batch).consume()
        vertices_create_end_time = time.time()
        output_data.add_measurement(
            'vertices_create_time',
            vertices_create_end_time - cleanup_end_time)

        # concurrent create execution & tests
        with multiprocessing.Pool(args.no_workers) as p:
            create_edges_start_time = time.time()
            for worker_id, create_time, time_unit, no_failures in \
                    p.map(create_u_v_edges, [i for i in range(args.no_u)]):
                log.info('Worker ID: %s; Create time: %s%s Failures: %s' %
                         (worker_id, create_time, time_unit, no_failures))
            create_edges_end_time = time.time()
            output_data.add_measurement(
                'edges_create_time',
                create_edges_end_time - create_edges_start_time)

            # check total number of edges
            assert_equal(
                args.no_v * args.no_u,
                session.run(
                    'MATCH ()-[r]->() '
                    'RETURN count(r) AS cnt').data()[0]['cnt'],
                "Total number of edges isn't correct! Expected: %s Actual: %s")

            # check traversals starting from all elements of U
            traverse_from_u_start_time = time.time()
            for u, traverse_u_time, time_unit in \
                    p.map(traverse_from_u_worker,
                          [i for i in range(args.no_u)]):
                log.info("U {id: %s} %s%s" % (u, traverse_u_time, time_unit))
            traverse_from_u_end_time = time.time()
            output_data.add_measurement(
                'traverse_from_u_time',
                traverse_from_u_end_time - traverse_from_u_start_time)

            # check traversals starting from all elements of V
            traverse_from_v_start_time = time.time()
            for v, traverse_v_time, time_unit in \
                    p.map(traverse_from_v_worker,
                          [i for i in range(args.no_v)]):
                log.info("V {id: %s} %s%s" % (v, traverse_v_time, time_unit))
            traverse_from_v_end_time = time.time()
            output_data.add_measurement(
                'traverse_from_v_time',
                traverse_from_v_end_time - traverse_from_v_start_time)

        # check total number of vertices
        assert_equal(
            args.no_v + args.no_u,
            session.run('MATCH (n) RETURN count(n) AS cnt').data()[0]['cnt'],
            "Total number of vertices isn't correct! Expected: %s Actual: %s")

        # check total number of edges
        assert_equal(
            args.no_v * args.no_u,
            session.run(
                'MATCH ()-[r]->() RETURN count(r) AS cnt').data()[0]['cnt'],
            "Total number of edges isn't correct! Expected: %s Actual: %s")

        end_time = time.time()
        output_data.add_measurement("total_execution_time",
                                    end_time - start_time)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    output_data.add_status("stress_test_name", "bipartite")
    output_data.add_status("number_of_vertices", args.no_u + args.no_v)
    output_data.add_status("number_of_edges", args.no_u * args.no_v)
    output_data.add_status("vertex_batch_size", args.vertex_batch_size)
    output_data.add_status("edge_batching", args.edge_batching)
    output_data.add_status("edge_batch_size", args.edge_batch_size)
    execution_handler()
    output_data.console_dump()
