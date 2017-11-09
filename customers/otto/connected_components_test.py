#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
This script attempts to evaluate the feasibility of using Memgraph for
Otto group's usecase. The usecase is finding connected componentes in
a large, very sparse graph (cca 220M nodes, 250M edges), based on a dynamic
inclusion / exclusion of edges (w.r.t variable parameters and the source node
type).

This implementation defines a random graph with the given number of nodes
and edges and looks for connected components using breadth-first expansion.
Edges are included / excluded based on a simple expression, only demonstrating
possible usage.
"""

from argparse import ArgumentParser
import logging
from time import time
from collections import defaultdict
from math import log2
from random import randint

from neo4j.v1 import GraphDatabase

log = logging.getLogger(__name__)


def generate_graph(sess, node_count, edge_count):
    # An index that will speed-up edge creation.
    sess.run("CREATE INDEX ON :Node(id)").consume()

    # Create the given number of nodes with a randomly selected type from:
    # [0.5, 1.5, 2.5].
    sess.run(("UNWIND range(0, {} - 1) AS id CREATE "
              "(:Node {{id: id, type: 0.5 + tointeger(rand() * 3)}})").format(
                 node_count)).consume()

    # Create the given number of edges, each with a 'value' property of
    # a random [0, 3.0) float. Each edge connects two random nodes, so the
    # expected node degree is (edge_count * 2 / node_count). Generate edges
    # so the connectivity is non-uniform (to produce connected components of
    # various sizes).
    sess.run(("UNWIND range(0, {0} - 1) AS id WITH id "
              "MATCH (from:Node {{id: tointeger(rand() * {1})}}), "
              "(to:Node {{id: tointeger(rand() * {1} * id / {0})}}) "
              "CREATE (from)-[:Edge {{value: 3 * rand()}}]->(to)").format(
                 edge_count, node_count)).consume()


def get_connected_ids(sess, node_id):
    # Matches a node with the given ID and returns the IDs of all the nodes
    # it is connected to. Note that within the BFS lambda expression there
    # is an expression used to filter out edges expanded over.
    return sess.run((
        "MATCH (from:Node {{id: {}}})-"
        "[*bfs (e, n | abs(from.type - e.value) < 0.80)]-(d) "
        "RETURN count(*) AS c").format(node_id)).data()[0]['c']


def parse_args():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--endpoint', type=str, default='localhost:7687',
                        help='Memgraph instance endpoint. ')
    parser.add_argument('--node-count', type=int, default=1000,
                        help='The number of nodes in the graph')
    parser.add_argument('--edge-count', type=int, default=1000,
                        help='The number of edges in the graph')
    parser.add_argument('--sample-count', type=int, default=None,
                        help='The number of samples to take')
    return parser.parse_args()


def main():
    args = parse_args()
    logging.basicConfig(level=logging.INFO)
    log.info("Memgraph - Otto test database generator")
    logging.getLogger("neo4j").setLevel(logging.WARNING)

    driver = GraphDatabase.driver(
        'bolt://' + args.endpoint,
        auth=("ignored", "ignored"),
        encrypted=False)
    sess = driver.session()

    sess.run("MATCH (n) DETACH DELETE n").consume()

    log.info("Generating graph with %s nodes and %s edges...",
             args.node_count, args.edge_count)
    generate_graph(sess, args.node_count, args.edge_count)

    # Track which vertices have been found as part of a component.
    start_time = time()
    max_query_time = 0
    log.info("Looking for connected components...")
    # Histogram of log2 sizes of connected components found.
    histogram = defaultdict(int)
    sample_count = args.sample_count if args.sample_count else args.node_count
    for i in range(sample_count):
        node_id = randint(0, args.node_count - 1)
        query_start_time = time()
        log2_size = int(log2(1 + get_connected_ids(sess, node_id)))
        max_query_time = max(max_query_time, time() - query_start_time)
        histogram[log2_size] += 1
    elapsed = time() - start_time
    log.info("Connected components found in %.2f sec (avg %.2fms, max %.2fms)",
             elapsed, elapsed / sample_count * 1000, max_query_time * 1000)
    log.info("Component size histogram (count | range)")
    for log2_size, count in sorted(histogram.items()):
        log.info("\t%5d | %d - %d", count, 2 ** log2_size,
                 2 ** (log2_size + 1) - 1)

    sess.close()
    driver.close()


if __name__ == '__main__':
    main()
