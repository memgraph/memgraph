#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
A script for transfering all data from a Neo4j database
into a Memgraph database.
'''

import logging
from time import time

from neo4j.v1 import GraphDatabase, basic_auth

log = logging.getLogger(__name__)


TEMP_ID = "__memgraph_temp_id_314235423"


def parse_args():
    # TODO parse args properly from the cmdline
    args = type('', (), {})()
    args.neo_url = "127.0.0.1:7687"
    args.neo_user = "neo4j"
    args.neo_password = "1234"
    args.neo_ssl = False
    args.memgraph_url = "127.0.0.1:7688"
    args.logging = "DEBUG"
    return args


def create_vertex_cypher(vertex):
    """
    Helper function that generates a cypher query for creting
    a vertex based on the given Bolt vertex.
    """
    if vertex.labels:
        labels = ":" + ":".join(vertex.labels)
    else:
        labels = ""
    vertex.properties[TEMP_ID] = vertex.id
    properties = ", ".join('%s: %r' % kv for kv
                           in vertex.properties.items())
    return "CREATE (%s {%s})" % (labels, properties)


def create_edge_cypher(edge):
    """
    Helper function that generates a cypher query for creting
    a edge based on the given Bolt edge.
    """
    properties = ", ".join('%s: %r' % kv for kv
                           in edge.properties.items())
    return "MATCH (from {%s: %r}), (to {%s: %r}) " \
           "CREATE (from)-[:%s {%s}]->(to)" % (
                TEMP_ID, edge.start, TEMP_ID, edge.end, edge.type, properties)


def transfer(neo_driver, memgraph_driver):
    """ Copies all the data from Neo4j to Memgraph. """

    # TODO add error handling
    neo_session = neo_driver.session()
    memgraph_session = memgraph_driver.session()

    # TODO when available add index on TEMP_ID

    # transfer vertices
    # TODO do it in batches
    # load batches of let's say a few thousand, insert batches of let's
    # say 100
    vertex_count = 0
    for vertex in neo_session.run("MATCH (n) RETURN n"):
        vertex = vertex['n']
        vertex_cypher = create_vertex_cypher(vertex)
        log.debug("Vertex creaton cypher: %s", vertex_cypher)
        memgraph_session.run(vertex_cypher).consume()
        # TODO store all vertices into JSON, also in batches
        # make destination and storage (if it should be stored) configurable
        vertex_count += 1

    # transfer edges
    # TODO do it in batches, like with vertices
    edge_count = 0
    for edge in neo_session.run("MATCH ()-[r]->() return r"):
        edge = edge['r']
        edge_cypher = create_edge_cypher(edge)
        log.debug("Edge creation cypher: %s", edge_cypher)
        memgraph_session.run(edge_cypher).consume()
        # TODO store all edges into JSON, configure like edges
        edge_count += 1

    # TODO when available remove index on TEMP_ID

    memgraph_session.run("MATCH (n) REMOVE n.%s" % TEMP_ID)

    log.info("Created %d vertiecs and %d edges", vertex_count, edge_count)


def main():
    args = parse_args()
    if args.logging:
        logging.basicConfig(level=args.logging)
        logging.getLogger("neo4j").setLevel(logging.WARNING)

    log.info("Memgraph from Neo4j data import tool")

    neo_driver = GraphDatabase.driver(
        "bolt://" + args.neo_url,
        auth=basic_auth(args.neo_user, args.neo_password),
        encrypted=args.neo_ssl)
    memgraph_driver = GraphDatabase.driver(
        "bolt://" + args.memgraph_url,
        auth=basic_auth("", ""),
        encrypted=False)

    start_time = time()
    transfer(neo_driver, memgraph_driver)
    log.info("Import complete in %.2f seconds", time() - start_time)

    pass

if __name__ == '__main__':
    main()
