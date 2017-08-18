#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
A long running test that performs
random CRUD ops on a bolt database.

Parameterized with vertex and edge counts around which
the graph state oscilates.
"""

import logging
import multiprocessing
import neo4j.exceptions
import random
import time
from collections import defaultdict

import common


log = logging.getLogger(__name__)


INDEX_FORMAT = "indexed_label{}"


def random_element(lst):
    return lst[random.randint(0, len(lst) - 1)]


def bernoulli(p):
    return random.random() < p


class Graph():
    """
    Exposes functions for working on a graph, and tracks some
    statistics about graph state.
    """

    def __init__(self, vertex_count, edge_count, labels=5):
        """
        Args:
            vertex_count - int, desired vertex count
            edge_count - int, desired edge count
            labels - int, the number of labels to use
        """

        # desired vertex and edge counts
        self.vertex_count = vertex_count
        self.edge_count = edge_count

        # storage
        self.edges = []
        self.vertices = []
        self.labels = {"label%d" % i: [] for i in range(labels)}

        # info about query failures, maps exception string representations into
        # occurence counts
        self._query_failure_counts = defaultdict(int)

    def add_query_failure(self, reason):
        self._query_failure_counts[reason] += 1

    def query_failures(self):
        return dict(self._query_failure_counts)


class GraphSession():
    """
    Encapsulates a Graph and a Bolt session and provides CRUD op functions.
    Also defines a run-loop for a generic exectutor, and a graph state
    verification function.
    """

    def __init__(self, sid, graph, session):
        self.sid = sid

        # the label in the database that is indexed
        # used for matching vertices faster
        self.indexed_label = INDEX_FORMAT.format(sid)

        self.vertex_id = 1
        self.edge_id = 1

        self.graph = graph
        self.session = session
        self.executed_queries = 0
        self._start_time = time.time()

    @property
    def v(self):
        return self.graph.vertices

    @property
    def e(self):
        return self.graph.edges

    def execute(self, query):
        log.debug("Runner %d executing query: %s", self.sid, query)
        self.executed_queries += 1
        try:
            return self.session.run(query).data()
        except neo4j.exceptions.ServiceUnavailable as e:
            raise e
        except Exception as e:
            self.graph.add_query_failure(str(e))
            return None

    def create_vertices(self, vertices_count):
        query = ""
        if vertices_count == 0: return
        for _ in range(vertices_count):
            query += "CREATE (:%s {id: %r}) " % (self.indexed_label,
                     self.vertex_id)
            self.v.append(self.vertex_id)
            self.vertex_id += 1
        self.execute(query)

    def remove_vertex(self):
        vertex_id = random_element(self.v)
        result = self.execute(
            "MATCH (n:%s {id: %r}) OPTIONAL MATCH (n)-[r]-() "
            "DETACH DELETE n RETURN n.id, labels(n), r.id" %
            (self.indexed_label, vertex_id))
        if result:
            process_vertex_ids = set()
            for row in result:
                # remove vertex but note there could be duplicates
                vertex_id = row['n.id']
                if vertex_id not in process_vertex_ids:
                    process_vertex_ids.add(vertex_id)
                    self.v.remove(vertex_id)
                    for label in row['labels(n)']:
                        if (label != self.indexed_label):
                            self.graph.labels[label].remove(vertex_id)
                # remove edge
                edge_id = row['r.id']
                if edge_id != None:
                    self.e.remove(edge_id)

    def create_edge(self):
        creation = self.execute(
            "MATCH (from:%s {id: %r}), (to:%s {id: %r}) "
            "CREATE (from)-[e:EdgeType {id: %r}]->(to) RETURN e" % (
            self.indexed_label, random_element(self.v), self.indexed_label,
            random_element(self.v), self.edge_id))
        if creation:
            self.e.append(self.edge_id)
            self.edge_id += 1

    def remove_edge(self):
        edge_id = random_element(self.e)
        result = self.execute("MATCH (:%s)-[e {id: %r}]->(:%s) DELETE e "
                              "RETURN e.id" % (self.indexed_label, edge_id,
                                               self.indexed_label))
        if result:
            self.e.remove(edge_id)

    def add_label(self):
        vertex_id = random_element(self.v)
        label = random.choice(list(self.graph.labels.keys()))
        # add a label on a vertex that didn't have that label
        # yet (we need that for book-keeping)
        result = self.execute("MATCH (v:%s {id: %r}) WHERE not v:%s SET v:%s "
                              "RETURN v.id" % (self.indexed_label, vertex_id,
                                               label, label))
        if result:
            self.graph.labels[label].append(vertex_id)

    def update_global_vertices(self):
        lo = random.randint(0, self.vertex_id)
        hi = lo + int(self.vertex_id * 0.01)
        num = random.randint(0, 2 ** 20)
        self.execute("MATCH (n) WHERE n.id > %d AND n.id < %d "
                     "SET n.value = %d" % (lo, hi, num))

    def update_global_edges(self):
        lo = random.randint(0, self.edge_id)
        hi = lo + int(self.edge_id * 0.01)
        num = random.randint(0, 2 ** 20)
        self.execute("MATCH ()-[e]->() WHERE e.id > %d AND e.id < %d "
                     "SET e.value = %d" % (lo, hi, num))

    def verify_graph(self):
        """ Checks if the local info corresponds to DB state """

        def test(obj, length, message):
            assert len(obj) == length, message % (len(obj), length)

        def get(query, key):
            ret = self.execute(query)
            assert ret != None, "Query '{}' returned 'None'!".format(query)
            return [row[key] for row in ret]

        test(self.v, get("MATCH (n:{}) RETURN count(n)".format(
             self.indexed_label), "count(n)")[0],
             "Expected %d vertices, found %d")
        test(self.e, get("MATCH (:{0})-[r]->(:{0}) RETURN count(r)".format(
             self.indexed_label), "count(r)")[0],
             "Expected %d edges, found %d")
        for lab, exp in self.graph.labels.items():
            test(exp, get("MATCH (n:%s:%s) RETURN count(n)" % (
                 self.indexed_label, lab), "count(n)")[0],
                 "Expected %d vertices with label '{}', found %d".format(
                 lab))

        log.info("Runner %d graph verification success:", self.sid)
        log.info("\tExecuted %d queries in %.2f seconds",
                 self.executed_queries, time.time() - self._start_time)
        log.info("\tGraph has %d vertices and %d edges",
                 len(self.v), len(self.e))
        for label in sorted(self.graph.labels.keys()):
            log.info("\tVertices with label '%s': %d",
                     label, len(self.graph.labels[label]))
        failures = self.graph.query_failures()
        if failures:
            log.info("\tQuery failed (reason: count)")
            for reason, count in failures.items():
                log.info("\t\t'%s': %d", reason, count)

    def run_loop(self, vertex_batch, query_count, max_time, verify):
        # start the test
        start_time = last_verify = time.time()

        # initial batched vertex creation
        for _ in range(self.graph.vertex_count // vertex_batch):
            if (time.time() - start_time) / 60 > max_time \
                    or self.executed_queries > query_count:
                break
            self.create_vertices(vertex_batch)
        self.create_vertices(self.graph.vertex_count % vertex_batch)

        # run rest
        while self.executed_queries < query_count:
            now_time = time.time()
            if (now_time - start_time) / 60 > max_time:
                break

            if verify > 0 and (now_time - last_verify) > verify:
                self.verify_graph()
                last_verify = now_time

            ratio_e = len(self.e) / self.graph.edge_count
            ratio_v = len(self.v) / self.graph.vertex_count

            # try to edit vertices globally
            if bernoulli(0.01):
                self.update_global_vertices()

            # try to edit edges globally
            if bernoulli(0.01):
                self.update_global_edges()

            # prefer adding/removing edges whenever there is an edge
            # disbalance and there is enough vertices
            if ratio_v > 0.5 and abs(1 - ratio_e) > 0.2:
                if bernoulli(ratio_e / 2.0):
                    self.remove_edge()
                else:
                    self.create_edge()
                continue

            # if we are near vertex balance, we can also do updates
            # instad of update / deletes
            if abs(1 - ratio_v) < 0.5 and bernoulli(0.5):
                self.add_label()
                continue

            if bernoulli(ratio_v / 2.0):
                self.remove_vertex()
            else:
                self.create_vertices(1)


def runner(params):
    num, args = params
    driver = common.argument_driver(args)
    graph = Graph(args.vertex_count // args.worker_count,
                  args.edge_count // args.worker_count)
    log.info("Starting query runner process")
    session = GraphSession(num, graph, driver.session())
    session.run_loop(args.vertex_batch, args.max_queries // args.worker_count,
                     args.max_time, args.verify)
    log.info("Runner %d executed %d queries", num, session.executed_queries)
    driver.close()


def parse_args():
    argp = common.connection_argument_parser()
    argp.add_argument("--logging", default="INFO",
                      choices=["INFO", "DEBUG", "WARNING", "ERROR"],
                      help="Logging level")
    argp.add_argument("--vertex-count", type=int, required=True,
                      help="The average number of vertices in the graph")
    argp.add_argument("--edge-count", type=int, required=True,
                      help="The average number of edges in the graph")
    argp.add_argument("--vertex-batch", type=int, default=200,
                      help="The number of vertices to be created "
                      "simultaneously")
    argp.add_argument("--prop-count", type=int, default=5,
                      help="The max number of properties on a node")
    argp.add_argument("--max-queries", type=int, default=2 ** 30,
                      help="Maximum number of queries  to execute")
    argp.add_argument("--max-time", type=int, default=2 ** 30,
                      help="Maximum execution time in minutes")
    argp.add_argument("--verify", type=int, default=0,
                      help="Interval (seconds) between checking local info")
    argp.add_argument("--worker-count", type=int, default=1,
                      help="The number of workers that operate on the graph "
                      "independently")
    return argp.parse_args()


def main():
    args = parse_args()
    if args.logging:
        logging.basicConfig(level=args.logging)
        logging.getLogger("neo4j").setLevel(logging.WARNING)
    log.info("Starting Memgraph long running test")

    # cleanup and create indexes
    driver = common.argument_driver(args)
    driver.session().run("MATCH (n) DETACH DELETE n").consume()
    for i in range(args.worker_count):
        label = INDEX_FORMAT.format(i)
        driver.session().run("CREATE INDEX ON :%s(id)" % label).consume()
    driver.close()

    params = [(i, args) for i in range(args.worker_count)]
    with multiprocessing.Pool(args.worker_count) as p:
        p.map(runner, params, 1)

    log.info("All query runners done")


if __name__ == '__main__':
    main()
