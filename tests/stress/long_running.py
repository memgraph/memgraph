#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
A long running test that performs
random CRUD ops on a bolt database.

Parameterized with vertex and edge counts around which
the graph state oscilates.
"""

import logging
import random
import time
from uuid import uuid4
from threading import Lock, Thread
from contextlib import contextmanager
from collections import defaultdict

import common

log = logging.getLogger(__name__)

# the label in the database that is indexed
# used for matching vertices faster
INDEXED_LABEL = "indexed_label"


def rint(upper_exclusive):
    return random.randint(0, upper_exclusive - 1)


def bernoulli(p):
    return random.random() < p


def random_id():
    return str(uuid4())


class QueryExecutionSynchronizer():
    """
    Fascilitates running a query with not other queries being
    concurrently executed.

    Exposes a count of how many queries in total have been
    executed through `count_total`.
    """

    def __init__(self, sleep_time=0.2):
        """
        Args:
            sleep_time - Sleep time while awaiting execution rights
        """
        self.count_total = 0

        self._lock = Lock()
        self._count = 0
        self._can_run = True
        self._sleep_time = sleep_time

    @contextmanager
    def run(self):
        """
        Provides a context for running a query without isolation.
        Isolated queries can't be executed while such a context exists.
        """
        while True:
            with self._lock:
                if self._can_run:
                    self._count += 1
                    self.count_total += 1
                    break
            time.sleep(self._sleep_time)

        try:
            yield
        finally:
            with self._lock:
                self._count -= 1

    @contextmanager
    def run_isolated(self):
        """
        Provides a context for runnig a query with isolation. Prevents
        new queries from executing. Waits till the currently executing
        queries are done. Once this context exits execution can
        continue.
        """
        with self._lock:
            self._can_run = False

        while True:
            with self._lock:
                if self._count == 0:
                    break
            time.sleep(self._sleep_time)

        with self._lock:
            try:
                yield
            finally:
                self._can_run = True


class LabelCounter():
    """ Encapsulates a label and a thread-safe counter """

    def __init__(self, label):
        self.label = label
        self._count = 0
        self._lock = Lock()

    def increment(self):
        with self._lock:
            self._count += 1

    def decrement(self):
        with self._lock:
            self._count -= 1


class ThreadSafeList():
    """ Provides a thread-safe access to a list for a few functionalities. """

    def __init__(self):
        self._list = []
        self._lock = Lock()

    def append(self, element):
        with self._lock:
            self._list.append(element)

    def remove(self, element):
        with self._lock:
            self._list.remove(element)

    def random(self):
        with self._lock:
            return self._list[rint(len(self._list))]

    def __len__(self):
        with self._lock:
            return len(self._list)


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

        self.query_execution_synchronizer = QueryExecutionSynchronizer()

        # storage
        self.edges = ThreadSafeList()
        self.vertices = ThreadSafeList()
        self.labels = {"label%d" % i: ThreadSafeList() for i in range(labels)}

        # info about query failures, maps exception string representations into
        # occurence counts
        self._query_failure_counts = defaultdict(int)
        self._query_failure_counts_lock = Lock()

    def add_query_failure(self, reason):
        with self._query_failure_counts_lock:
            self._query_failure_counts[reason] += 1

    def query_failures(self):
        with self._query_failure_counts_lock:
            return dict(self._query_failure_counts)


class GraphSession():
    """
    Encapsulates a Graph and a Bolt session and provides CRUD op functions.
    Also defines a run-loop for a generic exectutor, and a graph state
    verification function.
    """

    def __init__(self, graph, session):
        self.graph = graph
        self.session = session
        self._start_time = time.time()

    @property
    def v(self):
        return self.graph.vertices

    @property
    def e(self):
        return self.graph.edges

    def execute_basic(self, query):
        log.debug("Executing query: %s", query)
        try:
            return self.session.run(query).data()
        except Exception as e:
            self.graph.add_query_failure(str(e))
            return None

    def execute(self, query):
        with self.graph.query_execution_synchronizer.run():
            return self.execute_basic(query)

    def create_vertex(self):
        vertex_id = random_id()
        self.execute("CREATE (:%s {id: %r})" % (INDEXED_LABEL, vertex_id))
        self.v.append(vertex_id)

    def remove_vertex(self):
        vertex_id = self.v.random()
        result = self.execute(
            "MATCH (n:%s {id: %r}) OPTIONAL MATCH (n)-[r]-() "
            "DETACH DELETE n RETURN n.id, labels(n), r.id" % (INDEXED_LABEL, vertex_id))
        if result:
            process_vertex_ids = set()
            for row in result:
                # remove vertex but note there could be duplicates
                vertex_id = row['n.id']
                if vertex_id not in process_vertex_ids:
                    process_vertex_ids.add(vertex_id)
                    self.v.remove(vertex_id)
                    for label in row['labels(n)']:
                        if (label != INDEXED_LABEL):
                            self.graph.labels[label].remove(vertex_id)
                # remove edge
                edge_id = row['r.id']
                if edge_id:
                    self.e.remove(edge_id)

    def create_edge(self):
        eid = random_id()
        creation = self.execute(
            "MATCH (from:%s {id: %r}), (to:%s {id: %r}) "
            "CREATE (from)-[e:EdgeType {id: %r}]->(to) RETURN e" % (
                INDEXED_LABEL, self.v.random(), INDEXED_LABEL, self.v.random(), eid))
        if creation:
            self.e.append(eid)

    def remove_edge(self):
        edge_id = self.e.random()
        result = self.execute(
            "MATCH ()-[e {id: %r}]->() DELETE e RETURN e.id" % edge_id)
        if result:
            self.e.remove(edge_id)

    def add_label(self):
        vertex_id = self.v.random()
        label = random.choice(list(self.graph.labels.keys()))
        # add a label on a vertex that didn't have that label
        # yet (we need that for book-keeping)
        result = self.execute(
            "MATCH (v {id: %r}) WHERE not v:%s SET v:%s RETURN v.id" % (
                vertex_id, label, label))
        if result:
            self.graph.labels[label].append(vertex_id)

    def verify_graph(self):
        """ Checks if the local info corresponds to DB state """

        def test(a, b, message):
            assert set(a) == set(b), message % (len(a), len(b))

        def get(query, key):
            return [row[key] for row in self.execute_basic(query)]

        # graph state verification must be run in isolation
        with self.graph.query_execution_synchronizer.run_isolated():
            test(self.v._list, get("MATCH (n) RETURN n.id", "n.id"),
                 "Expected %d vertices, found %d")
            test(self.e._list, get("MATCH ()-[r]->() RETURN r.id", "r.id"),
                 "Expected %d edges, found %d")
            for lab, exp in self.graph.labels.items():
                test(get("MATCH (n:%s) RETURN n.id" % lab, "n.id"), exp._list,
                     "Expected %d vertices with label '{}', found %d".format(
                        lab))

            log.info("Graph verification success:")
            log.info("\tExecuted %d queries in %.2f seconds",
                     self.graph.query_execution_synchronizer.count_total,
                     time.time() - self._start_time)
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

    def run_loop(self, query_count, max_time):
        start_time = time.time()
        for _ in range(query_count):
            if (time.time() - start_time) / 60 > max_time:
                break

            ratio_e = len(self.e) / self.graph.edge_count
            ratio_v = len(self.v) / self.graph.vertex_count

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
                self.create_vertex()


def parse_args():
    argp = common.connection_argument_parser()
    argp.add_argument("--logging", default="INFO",
                      choices=["INFO", "DEBUG", "WARNING", "ERROR"],
                      help="Logging level")
    argp.add_argument("--vertex-count", type=int, required=True,
                      help="The average number of vertices in the graph")
    argp.add_argument("--edge-count", type=int, required=True,
                      help="The average number of edges in the graph")
    argp.add_argument("--prop-count", type=int, default=5,
                      help="The max number of properties on a node")
    argp.add_argument("--max-queries", type=int, default=2 ** 30,
                      help="Maximum number of queries  to execute")
    argp.add_argument("--max-time", type=int, default=2 ** 30,
                      help="Maximum execution time in minutes")
    argp.add_argument("--verify", type=int, default=0,
                      help="Interval (seconds) between checking local info")
    argp.add_argument("--thread-count", type=int, default=1,
                      help="The number of threads that operate on the graph"
                      "independently")
    return argp.parse_args()


def main():
    args = parse_args()
    if args.logging:
        logging.basicConfig(level=args.logging)
        logging.getLogger("neo4j").setLevel(logging.WARNING)
    log.info("Starting Memgraph long running test")

    graph = Graph(args.vertex_count, args.edge_count)
    driver = common.argument_driver(args)

    # cleanup
    driver.session().run("MATCH (n) DETACH DELETE n").consume()
    driver.session().run("CREATE INDEX ON :%s(id)" % INDEXED_LABEL).consume()

    if args.verify > 0:
        log.info("Creating veification session")
        verififaction_session = GraphSession(graph, driver.session())
        common.periodically_execute(verififaction_session.verify_graph, (),
                                    args.verify)
    # TODO better verification failure handling

    threads = []
    for _ in range(args.thread_count):
        log.info("Creating query runner thread")
        session = GraphSession(graph, driver.session())
        thread = Thread(target=session.run_loop,
                        args=(args.max_queries // args.thread_count,
                              args.max_time),
                        daemon=True)
        threads.append(thread)
    list(map(Thread.start, threads))

    list(map(Thread.join, threads))
    driver.close()
    log.info("All query runners done")


if __name__ == '__main__':
    main()
