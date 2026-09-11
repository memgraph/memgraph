import argparse
import json
import re
import time
from abc import ABC, abstractmethod
from multiprocessing import Array, Lock, Manager, Process, Value

import neo4j
from neo4j import GraphDatabase

# psycopg2 (PostgreSQL) and falkordb are optional vendor drivers, imported lazily in their client
# classes so this module loads with only the neo4j driver present, which is all the mgbench venv
# installs. A memgraph run must not fail because an unrelated vendor's driver is absent.

# A query is routed as a write (to main) when it contains a write clause, and as a read otherwise.
# Whole-word match so substrings like OFFSET or a "set"-containing property name are not mistaken
# for the SET clause. Sufficient for the mgbench workloads; a read misrouted to a replica would be
# rejected, so keep this in sync with the clauses the workloads actually emit.
_WRITE_CLAUSE = re.compile(r"\b(CREATE|MERGE|SET|DELETE|REMOVE|DROP|CALL)\b", re.IGNORECASE)

# DDL / schema commands that Memgraph refuses inside an explicit (multicommand) transaction and so
# must run as an implicit auto-commit query even in managed routing (e.g. dataset-import index setup).
_DDL_CLAUSE = re.compile(r"\b(INDEX|CONSTRAINT|TRIGGER)\b", re.IGNORECASE)


def _is_write_query(query):
    return bool(_WRITE_CLAUSE.search(query))


def _is_ddl_query(query):
    return bool(_DDL_CLAUSE.search(query))


class PythonClient(ABC):
    @abstractmethod
    def execute_query(self, query, params):
        pass

    def execute(self, query, params, max_attempts):
        for attempt in range(max_attempts):
            try:
                latency = self.execute_query(query, params)
                return latency, attempt
            except Exception as e:
                if attempt == max_attempts - 1:
                    raise Exception(f"Could not execute query '{query}' {attempt} times! Error message: {e}")
                time.sleep(0.1)  # Brief pause before retrying

    def close(self):
        pass


class FalkorDBClient(PythonClient):
    GRAPH_NAME = "benchmark_graph"
    MILLISECOND_MULTIPLIER = 1000

    def __init__(self, host, port):
        super().__init__()
        from falkordb import FalkorDB

        self._db = FalkorDB(host=host, port=port)
        self._graph = self._db.select_graph(FalkorDBClient.GRAPH_NAME)

    def execute_query(self, query, params):
        start = time.time()
        result = self._graph.query(query, params)
        end = time.time()
        _ = list(result.result_set)  # Force client to pull results
        return (end - start) * self.MILLISECOND_MULTIPLIER


class Neo4jClient(PythonClient):
    # bolt+routing transaction styles (routing_tx_mode):
    #   "managed"  - execute_read/execute_write transaction functions (BEGIN/run/COMMIT), routed by
    #                access mode and retried on transient errors.
    #   "implicit" - auto-commit session.run() with the session's access mode (single RUN+PULL).
    # A FRESH session is opened per query in both modes. A reused session keeps reusing its warm
    # connection under load and stops load-balancing, pinning reads to whichever server it first hit
    # (main, when reads-on-main is enabled); a fresh session re-routes each query so reads actually
    # distribute across the routing table's READ servers. This matches the canonical driver usage in
    # tests/e2e/high_availability/implicit_routing.py.
    ROUTING_TX_MODES = ("managed", "implicit")

    def __init__(self, host, port, user="", password="", routing=False, routing_tx_mode="managed"):
        self._routing = routing
        self._routing_tx_mode = routing_tx_mode
        if routing:
            self._driver = GraphDatabase.driver(f"neo4j://{host}:{port}", auth=(user, password))
        else:
            self._driver = GraphDatabase.driver(f"bolt://{host}:{port}", auth=(user, password))
            self._session = self._driver.session()

    def close(self):
        if not self._routing:
            self._session.close()
        self._driver.close()

    def execute_query(self, query, params=None):
        if self._routing:
            write = _is_write_query(query)
            mode = neo4j.WRITE_ACCESS if write else neo4j.READ_ACCESS
            start = time.time()
            # Fresh session per query so each one re-routes by access mode (readers load-balanced
            # across the routing table's READ servers, writes to main).
            with self._driver.session(default_access_mode=mode) as session:
                if self._routing_tx_mode == "implicit" or (write and _is_ddl_query(query)):
                    # Implicit auto-commit run(). DDL (CREATE/DROP INDEX/CONSTRAINT/TRIGGER) must be
                    # auto-commit even in managed mode (it cannot run in an explicit transaction).
                    session.run(query, parameters=params or {}).consume()
                elif write:
                    session.execute_write(lambda tx: tx.run(query, parameters=params or {}).consume())
                else:
                    session.execute_read(lambda tx: tx.run(query, parameters=params or {}).consume())
            end = time.time()
            return (end - start) * 1000

        start = time.time()
        result = self._session.run(query, parameters=params or {})
        _ = result.consume()
        end = time.time()
        return (end - start) * 1000


class PostgreSQLClient(PythonClient):
    def __init__(self, host, port, user="postgres", password="postgres", database="postgres"):
        import psycopg2
        import psycopg2.extras

        self._conn = psycopg2.connect(host=host, port=port, user=user, password=password, database=database)
        self._conn.autocommit = True
        self._cursor = self._conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def close(self):
        self._cursor.close()
        self._conn.close()

    def execute_query(self, query, params=None):
        start = time.time()
        self._cursor.execute(query, params or {})

        # Only fetch results if the query returns rows
        if self._cursor.description:
            _ = self._cursor.fetchall()

        end = time.time()
        return (end - start) * 1000


def get_python_client(vendor):
    if vendor == "memgraph" or vendor == "neo4j":
        return Neo4jClient
    if vendor == "falkordb":
        return FalkorDBClient
    if vendor == "postgresql":
        return PostgreSQLClient
    raise Exception(f"Unknown vendor {vendor} when running benchmarks with a Python client!")


def _make_client(vendor, host, port, routing, routing_tx_mode):
    # Only the neo4j/bolt client speaks bolt+routing; other vendors ignore the flags.
    client_cls = get_python_client(vendor)
    if client_cls is Neo4jClient:
        return client_cls(host, port, routing=routing, routing_tx_mode=routing_tx_mode)
    return client_cls(host, port)


def execute_validation_task(
    worker_id,
    vendor,
    host,
    port,
    queries,
    position,
    lock,
    results,
    durations,
    max_retries,
    time_limit,
    routing,
    routing_tx_mode,
):
    # The method uses same set of arguments so it can be called with multiple workers with the same pattern
    # For this reason, in this function we will not use the following argumetns:
    # - Time limit: Validation queries are performed which are independent from timed execution
    # - Position and lock: There is no synchronization needed as validation query is the sole query needed
    #   to be executed.
    client = _make_client(vendor, host, port, routing, routing_tx_mode)

    if len(queries) != 1:
        raise Exception("Validation query should be performed with only one query!")

    query_start_time = time.time()
    query, params = queries[0]
    latency, attempts = client.execute(query, params, max_retries)
    query_end_time = time.time()

    duration = query_end_time - query_start_time
    results[worker_id] = {
        "worker": worker_id,
        "latencies": latency,
        "attempts": attempts,
        "durations": [duration],
    }

    durations[0] = duration
    client.close()


def execute_queries_task(
    worker_id,
    vendor,
    host,
    port,
    queries,
    position,
    lock,
    results,
    durations,
    max_retries,
    time_limit,
    routing,
    routing_tx_mode,
):
    # The method uses same set of arguments so it can be called with multiple workers with the same pattern
    # For this reason, in this function we will not use the following argumetns:
    # - Time limit: This task is independent from timed execution as every query will be executed only once
    client = _make_client(vendor, host, port, routing, routing_tx_mode)

    size = len(queries)

    aggregate = {
        "worker": worker_id,
        "latencies": [],
        "attempts": 0,
        "durations": [],
    }

    worker_timer_start = time.time()
    while True:
        with lock:
            # Every query needs to be executed only once. For this reason, we're protecting the increment
            # of the value with the lock
            pos = position.value
            if pos >= size:
                worker_timer_end = time.time()
                durations[worker_id] = worker_timer_end - worker_timer_start
                results[worker_id] = aggregate
                client.close()
                return

            position.value += 1

        query, params = queries[pos]

        query_time_start = time.time()
        latency, attempts = client.execute(query, params, max_retries)
        query_time_end = time.time()

        query_duration = query_time_end - query_time_start
        aggregate["durations"].append(query_duration)
        aggregate["latencies"].append(latency)
        aggregate["attempts"] += attempts


def execute_time_dependent_task(
    worker_id,
    vendor,
    host,
    port,
    queries,
    position,
    lock,
    results,
    durations,
    max_retries,
    time_limit,
    routing,
    routing_tx_mode,
):
    client = _make_client(vendor, host, port, routing, routing_tx_mode)

    size = len(queries)

    aggregate = {
        "worker": worker_id,
        "latencies": [],
        "attempts": 0,
        "durations": [],
    }

    workload_start_time = time.time()
    worker_start_time = time.time()
    while time.time() - workload_start_time < time_limit:
        with lock:
            pos = position.value
            if pos >= size:
                pos = 0
                position.value = 1

        query, params = queries[pos]
        query_start_time = time.time()
        latency, attempts = client.execute(query, params, max_retries)
        query_end_time = time.time()

        query_duration = query_end_time - query_start_time
        aggregate["durations"].append(query_duration)
        aggregate["latencies"].append(latency)
        aggregate["attempts"] += attempts

        worker_end_time = time.time()
        durations[worker_id] = worker_end_time - worker_start_time

    results[worker_id] = aggregate
    client.close()


def execute_workload(queries, args):
    num_workers = args.num_workers
    validation = args.validation
    time_limit = args.time_dependent_execution
    manager = Manager()

    results = manager.list([[] for _ in range(num_workers)])
    durations = Array("d", [0.0] * num_workers)

    lock = Lock()
    position = Value("i", 0)

    task = execute_queries_task
    if validation is True:
        task = execute_validation_task
        num_workers = 1
    elif time_limit > 0:
        task = execute_time_dependent_task

    processes = []
    for worker_id in range(num_workers):
        process = Process(
            target=task,
            args=(
                worker_id,
                args.vendor,
                args.host,
                args.port,
                queries,
                position,
                lock,
                results,
                durations,
                args.max_retries,
                time_limit,
                args.routing,
                args.routing_tx_mode,
            ),
        )
        process.start()
        processes.append(process)

    for process in processes:
        process.join()

    return results, durations


def calculate_latency_statistics(results):
    if not results:
        return {}

    query_durations = []
    for result in results:
        for duration in result["durations"]:
            query_durations.append(duration)

    query_durations.sort()
    total = len(query_durations)
    return {
        "iterations": total,
        "min": query_durations[0],
        "max": query_durations[-1],
        "mean": sum(query_durations) / total,
        "p99": query_durations[int(total * 0.99) - 1],
        "p95": query_durations[int(total * 0.95) - 1],
        "p90": query_durations[int(total * 0.90) - 1],
        "p75": query_durations[int(total * 0.75) - 1],
        "p50": query_durations[int(total * 0.50) - 1],
    }


def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ("yes", "true", "t", "1"):
        return True
    elif v.lower() in ("no", "false", "f", "0"):
        return False
    else:
        raise argparse.ArgumentTypeError("Boolean value expected.")


def main():
    parser = argparse.ArgumentParser(description="FalkorDB Concurrent Query Executor")
    parser.add_argument("--vendor", default="memgraph", help="Vendor name for the python client")
    parser.add_argument("--host", default="localhost", help="Database server host")
    parser.add_argument("--port", type=int, default=7687, help="Database server port")
    parser.add_argument("--username", default="", help="Username for the database")
    parser.add_argument("--password", default="", help="Password for the database")
    parser.add_argument("--num-workers", type=int, default=1, help="Number of worker threads")
    parser.add_argument("--max-retries", type=int, default=50, help="Maximum number of retries for each query")
    parser.add_argument(
        "--routing",
        type=str2bool,
        nargs="?",
        const=True,
        default=False,
        help="Connect with bolt+routing (neo4j://) to a coordinator and route reads/writes by access "
        "mode, instead of a direct bolt:// connection to a single instance.",
    )
    parser.add_argument(
        "--routing-tx-mode",
        type=str,
        default="managed",
        choices=Neo4jClient.ROUTING_TX_MODES,
        help="bolt+routing transaction style: 'managed' (execute_read/execute_write transaction "
        "functions) or 'implicit' (auto-commit session.run() with a manually set session access "
        "mode). Only used with --routing.",
    )
    parser.add_argument("--input", default="", help="Input file containing queries in JSON format")
    parser.add_argument("--output", default="", help="Output file to write results in JSON format")
    parser.add_argument(
        "--time-dependent-execution",
        type=int,
        default=0,
        help="Execute queries for the specified number of seconds. If set to 0, execute all queries once.",
    )
    parser.add_argument(
        "--queries-json",
        type=str2bool,
        nargs="?",
        const=True,
        default=False,
        help="""
        Set to true to load all queries as as single JSON encoded list. Each item in the list
        should contain another list whose first element is the query that should be executed and the
        second element should be a dictionary of query parameters for that query.
        """,
    )
    parser.add_argument(
        "--validation",
        type=str2bool,
        nargs="?",
        const=True,
        default=False,
        help="""
        Set to true to run client in validation mode.
        Validation mode works for singe query and returns results for validation
        with metadata
        """,
    )

    args = parser.parse_args()

    queries = []

    infile = open(args.input, "r")
    content = infile.read().strip()
    infile.close()

    if not content:
        print("Error: Input file is empty. Exiting.")
        exit(1)

    if args.queries_json:  # Load as JSON
        try:
            for line in content.split("\n"):
                stripped = line.strip()
                queries.append(json.loads(stripped))
            if not isinstance(queries, list):
                raise ValueError("Expected a JSON list of queries.")
        except json.JSONDecodeError as e:
            print(f"Error: Invalid JSON format in input file.\n{e}")
            exit(1)
    else:  # Read Cypher queries line by line
        for line in content.split("\n"):
            stripped = line.strip()
            if stripped == "" or stripped == ";":
                execute_workload(queries, args)
                queries.clear()
                continue
            queries.append([stripped, {}])

    if not queries:
        print("Error: No queries provided. Exiting.")
        exit(1)

    results, durations = execute_workload(queries, args)
    results = list(results)
    durations = list(durations)

    final_duration = 0.0
    for duration in durations:
        final_duration += duration

    final_retries = 0
    for worker_result in results:
        final_retries += worker_result["attempts"]

    count = len(queries)
    final_duration = final_duration / args.num_workers
    total_iterations = sum([len(x["latencies"]) for x in results])
    if args.time_dependent_execution > 0:
        execution_delta = args.time_dependent_execution / final_duration
        throughput = (total_iterations / final_duration) * execution_delta
        raw_throughput = total_iterations / final_duration
    else:
        throughput = raw_throughput = count / final_duration

    latency_stats = calculate_latency_statistics(results)

    summary = {
        "count": count,
        "duration": final_duration,
        "latency_stats": latency_stats,
        "metadata": {},
        "num_workers": args.num_workers,
        "queries_executed": total_iterations,
        "raw_throughput": raw_throughput,
        "results": results,
        "retries": final_retries,
        "throughput": throughput,
        "time_limit": args.time_dependent_execution,
    }

    if args.output:
        with open(args.output, "w") as outfile:
            json.dump(summary, outfile, indent=2)
    else:
        try:
            print(json.dumps(summary))
        except Exception as e:
            print("Error happened when dumping results!")


if __name__ == "__main__":
    main()
