import argparse
import json
import time
from multiprocessing import Process, Manager, Lock, Value
from falkordb import FalkorDB


GRAPH_NAME = "social"


def execute_query(graph, query, params, max_attempts):
    for attempt in range(max_attempts):
        try:
            result = graph.query(query, params)
            return result, attempt
        except Exception as e:
            if attempt == max_attempts - 1:
                # print(f"Failed to execute query '{query}' after {max_attempts} attempts. Error: {e}")
                return None, None
            time.sleep(0.1)  # Brief pause before retrying


def execute_validation_task(worker_id, host, port, queries, position, lock, results, durations, max_retries, time_limit):
    db = FalkorDB(host=host, port=port)
    graph = db.select_graph(GRAPH_NAME)

    if len(queries) == 1:
        query, params = queries[0]
        start_time = time.time()
        result, attempts = execute_query(graph, query, params, max_retries)
        duration = time.time() - start_time
        if result is not None:
            results.append(
                {
                    "worker": worker_id,
                    "query": query,
                    "params": params,
                    "latency": result.run_time_ms,
                    "attempts": attempts,
                    "duration": duration,
                }
            )
            durations.append(duration)
    else:
        raise Exception("Validation query should be performed with only one query!")


def execute_queries_task(worker_id, host, port, queries, position, lock, results, durations, max_retries, time_limit):
    db = FalkorDB(host=host, port=port)
    graph = db.select_graph(GRAPH_NAME)

    size = len(queries)
    pos = 0
    
    while True:
        with lock:
            pos = position.value
            if pos >= size:
                return
            position.value += 1

        query, params = queries[pos]
        start_time = time.time()
        result, attempts = execute_query(graph, query, params, max_retries)
        duration = time.time() - start_time
        if result is not None:
            results.append(
                {
                    "worker": worker_id,
                    "query": query,
                    "params": params,
                    "latency": result.run_time_ms,
                    "attempts": attempts,
                    "duration": duration,
                }
            )
            durations.append(duration)


def execute_time_dependent_task(worker_id, host, port, queries, position, lock, results, durations, max_retries, time_limit):
    db = FalkorDB(host=host, port=port)
    graph = db.select_graph(GRAPH_NAME)

    size = len(queries)
    pos = 0

    workload_start_time = time.time()
    while time.time() - workload_start_time < time_limit:
        with lock:
            pos = position.value
            if pos >= size:
                pos = 0
                position.value = 0
                position.value += 1

        query, params = queries[pos]
        start_time = time.time()
        result, attempts = execute_query(graph, query, params, max_retries)
        duration = time.time() - start_time
        if result is not None:
            results.append(
                {
                    "worker": worker_id,
                    "query": query,
                    "params": params,
                    "latency": result.run_time_ms,
                    "attempts": attempts,
                    "duration": duration,
                }
            )
            durations.append(duration)


def execute_workload(queries, args):
    manager = Manager()
    results = manager.list()
    durations = manager.list()
    validation = args.validation
    time_limit = args.time_dependent_execution
    num_workers = args.num_workers

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
                args.host,
                args.port,
                queries,
                position,
                lock,
                results,
                durations,
                args.max_retries,
                time_limit,
            ),
        )
        process.start()
        processes.append(process)

    for process in processes:
        process.join()

    return results, durations


def calculate_latency_statistics(durations):
    if not durations:
        return {}
    durations.sort()
    total = len(durations)
    return {
        "iterations": total,
        "min": durations[0],
        "max": durations[-1],
        "mean": sum(durations) / total,
        "p99": durations[int(total * 0.99) - 1],
        "p95": durations[int(total * 0.95) - 1],
        "p90": durations[int(total * 0.90) - 1],
        "p75": durations[int(total * 0.75) - 1],
        "p50": durations[int(total * 0.50) - 1],
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
    parser.add_argument("--host", default="localhost", help="FalkorDB server host")
    parser.add_argument("--port", type=int, default=6379, help="FalkorDB server port")
    parser.add_argument("--username", default="", help="Username for the database")
    parser.add_argument("--password", default="", help="Password for the database")
    parser.add_argument("--num-workers", type=int, default=1, help="Number of worker threads")
    parser.add_argument("--max-retries", type=int, default=50, help="Maximum number of retries for each query")
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

    start_time = time.time()

    results, durations = execute_workload(queries, args)
    results = list(results)
    durations = list(durations)

    total_time = time.time() - start_time

    latency_stats = calculate_latency_statistics(durations)
    throughput = len(results) / total_time if total_time > 0 else 0

    summary = {
        "duration": total_time,
        "count": len(queries),
        "queries_executed": len(results),
        "throughput": throughput,
        "latency_stats": latency_stats,
        "retries": sum([x["attempts"] for x in results]),
        "num_workers": args.num_workers,
        "results": results,
    }

    if args.output:
        with open(args.output, "w") as outfile:
            json.dump(summary, outfile)
    else:
        try:
            print(json.dumps(summary))
        except Exception as e:
            print("Error happened when dumping results!")


if __name__ == "__main__":
    main()
