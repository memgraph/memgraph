import argparse
import json
import time
from multiprocessing import Array, Lock, Manager, Process, Queue, Value
from falkordb import FalkorDB


GRAPH_NAME = "social"


def execute_query(graph, query, params, max_attempts):
    for attempt in range(max_attempts):
        try:
            result = graph.query(query, params)
            return result, attempt
        except Exception as e:
            if attempt >= max_attempts - 1:
                raise Exception(f"Could not execute query '{query}' {attempt} times! Error message: {e}")
            time.sleep(0.1)  # Brief pause before retrying


def execute_validation_task(
    worker_id, host, port, queries, position, lock, results, durations, max_retries, time_limit
):
    db = FalkorDB(host=host, port=port)
    graph = db.select_graph(GRAPH_NAME)

    if len(queries) != 1:
        raise Exception("Validation query should be performed with only one query!")

    query_start_time = time.time()
    query, params = queries[0]
    result, attempts = execute_query(graph, query, params, max_retries)
    query_end_time = time.time()

    duration = query_end_time - query_start_time
    results[worker_id] = {
        "worker": worker_id,
        "latencies": [result.run_time_ms],
        "attempts": attempts,
        "durations": [duration],
    }

    durations[0] = duration


def execute_queries_task(worker_id, host, port, queries, position, lock, results, durations, max_retries, time_limit):
    db = FalkorDB(host=host, port=port)
    graph = db.select_graph(GRAPH_NAME)

    size = len(queries)
    pos = 0

    aggregate = {
        "worker": worker_id,
        "latencies": [],
        "attempts": 0,
        "durations": [],
    }

    worker_timer_start = time.time()
    while True:
        with lock:
            pos = position.value
            if pos >= size:
                worker_timer_end = time.time()
                durations[worker_id] = worker_timer_end - worker_timer_start
                results[worker_id] = aggregate
                return

            position.value += 1

        query, params = queries[pos]

        query_time_start = time.time()
        result, attempts = execute_query(graph, query, params, max_retries)
        query_time_end = time.time()

        query_duration = query_time_end - query_time_start
        aggregate["durations"].append(query_duration)
        aggregate["latencies"].append(result.run_time_ms)
        aggregate["attempts"] += attempts


def execute_time_dependent_task(
    worker_id, host, port, queries, position, lock, results, durations, max_retries, time_limit
):
    db = FalkorDB(host=host, port=port)
    graph = db.select_graph(GRAPH_NAME)

    size = len(queries)
    pos = 0

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
                position.value = 0
                position.value += 1

        query, params = queries[pos]
        query_start_time = time.time()
        result, attempts = execute_query(graph, query, params, max_retries)
        query_end_time = time.time()

        query_duration = query_end_time - query_start_time
        aggregate["durations"].append(query_duration)
        aggregate["latencies"].append(result.run_time_ms)
        aggregate["attempts"] += attempts

        worker_end_time = time.time()
        durations[worker_id] = worker_end_time - worker_start_time

    results[worker_id] = aggregate


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
        "time_limit": args.time_dependent_execution,
        "queries_executed": total_iterations,
        "throughput": throughput,
        "raw_throughput": raw_throughput,
        "latency_stats": latency_stats,
        "retries": final_retries,
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
