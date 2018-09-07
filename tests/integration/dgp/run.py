#!/usr/bin/python3

'''
Test dynamic graph partitioner on Memgraph cluster
with randomly generated graph (uniform distribution
of nodes and edges). The partitioning goal is to
minimize number of crossing edges while keeping
the cluster balanced.
'''

import argparse
import atexit
import logging
import os
import random
import sys
import subprocess
import time

try:
    # graphviz==0.9
    from graphviz import Digraph
except ImportError:
    print("graphviz module isn't available. "
          "Graph won't be generated but the checks will still work.")
from neo4j.v1 import GraphDatabase, basic_auth

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "..", ".."))
COMMON_ARGS = ["--durability-enabled=false",
               "--snapshot-on-exit=false",
               "--db-recover-on-startup=false"]
MASTER_ARGS = ["--master",
               "--master-port", "10000",
               "--dynamic-graph-partitioner-enabled",
               "--durability-directory=durability_master"]

log = logging.getLogger(__name__)
memgraph_processes = []


def worker_args(worker_id):
    args = ["--worker",
            "--worker-id", str(worker_id),
            "--worker-port", str(10000 + worker_id),
            "--master-port", str(10000),
            "--durability-directory=durability_worker%s" % worker_id]
    return args


def wait_for_server(port, delay=0.01):
    cmd = ["nc", "-z", "-w", "1", "127.0.0.1", port]
    while subprocess.call(cmd) != 0:
        time.sleep(delay)
    time.sleep(delay)


def run_memgraph_process(binary, args):
    global memgraph_processes
    process = subprocess.Popen([binary] + args, cwd=os.path.dirname(binary))
    memgraph_processes.append(process)


def run_distributed_memgraph(args):
    run_memgraph_process(args.memgraph, COMMON_ARGS + MASTER_ARGS)
    wait_for_server("10000")
    for i in range(1, int(args.machine_count)):
        run_memgraph_process(args.memgraph, COMMON_ARGS + worker_args(i))
    wait_for_server("7687")


def terminate():
    global memgraph_processes
    for process in memgraph_processes:
        process.terminate()
        status = process.wait()
        if status != 0:
            raise Exception(
                "Memgraph binary returned non-zero ({})!".format(status))


@atexit.register
def cleanup():
    global memgraph_processes
    for proc in memgraph_processes:
        if proc.poll() is not None:
            continue
        proc.kill()
        proc.wait()


def run_test(args):
    driver = GraphDatabase.driver(args.endpoint,
                                  auth=basic_auth(args.username,
                                                  args.password),
                                  encrypted=args.encrypted)
    session = driver.session()

    session.run("CREATE INDEX ON :Node(id)").consume()
    session.run(
        "UNWIND range(0, $num - 1) AS n CREATE (:Node {id: n})",
        num=args.node_count - 1).consume()

    created_edges = 0
    while created_edges <= args.edge_count:
        # Retry block is here because DGP is running in background and
        # serialization errors may occure.
        try:
            session.run("MATCH (n:Node {id: $id1}), (m:Node {id:$id2}) "
                        "CREATE (n)-[:Edge]->(m)",
                        id1=random.randint(0, args.node_count - 1),
                        id2=random.randint(0, args.node_count - 1)).consume()
            created_edges += 1
        except Exception:
            pass

    # Check cluster state periodically.
    crossing_edges_history = []
    load_history = []
    duration = 0
    for iteration in range(args.iteration_count):
        iteration_start_time = time.time()
        data = session.run(
            "MATCH (n)-[r]->(m) "
            "RETURN "
            "    id(n) AS v1, id(m) AS v2, "
            "    workerid(n) AS position_n, workerid(m) AS position_m").data()

        # Visualize cluster state.
        if args.visualize and 'graphviz' in sys.modules:
            output_dir = os.path.join(SCRIPT_DIR, "output")
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            cluster = Digraph(name="memgraph_cluster", format="png")
            cluster.attr(splines="false", rank="TB")
            subgraphs = [Digraph(name="cluster_worker%s" % i)
                         for i in range(args.machine_count)]
            for index, subgraph in enumerate(subgraphs):
                subgraph.attr(label="worker%s" % index)
            edges = []
            for row in data:
                #             start_id   end_id     machine_id
                edges.append((row["v1"], row["v2"], row["position_n"]))
            edges = sorted(edges, key=lambda x: x[0])
            for edge in edges:
                #         machine_id        start_id      end_id
                subgraphs[edge[2]].edge(str(edge[0]), str(edge[1]))
            for subgraph in subgraphs:
                cluster.subgraph(subgraph)
            cluster.render("output/iteration_%s" % iteration)

        # Collect data.
        num_of_crossing_edges = 0
        load = [0] * args.machine_count
        for edge in data:
            src_worker = int(edge["position_n"])
            dst_worker = int(edge["position_m"])
            if src_worker != dst_worker:
                num_of_crossing_edges = num_of_crossing_edges + 1
            load[src_worker] = load[src_worker] + 1
        crossing_edges_history.append(num_of_crossing_edges)
        load_history.append(load)
        iteration_delta_time = time.time() - iteration_start_time
        duration += iteration_delta_time
        log.info("Number of crossing edges %s" % num_of_crossing_edges)
        log.info("Cluster load %s" % load)

        # Wait for DGP a bit.
        if iteration_delta_time < args.iteration_interval:
            time.sleep(args.iteration_interval - iteration_delta_time)
            duration += args.iteration_interval
        else:
            duration += iteration_delta_time
        # TODO (buda): Somehow align with DGP turns. Replace runtime param with
        #              the query.

    assert crossing_edges_history[-1] < crossing_edges_history[0], \
        "Number of crossing edges is equal or bigger."
    for machine in range(args.machine_count):
        assert load_history[-1][machine] > 0, "Machine %s is empty." % machine
    log.info("Config")
    log.info("    Machines: %s" % args.machine_count)
    log.info("    Nodes: %s" % args.node_count)
    log.info("    Edges: %s" % args.edge_count)
    log.info("Start")
    log.info("    Crossing Edges: %s" % crossing_edges_history[0])
    log.info("    Cluster Load: %s" % load_history[0])
    log.info("End")
    log.info("    Crossing Edges: %s" % crossing_edges_history[-1])
    log.info("    Cluster Load: %s" % load_history[-1])
    log.info("Stats")
    log.info("    Duration: %ss" % duration)

    session.close()
    driver.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    memgraph_binary = os.path.join(PROJECT_DIR, "build", "memgraph")
    if not os.path.exists(memgraph_binary):
        memgraph_binary = os.path.join(PROJECT_DIR, "build_debug", "memgraph")

    parser = argparse.ArgumentParser()
    parser.add_argument("--memgraph", default=memgraph_binary)
    parser.add_argument("--endpoint", type=str,
                        default="bolt://localhost:7687")
    parser.add_argument("--username", type=str, default="")
    parser.add_argument("--password", type=str, default="")
    parser.add_argument("--encrypted", type=bool, default=False)
    parser.add_argument("--visualize", type=bool, default=False)
    parser.add_argument("--machine-count", type=int, default=3)
    parser.add_argument("--node-count", type=int, default=1000000)
    parser.add_argument("--edge-count", type=int, default=1000000)
    parser.add_argument("--iteration-count", type=int, default=10)
    parser.add_argument("--iteration-interval", type=float, default=5)
    args = parser.parse_args()

    run_distributed_memgraph(args)
    run_test(args)
    terminate()
