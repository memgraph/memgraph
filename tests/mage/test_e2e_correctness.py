#!/usr/bin/env python3

import argparse
import os
import subprocess
import sys

WORK_DIRECTORY = os.getcwd()
E2E_CORRECTNESS_DIRECTORY = f"{WORK_DIRECTORY}/e2e_correctness"


class ConfigConstants:
    NEO4J_PORT = 7688
    MEMGRAPH_PORT = 7687
    NEO4J_CONTAINER_NAME = "neo4j"


def parse_arguments():
    parser = argparse.ArgumentParser(description="Test MAGE E2E correctness.")
    parser.add_argument(
        "-k",
        help="Filter what tests you want to run",
        type=str,
        required=False,
    )
    parser.add_argument(
        "--memgraph-port",
        help="Set the port that Memgraph is listening on",
        type=int,
        required=False,
    )
    parser.add_argument(
        "--neo4j-port",
        help="Set the port that Neo4j is listening on",
        type=int,
        required=False,
    )
    parser.add_argument(
        "--neo4j-container",
        help="Set the Neo4j container name",
        type=str,
        required=False,
    )
    args = parser.parse_args()
    return args


#################################################
#                End to end tests               #
#################################################


def main(
    test_filter: str = None,
    memgraph_port: str = str(ConfigConstants.MEMGRAPH_PORT),
    neo4j_port: str = str(ConfigConstants.NEO4J_PORT),
    neo4j_container: str = ConfigConstants.NEO4J_CONTAINER_NAME,
):
    os.environ["PYTHONPATH"] = E2E_CORRECTNESS_DIRECTORY
    os.chdir(E2E_CORRECTNESS_DIRECTORY)
    command = ["python3", "-m", "pytest", ".", "-vv"]
    if test_filter:
        command.extend(["-k", test_filter])

    command.extend(["--memgraph-port", memgraph_port])
    command.extend(["--neo4j-port", neo4j_port])
    command.extend(["--neo4j-container", neo4j_container])

    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")
        sys.exit(e.returncode)


if __name__ == "__main__":
    args = parse_arguments()
    test_filter = args.k
    memgraph_port = args.memgraph_port
    neo4j_port = args.neo4j_port
    neo4j_container = args.neo4j_container

    if memgraph_port:
        memgraph_port = str(memgraph_port)
    if neo4j_port:
        neo4j_port = str(neo4j_port)
    if args.neo4j_container:
        neo4j_container = args.neo4j_container

    main(
        test_filter=test_filter,
        memgraph_port=memgraph_port,
        neo4j_port=neo4j_port,
        neo4j_container=neo4j_container,
    )
