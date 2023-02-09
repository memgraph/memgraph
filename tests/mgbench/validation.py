import argparse

import runners
from helpers import generate_workload, list_possible_workloads

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        prog="Validator for individual query checking",
        description="""Validates that query is running, and validates output between different vendors""",
    )

    parser.add_argument(
        "benchmarks",
        nargs="*",
        default="",
        help="descriptions of benchmarks that should be run; "
        "multiple descriptions can be specified to run multiple "
        "benchmarks; the description is specified as "
        "dataset/variant/group/query; Unix shell-style wildcards "
        "can be used in the descriptions; variant, group and query "
        "are optional and they can be left out; the default "
        "variant is '' which selects the default dataset variant; "
        "the default group is '*' which selects all groups; the"
        "default query is '*' which selects all queries",
    )

    args = parser.parse_args()

    vendor = runners.Memgraph(
        "/home/maple/repos/test/memgraph/build/memgraph",
        "/tmp",
        False,
        7687,
        False,
    )

    av = generate_workload("ldbc")

    if True:
        list_possible_workloads()

    client = runners.Client("/home/maple/repos/test/memgraph/build/tests/mgbench/client", "/tmp", 7687)
    vendor.start_benchmark("validation")
    ret = client.execute(queries=[("MATCH (n1)-[M]-(n2) RETURN n1, M , n2;", {})], num_workers=1, validation=True)
    vendor.stop("validation")
    print(ret)
