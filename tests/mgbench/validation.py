import argparse
import collections
import copy
import fnmatch
import inspect
import random
import sys
import time

import helpers
import importer
import runners
import workload
from workload import dataset


def get_queries(gen, count):
    # Make the generator deterministic.
    random.seed(gen.__name__)
    # Generate queries.
    ret = []
    for i in range(count):
        ret.append(gen())
    return ret


def match_patterns(dataset, variant, group, query, is_default_variant, patterns):
    for pattern in patterns:
        verdict = [fnmatch.fnmatchcase(dataset, pattern[0])]
        if pattern[1] != "":
            verdict.append(fnmatch.fnmatchcase(variant, pattern[1]))
        else:
            verdict.append(is_default_variant)
        verdict.append(fnmatch.fnmatchcase(group, pattern[2]))
        verdict.append(fnmatch.fnmatchcase(query, pattern[3]))
        if all(verdict):
            return True
    return False


def filter_benchmarks(generators, patterns, vendor_name: str):
    patterns = copy.deepcopy(patterns)
    for i in range(len(patterns)):
        pattern = patterns[i].split("/")
        if len(pattern) > 5 or len(pattern) == 0:
            raise Exception("Invalid benchmark description '" + pattern + "'!")
        pattern.extend(["", "*", "*"][len(pattern) - 1 :])
        patterns[i] = pattern
    filtered = []
    for dataset in sorted(generators.keys()):
        generator, queries = generators[dataset]
        for variant in generator.VARIANTS:
            is_default_variant = variant == generator.DEFAULT_VARIANT
            current = collections.defaultdict(list)
            for group in queries:
                for query_name, query_func in queries[group]:
                    if match_patterns(
                        dataset,
                        variant,
                        group,
                        query_name,
                        is_default_variant,
                        patterns,
                    ):
                        current[group].append((query_name, query_func))
            if len(current) == 0:
                continue

            # Ignore benchgraph "basic" queries in standard CI/CD run
            for pattern in patterns:
                res = pattern.count("*")
                key = "basic"
                if res >= 2 and key in current.keys():
                    current.pop(key)

            filtered.append((generator(variant, vendor_name), dict(current)))
    return filtered


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

    parser.add_argument(
        "--client-binary",
        default=helpers.get_binary_path("tests/mgbench/client"),
        help="Client binary used for benchmarking",
    )

    parser.add_argument(
        "--temporary-directory",
        default="/tmp",
        help="directory path where temporary data should " "be stored",
    )

    args = parser.parse_args()

    generators = {}
    workloads = map(workload.__dict__.get, workload.__all__)
    for module in workloads:
        if module != None:
            for key in dir(module):
                dataset_class = getattr(module, key)
                if not inspect.isclass(dataset_class) or not issubclass(dataset_class, dataset.Dataset):
                    continue
                queries = collections.defaultdict(list)
                for funcname in dir(dataset_class):
                    if not funcname.startswith("benchmark__"):
                        continue
                    group, query = funcname.split("__")[1:]
                    queries[group].append((query, funcname))
                generators[dataset_class.NAME] = (dataset_class, dict(queries))
                if dataset_class.PROPERTIES_ON_EDGES and False:
                    raise Exception(
                        'The "{}" dataset requires properties on edges, '
                        "but you have disabled them!".format(dataset.NAME)
                    )

    # List datasets if there is no specified dataset.
    if len(args.benchmarks) == 0:
        log.init("Available queries")
        for name in sorted(generators.keys()):
            print("Dataset:", name)
            dataset, queries = generators[name]
            print(
                "    Variants:",
                ", ".join(dataset.VARIANTS),
                "(default: " + dataset.DEFAULT_VARIANT + ")",
            )
            for group in sorted(queries.keys()):
                print("    Group:", group)
                for query_name, query_func in queries[group]:
                    print("        Query:", query_name)
        sys.exit(0)

    memgraph = runners.Memgraph(
        "/home/maple/repos/test/memgraph/build/memgraph",
        "/tmp",
        True,
        7687,
        False,
    )

    cache = helpers.Cache()
    client = runners.Client(args.client_binary, args.temporary_directory, 7687)

    benchmarks_memgraph = filter_benchmarks(generators, args.benchmarks, "memgraph")

    results_memgraph = {}

    for dataset, queries in benchmarks_memgraph:

        dataset.prepare(cache.cache_directory("datasets", dataset.NAME, dataset.get_variant()))
        impor = importer.Importer(dataset=dataset, vendor=memgraph, client=client)

        status = impor.try_optimal_import()

        if status == False:
            print("Need alternative import")
        else:
            print("Fast import executed")

        for group in sorted(queries.keys()):
            for query, funcname in queries[group]:
                print("Running query:{}/{}/{}".format(group, query, funcname))
                func = getattr(dataset, funcname)
                sample = (get_queries(func, 1),)
                count = 1
                # Benchmark run.
                print("Sample query:", get_queries(func, 1)[0][0])
                memgraph.start_benchmark("validation")
                try:
                    ret = client.execute(queries=get_queries(func, count), num_workers=1, validation=True)[0]
                    results_memgraph[funcname] = ret["results"].items()

                except Exception as e:
                    print("Issue running the query" + funcname)
                    print(e)
                    results_memgraph[funcname] = (funcname, "Query not executed properly")
                finally:
                    usage = memgraph.stop("validation")
                    print("Database used {:.3f} seconds of CPU time.".format(usage["cpu"]))
                    print("Database peaked at {:.3f} MiB of memory.".format(usage["memory"] / 1024.0 / 1024.0))

    neo4j = runners.Neo4j(
        "/home/maple/repos/neo4j-community-5.4.0",
        "/tmp",
        7687,
        False,
    )
    benchmarks_neo4j = filter_benchmarks(generators, args.benchmarks, "neo4j")

    results_neo4j = {}

    for dataset, queries in benchmarks_neo4j:

        dataset.prepare(cache.cache_directory("datasets", dataset.NAME, dataset.get_variant()))
        impo = importer.Importer(dataset=dataset, vendor=neo4j, client=client)

        status = impo.try_optimal_import()

        if status == False:
            print("Need alternative import")
        else:
            print("Fast import executed")

        for group in sorted(queries.keys()):
            for query, funcname in queries[group]:
                print("Running query:{}/{}/{}".format(group, query, funcname))
                func = getattr(dataset, funcname)
                sample = (get_queries(func, 1),)
                count = 1
                # Benchmark run.
                print("Sample query:", get_queries(func, 1)[0][0])
                neo4j.start_benchmark("validation")
                try:
                    ret = client.execute(queries=get_queries(func, count), num_workers=1, validation=True)[0]
                    results_neo4j[funcname] = ret["results"].items()

                except Exception as e:
                    print("Issue running the query" + funcname)
                    print(e)
                    results_neo4j[funcname] = (funcname, "Query not executed properly")
                finally:
                    usage = neo4j.stop("validation")
                    print("Database used {:.3f} seconds of CPU time.".format(usage["cpu"]))
                    print("Database peaked at {:.3f} MiB of memory.".format(usage["memory"] / 1024.0 / 1024.0))

    validation = {}
    for key in results_memgraph.keys():
        memgraph_values = set()
        for index, value in results_memgraph[key]:
            memgraph_values.add(value)
        neo4j_values = set()
        for index, value in results_neo4j[key]:
            neo4j_values.add(value)

        if memgraph_values == neo4j_values:
            validation[key] = "Identical results"
        else:
            print(neo4j_values)
            print(memgraph_values)
            s1 = memgraph_values.intersection(neo4j_values)
            s2 = neo4j_values.intersection(memgraph_values)
            print(s1)
            print(s2)
            validation[key] = "Different results"

    for key, value in validation.items():
        print(key + " " + value)
