import argparse
import collections
import copy
import fnmatch
import inspect
import random
import sys

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


def filter_benchmarks(generators, patterns):
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

            filtered.append((generator(variant, args.vendor_name), dict(current)))
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
                if dataset_class.PROPERTIES_ON_EDGES and args.no_properties_on_edges:
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
        False,
        7687,
        False,
    )

    neo4j = runners.Neo4j(
        "/home/maple/repos/neo4j-community-5.4.0",
        "/tmp",
        7687,
        False,
    )

    benchmarks = filter_benchmarks(generators, args.benchmarks)

    output = {}

    for dataset, queries in benchmarks:

        importer = importer.Importer(dataset=dataset, vendor=neo4j, client=neo4j)

        status = importer.try_optimal_import()

        if status == False:
            print("Need alternative import")
        else:
            print("Fast import executed")

        for group in sorted(queries.keys()):
            for query, funcname in queries[group]:
                print("Running query:{}/{}/{}".format(group, query, funcname))
                func = getattr(dataset, funcname)
                count = 1
                # Benchmark run.
                print("Sample query:", get_queries(func, 1)[0][0])
                print(
                    "Executing benchmark with",
                    count,
                )
                vendor.start_benchmark(dataset.NAME + dataset.get_variant() + "_" + workload.name + "_" + query)
                if args.warmup_run:
                    warmup(client)
                ret = client.execute(
                    queries=get_queries(func, count),
                    num_workers=args.num_workers_for_benchmark,
                )[0]
                usage = vendor.stop(dataset.NAME + dataset.get_variant() + "_" + workload.name + "_" + query)
                ret["database"] = usage
                ret["query_statistics"] = query_statistics

                # Output summary.
                print()
                print("Executed", ret["count"], "queries in", ret["duration"], "seconds.")
                print("Queries have been retried", ret["retries"], "times.")
                print("Database used {:.3f} seconds of CPU time.".format(usage["cpu"]))
                print("Database peaked at {:.3f} MiB of memory.".format(usage["memory"] / 1024.0 / 1024.0))
                print("{:<31} {:>20} {:>20} {:>20}".format("Metadata:", "min", "avg", "max"))
                metadata = ret["metadata"]
                for key in sorted(metadata.keys()):
                    print(
                        "{name:>30}: {minimum:>20.06f} {average:>20.06f} "
                        "{maximum:>20.06f}".format(name=key, **metadata[key])
                    )
                log.success("Throughput: {:02f} QPS".format(ret["throughput"]))

                # Save results.
                results_key = [
                    dataset.NAME,
                    dataset.get_variant(),
                    group,
                    query,
                    WITHOUT_FINE_GRAINED_AUTHORIZATION,
                ]
                results.set_value(*results_key, value=ret)

    vendor.start_benchmark("validation")
    ret_mem = client_memgraph.execute(
        queries=[("MATCH (n1)-[M]-(n2) RETURN n1, M , n2;", {})], num_workers=1, validation=True
    )[0]
    vendor.stop("validation")

    ret_neo = client_neo4j.execute(
        queries=[("MATCH (n1)-[M]-(n2) RETURN n1, M , n2;", {})], num_workers=1, validation=True
    )[0]

    # (TODO) Fix comparisons.
    for key, value in ret_mem["results"].items():
        for key, value in ret_neo["results"]:
            if value != ret_neo["results"][key]:
                print("Different")
                print(value)
                print(ret_neo["results"][key])
            else:
                print("Identical")
                print(value)
                print(ret_neo["results"][key])

    print(ret_mem)
    print("___")
    print(ret_neo)

    for dataset, queries in benchmarks:
        print(dataset)
        for group in queries.keys():
            print(queries)
            for query in queries[group]:
                print(query)
