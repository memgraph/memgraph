import logging
import os
import time
import itertools
import json
from argparse import ArgumentParser
from collections import defaultdict
import tempfile
from statistics import median
from common import get_absolute_path, WALL_TIME, CPU_TIME, MAX_MEMORY
from databases import Memgraph, Neo
from clients import QueryClient

log = logging.getLogger(__name__)


class _QuerySuite:
    """
    Executes a Query-based benchmark scenario. Query-based scenarios
    consist of setup steps (Cypher queries) executed before the benchmark,
    a single Cypher query that is benchmarked, and teardown steps
    (Cypher queries) executed after the benchmark.
    """
    # what the QuerySuite can work with
    KNOWN_KEYS = {"config", "setup", "itersetup", "run", "iterteardown",
                  "teardown", "common"}
    FORMAT = ["{:>24}", "{:>28}", "{:>16}", "{:>18}", "{:>22}",
              "{:>16}", "{:>16}", "{:>16}"]
    FULL_FORMAT = "".join(FORMAT) + "\n"
    summary = FULL_FORMAT.format(
                      "group_name", "scenario_name", "parsing_time",
                      "planning_time", "plan_execution_time",
                      WALL_TIME, CPU_TIME, MAX_MEMORY)

    def __init__(self, args):
        pass

    def run(self, scenario, group_name, scenario_name, runner):
        log.debug("QuerySuite.run() with scenario: %s", scenario)
        scenario_config = scenario.get("config")
        scenario_config = next(scenario_config()) if scenario_config else {}

        def execute(config_name, num_client_workers=1):
            queries = scenario.get(config_name)
            start_time = time.time()
            if queries:
                r_val = runner.execute(queries(), num_client_workers)
            else:
                r_val = None
            log.info("\t%s done in %.2f seconds" % (config_name,
                                                    time.time() - start_time))
            return r_val

        def add_measurement(dictionary, iteration, key):
            if key in dictionary:
                measurement = {"target": key,
                               "value": float(dictionary[key]),
                               "unit": "s",
                               "type": "time",
                               "iteration": iteration}
                measurements.append(measurement)
                try:
                    measurement_lists[key].append(float(dictionary[key]))
                except:
                    pass

        measurements = []

        measurement_lists = defaultdict(list)

        # Run the whole test 3 times because memgraph is sometimes
        # consistently slow and with this hack we get a good median
        for i in range(3):
            runner.start()
            execute("setup")

            # warmup phase
            for _ in range(min(scenario_config.get("iterations", 1),
                               scenario_config.get("warmup", 2))):
                execute("itersetup")
                execute("run", scenario_config.get("num_client_workers", 1))
                execute("iterteardown")

            # TODO per scenario/run runner configuration
            num_iterations = scenario_config.get("iterations", 1)
            for iteration in range(num_iterations):
                # TODO if we didn't have the itersetup it would be trivial
                # to move iteration to the bolt_client script, so we would not
                # have to start and stop the client for each iteration, it would
                # most likely run faster
                execute("itersetup")
                run_result = execute("run",
                                     scenario_config.get("num_client_workers", 1))
                add_measurement(run_result, iteration, CPU_TIME)
                add_measurement(run_result, iteration, MAX_MEMORY)
                assert len(run_result["groups"]) == 1, \
                        "Multiple groups in run step not yet supported"
                group = run_result["groups"][0]
                add_measurement(group, iteration, WALL_TIME)
                for measurement in ["parsing_time",
                                    "plan_execution_time",
                                    "planning_time"] :
                    for i in range(len(group.get("metadatas", []))):
                        add_measurement(group["metadatas"][i], iteration,
                                        measurement)
                execute("iterteardown")

            # TODO value outlier detection and warning across iterations
            execute("teardown")
            runner.stop()

        self.append_scenario_summary(group_name, scenario_name,
                                     measurement_lists, num_iterations)
        return measurements

    def append_scenario_summary(self, group_name, scenario_name,
                                measurement_lists, num_iterations):
        self.summary += self.FORMAT[0].format(group_name)
        self.summary += self.FORMAT[1].format(scenario_name)
        for i, key in enumerate(("parsing_time", "planning_time",
                    "plan_execution_time", WALL_TIME, CPU_TIME, MAX_MEMORY)):
            if key not in measurement_lists:
                time = "-"
            else:
                fmt = "{:.10f}"
                # Median is used instead of avg to avoid effect of outliers.
                value = median(measurement_lists[key])
                if key == MAX_MEMORY:
                    fmt = "{}"
                    value = int(value)
                time = fmt.format(value)
            self.summary += self.FORMAT[i + 2].format(time)
        self.summary += "\n"

    def runners(self):
        """ Which runners can execute a QuerySuite scenario """
        assert False, "This is a base class, use one of derived suites"

    def groups(self):
        """ Which groups can be executed by a QuerySuite scenario """
        assert False, "This is a base class, use one of derived suites"


class QuerySuite(_QuerySuite):
    def __init__(self, args):
        _QuerySuite.__init__(self, args)

    def runners(self):
        return {"MemgraphRunner" : MemgraphRunner, "NeoRunner" : NeoRunner}

    def groups(self):
        return ["1000_create", "unwind_create", "match", "dense_expand",
                "expression", "aggregation", "return", "update", "delete"]


class QueryParallelSuite(_QuerySuite):
    def __init__(self, args):
        _QuerySuite.__init__(self, args)

    def runners(self):
        # TODO: We should use different runners which will use more threads.
        return {"MemgraphRunner" : MemgraphRunner, "NeoRunner" : NeoRunner}

    def groups(self):
        return ["aggregation_parallel", "create_parallel"]


class _QueryRunner:
    """
    Knows how to start and stop database (backend) some client frontend (bolt),
    and execute a cypher query.
    Execution returns benchmarking data (execution times, memory
    usage etc).
    """
    def __init__(self, args, database):
        self.log = logging.getLogger("_HarnessClientRunner")
        self.database = database
        self.query_client = QueryClient(args)

    def start(self):
        self.database.start()

    def execute(self, queries, num_client_workers):
        return self.query_client(queries, self.database, num_client_workers)

    def stop(self):
        self.log.info("stop")
        self.database.stop()


class MemgraphRunner(_QueryRunner):
    """
    Configures memgraph database for QuerySuite execution.
    """
    def __init__(self, args):
        argp = ArgumentParser("MemgraphRunnerArgumentParser")
        argp.add_argument("--runner-config", default=get_absolute_path(
                "benchmarking_latency.conf", "config"),
                help="Path to memgraph config")
        argp.add_argument("--num-workers", help="Number of workers")
        self.args, remaining_args = argp.parse_known_args(args)
        database = Memgraph(remaining_args, self.args.runner_config,
                            self.args.num_workers)
        super(MemgraphRunner, self).__init__(remaining_args, database)


class NeoRunner(_QueryRunner):
    """
    Configures neo4j database for QuerySuite execution.
    """
    def __init__(self, args):
        argp = ArgumentParser("NeoRunnerArgumentParser")
        argp.add_argument("--runner-config",
                          default=get_absolute_path("config/neo4j.conf"),
                          help="Path to neo config file")
        self.args, remaining_args = argp.parse_known_args(args)
        database = Neo(remaining_args, self.args.runner_config)
        super(NeoRunner, self).__init__(remaining_args, database)
