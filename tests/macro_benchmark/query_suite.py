# Copyright 2021 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import logging
import os
import time
import itertools
import subprocess
import json
from argparse import ArgumentParser
from collections import defaultdict
import tempfile
from statistics import median, mean, stdev
from common import get_absolute_path, WALL_TIME, CPU_TIME, MAX_MEMORY, APOLLO
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
    headers = ["group_name", "scenario_name", "parsing_time",
               "planning_time", "plan_execution_time",
               WALL_TIME, CPU_TIME, MAX_MEMORY]
    summary = summary_raw = FULL_FORMAT.format(*headers)

    def __init__(self, args):
        argp = ArgumentParser("MemgraphRunnerArgumentParser")
        argp.add_argument("--perf", default=False, action="store_true",
            help="Run perf on running tests and store data")
        self.args, remaining_args = argp.parse_known_args(args)

    def run(self, scenario, group_name, scenario_name, runner):
        log.debug("QuerySuite.run() with scenario: %s", scenario)
        scenario_config = scenario.get("config")
        scenario_config = next(scenario_config()) if scenario_config else {}

        def execute(config_name, num_client_workers=None):
            queries = scenario.get(config_name)
            start_time = time.time()
            if queries:
                r_val = runner.execute(queries(), num_client_workers)
            else:
                r_val = None
            log.info("\t%s done in %.2f seconds" % (config_name,
                                                    time.time() - start_time))
            return r_val

        measurements = defaultdict(list)

        # Run the whole test three times because memgraph is sometimes
        # consistently slow and with this hack we get a good median
        for rerun_cnt in range(3):
            runner.start()
            execute("setup")

            # warmup phase
            for _ in range(min(scenario_config.get("iterations", 1),
                               scenario_config.get("warmup", 2))):
                execute("itersetup")
                execute("run")
                execute("iterteardown")

            # TODO per scenario/run runner configuration
            num_iterations = scenario_config.get("iterations", 1)
            for iteration in range(num_iterations):
                # TODO if we didn't have the itersetup it would be trivial
                # to move iteration to the bolt_client script, so we would not
                # have to start and stop the client for each iteration, it would
                # most likely run faster
                execute("itersetup")

                if self.args.perf:
                    file_directory = './perf_results/run_%d/%s/%s/' \
                                      % (rerun_cnt, group_name, scenario_name)
                    os.makedirs(file_directory, exist_ok=True)
                    file_name = '%d.perf.data' % iteration
                    path = file_directory + file_name
                    database_pid = str(runner.database.database_bin._proc.pid)
                    self.perf_proc = subprocess.Popen(
                        ["perf", "record", "-F", "999", "-g", "-o", path, "-p",
                        database_pid])

                run_result = execute("run")

                if self.args.perf:
                    self.perf_proc.terminate()
                    self.perf_proc.wait()

                measurements["cpu_time"].append(run_result["cpu_time"])
                measurements["max_memory"].append(run_result["max_memory"])

                assert len(run_result["groups"]) == 1, \
                        "Multiple groups in run step not yet supported"

                group = run_result["groups"][0]
                measurements["wall_time"].append(group["wall_time"])

                for key in ["parsing_time", "plan_execution_time",
                            "planning_time"]:
                    for i in range(len(group.get("metadatas", []))):
                        if not key in group["metadatas"][i]: continue
                        measurements[key].append(group["metadatas"][i][key])

                execute("iterteardown")

            execute("teardown")
            runner.stop()

        self.append_scenario_summary(group_name, scenario_name,
                                     measurements, num_iterations)

        # calculate mean, median and stdev of measurements
        for key in measurements:
            samples = measurements[key]
            measurements[key] = {"mean": mean(samples),
                                 "median": median(samples),
                                 "stdev": stdev(samples),
                                 "count": len(samples)}
        measurements["group_name"] = group_name
        measurements["scenario_name"] = scenario_name

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
        return {"MemgraphRunner" : MemgraphParallelRunner, "NeoRunner" :
                NeoParallelRunner}

    def groups(self):
        return ["aggregation_parallel", "create_parallel", "bfs_parallel"]


class _QueryRunner:
    """
    Knows how to start and stop database (backend) some client frontend (bolt),
    and execute a cypher query.
    Execution returns benchmarking data (execution times, memory
    usage etc).
    """
    def __init__(self, args, database, num_client_workers):
        self.log = logging.getLogger("_HarnessClientRunner")
        self.database = database
        self.query_client = QueryClient(args, num_client_workers)

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
        database = Memgraph(args, 1)
        super(MemgraphRunner, self).__init__(args, database, 1)


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


class NeoParallelRunner(_QueryRunner):
    """
    Configures neo4j database for QuerySuite execution.
    """
    def __init__(self, args):
        argp = ArgumentParser("NeoRunnerArgumentParser")
        argp.add_argument("--runner-config",
                          default=get_absolute_path("config/neo4j.conf"),
                          help="Path to neo config file")
        argp.add_argument("--num-client-workers", type=int, default=24,
                          help="Number of clients")
        self.args, remaining_args = argp.parse_known_args(args)
        assert not APOLLO or self.args.num_client_workers, \
                "--client-num-clients is obligatory flag on apollo"
        database = Neo(remaining_args, self.args.runner_config)
        super(NeoRunner, self).__init__(
                remaining_args, database, self.args.num_client_workers)


class MemgraphParallelRunner(_QueryRunner):
    """
    Configures memgraph database for QuerySuite execution.
    """
    def __init__(self, args):
        argp = ArgumentParser("MemgraphRunnerArgumentParser")
        argp.add_argument("--num-database-workers", type=int, default=8,
                          help="Number of workers")
        argp.add_argument("--num-client-workers", type=int, default=24,
                          help="Number of clients")
        self.args, remaining_args = argp.parse_known_args(args)
        assert not APOLLO or self.args.num_database_workers, \
                "--num-database-workers is obligatory flag on apollo"
        assert not APOLLO or self.args.num_client_workers, \
                "--num-client-workers is obligatory flag on apollo"
        database = Memgraph(remaining_args, self.args.num_database_workers)
        super(MemgraphParallelRunner, self).__init__(
                remaining_args, database, self.args.num_client_workers)
