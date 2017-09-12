import logging
import os
import time
import itertools
import json
from argparse import ArgumentParser
from collections import defaultdict
from statistics import median
from common import get_absolute_path
from databases import Memgraph, Neo
from clients import QueryClient, LongRunningClient

log = logging.getLogger(__name__)


class LongRunningSuite:
    KNOWN_KEYS = {"config", "setup", "run"}

    def __init__(self, args):
        argp = ArgumentParser("LongRunningSuiteArgumentParser")
        argp.add_argument("--num-client-workers", default=4)
        self.args, _ = argp.parse_known_args(args)
        pass

    def run(self, scenario, group_name, scenario_name, runner):
        runner.start()
        # This suite allows empty lines in setup. Those lines separate query
        # groups. It is guaranteed that groups will be executed sequentially,
        # but queries in each group are possibly executed concurrently.
        query_groups = [[]]
        for query in scenario.get("setup")():
            if query == "":
                query_groups.append([])
            else:
                query_groups[-1].append(query)
        if query_groups[-1] == []:
            query_groups.pop()

        log.info("Executing {} query groups in setup"
                .format(len(query_groups)))

        for i, queries in enumerate(query_groups):
            start_time = time.time()
            # TODO: number of threads configurable
            runner.setup(queries, self.args.num_client_workers)
            log.info("\t{}. group imported in done in {:.2f} seconds".format(
                i + 1, time.time() - start_time))

        config = next(scenario.get("config")())
        duration = config["duration"]
        log.info("Executing run for {} seconds with {} client workers".format(
            duration, self.args.num_client_workers))
        # TODO: number of threads configurable
        results = runner.run(next(scenario.get("run")()), duration,
                self.args.num_client_workers)

        runner.stop()

        measurements = []
        for result in results:
            print(result["num_executed_queries"], result["elapsed_time"])
            # TODO: Revise this.
            measurements.append({
                "target": "throughput",
                "value": result["num_executed_queries"] / result["elapsed_time"],
                "unit": "queries per second",
                "type": "throughput"})
        self.summary = "Throughtput: " + str(measurements[-1]["value"])
        return measurements

    def runners(self):
        return { "MemgraphRunner" : MemgraphRunner, "NeoRunner" : NeoRunner }

    def groups(self):
        return ["pokec"]


class _LongRunningRunner:
    def __init__(self, args, database):
        self.log = logging.getLogger("_LongRunningRunner")
        self.database = database
        self.query_client = QueryClient(args)
        self.long_running_client = LongRunningClient(args)

    def start(self):
        self.database.start()

    def setup(self, queries, num_client_workers):
        return self.query_client(queries, self.database, num_client_workers)

    def run(self, config, duration, num_client_workers):
        return self.long_running_client(
            config, self.database, duration, num_client_workers)

    def stop(self):
        self.log.info("stop")
        self.database.stop()


class MemgraphRunner(_LongRunningRunner):
    """
    Configures memgraph database for QuerySuite execution.
    """
    def __init__(self, args):
        argp = ArgumentParser("MemgraphRunnerArgumentParser")
        # TODO: change default config
        argp.add_argument("--runner-config", default=get_absolute_path(
                "benchmarking_throughput.conf", "config"),
                help="Path to memgraph config")
        argp.add_argument("--num-workers", help="Number of workers")
        self.args, remaining_args = argp.parse_known_args(args)
        database = Memgraph(remaining_args, self.args.runner_config,
                            self.args.num_workers)
        super(MemgraphRunner, self).__init__(remaining_args, database)


class NeoRunner(_LongRunningRunner):
    """
    Configures neo4j database for QuerySuite execution.
    """
    def __init__(self, args):
        argp = ArgumentParser("NeoRunnerArgumentParser")
        argp.add_argument("--runner-config",
                          default=get_absolute_path(
                              "config/neo4j_long_running.conf"),
                          help="Path to neo config file")
        self.args, remaining_args = argp.parse_known_args(args)
        database = Neo(remaining_args, self.args.runner_config, [1])
        super(NeoRunner, self).__init__(remaining_args, database)
