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
        argp.add_argument("--duration", type=int)
        self.args, _ = argp.parse_known_args(args)
        pass

    def run(self, scenario, group_name, scenario_name, runner):
        runner.start()

        log.info("Executing setup")
        runner.setup(scenario.get("setup")(), self.args.num_client_workers)

        config = next(scenario.get("config")())
        duration = config["duration"]
        if self.args.duration:
            duration = self.args.duration
        log.info("Executing run for {} seconds with {} client workers".format(
            duration, self.args.num_client_workers))
        results = runner.run(next(scenario.get("run")()), duration,
                self.args.num_client_workers)

        runner.stop()

        measurements = []
        summary_format = "{:>15} {:>22}\n"
        self.summary = summary_format.format(
                "elapsed_time", "num_executed_queries")
        for result in results:
            self.summary += summary_format.format(
                    result["elapsed_time"], result["num_executed_queries"])
            # TODO: Revise this.
            measurements.append({
                "target": "throughput",
                "value": result["num_executed_queries"] / result["elapsed_time"],
                "unit": "queries per second",
                "type": "throughput"})
        self.summary += "\n\nThroughtput: " + str(measurements[-1]["value"])
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
