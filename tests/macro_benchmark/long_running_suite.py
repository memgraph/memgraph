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
from argparse import ArgumentParser
from common import get_absolute_path, APOLLO
from databases import Memgraph, Neo
from clients import QueryClient, LongRunningClient

log = logging.getLogger(__name__)


class LongRunningSuite:
    KNOWN_KEYS = {"config", "setup", "run"}
    headers = ["elapsed_time", "num_executed_queries"]

    def __init__(self, args):
        argp = ArgumentParser("LongRunningSuiteArgumentParser")
        argp.add_argument("--duration", type=int)
        self.args, _ = argp.parse_known_args(args)
        pass

    def run(self, scenario, group_name, scenario_name, runner):
        runner.start()

        log.info("Executing setup")
        runner.setup(scenario.get("setup")())

        config = next(scenario.get("config")())
        duration = config["duration"]
        if self.args.duration:
            duration = self.args.duration
        log.info("Executing run for {} seconds".format(duration))
        results = runner.run(next(scenario.get("run")()), duration, config["client"])

        runner.stop()

        measurements = []
        summary_format = "{:>15} {:>22} {:>22}\n"
        self.summary = summary_format.format("elapsed_time", "num_executed_queries", "num_executed_steps")
        for result in results:
            self.summary += summary_format.format(
                result["elapsed_time"],
                result["num_executed_queries"],
                result["num_executed_steps"],
            )
            measurements.append(
                {
                    "target": "throughput",
                    "time": result["elapsed_time"],
                    "value": result["num_executed_queries"],
                    "steps": result["num_executed_steps"],
                    "unit": "number of executed queries",
                    "type": "throughput",
                }
            )
        self.summary += "\n\nThroughput: " + str(measurements[-1]["value"])
        self.summary += "\nExecuted steps: " + str(measurements[-1]["steps"])
        return measurements

    def runners(self):
        return {"MemgraphRunner": MemgraphRunner, "NeoRunner": NeoRunner}

    def groups(self):
        return ["pokec", "card_fraud", "bfs_pokec"]


class _LongRunningRunner:
    def __init__(self, args, database, num_client_workers, workload):
        self.log = logging.getLogger("_LongRunningRunner")
        self.database = database
        self.query_client = QueryClient(args, num_client_workers)
        self.long_running_client = LongRunningClient(args, num_client_workers, workload)

    def start(self):
        self.database.start()

    def setup(self, queries, num_client_workers=None):
        return self.query_client(queries, self.database, num_client_workers)

    def run(self, config, duration, client, num_client_workers=None):
        return self.long_running_client(config, self.database, duration, client, num_client_workers)

    def stop(self):
        self.log.info("stop")
        self.database.stop()


# TODO: This is mostly copy pasted from MemgraphParallelRunner from
# query_suite.py. Think of the way to remove duplication.
class MemgraphRunner(_LongRunningRunner):
    """
    Configures memgraph database for LongRunningSuite execution.
    """

    def __init__(self, args):
        argp = ArgumentParser("MemgraphRunnerArgumentParser")
        argp.add_argument("--num-database-workers", type=int, default=8, help="Number of workers")
        argp.add_argument("--num-client-workers", type=int, default=24, help="Number of clients")
        argp.add_argument(
            "--workload",
            type=str,
            default="",
            help="Type of client workload. Sets \
                          scenario flag for 'TestClient'",
        )
        self.args, remaining_args = argp.parse_known_args(args)
        assert not APOLLO or self.args.num_database_workers, "--num-database-workers is obligatory flag on apollo"
        assert not APOLLO or self.args.num_client_workers, "--num-client-workers is obligatory flag on apollo"
        database = Memgraph(remaining_args, self.args.num_database_workers)
        super(MemgraphRunner, self).__init__(remaining_args, database, self.args.num_client_workers, self.args.workload)


class NeoRunner(_LongRunningRunner):
    """
    Configures neo4j database for QuerySuite execution.
    """

    def __init__(self, args):
        argp = ArgumentParser("NeoRunnerArgumentParser")
        argp.add_argument(
            "--runner-config",
            default=get_absolute_path("config/neo4j.conf"),
            help="Path to neo config file",
        )
        argp.add_argument("--num-client-workers", type=int, default=24, help="Number of clients")
        argp.add_argument(
            "--workload",
            type=str,
            default="",
            help="Type of client workload. Sets \
                          scenario flag for 'TestClient'",
        )
        self.args, remaining_args = argp.parse_known_args(args)
        assert not APOLLO or self.args.num_client_workers, "--client-num-clients is obligatory flag on apollo"
        database = Neo(remaining_args, self.args.runner_config)
        super(NeoRunner, self).__init__(remaining_args, database, self.args.num_client_workers, self.args.workload)
