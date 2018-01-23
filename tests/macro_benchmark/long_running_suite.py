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
        log.info("Executing run for {} seconds".format(
                duration))
        results = runner.run(next(scenario.get("run")()), duration)

        runner.stop()

        measurements = []
        summary_format = "{:>15} {:>22}\n"
        self.summary = summary_format.format(
                "elapsed_time", "num_executed_queries")
        for result in results:
            self.summary += summary_format.format(
                    result["elapsed_time"], result["num_executed_queries"])
            measurements.append({
                "target": "throughput",
                "time": result["elapsed_time"],
                "value": result["num_executed_queries"],
                "unit": "number of executed queries",
                "type": "throughput"})
        self.summary += "\n\nThroughtput: " + str(measurements[-1]["value"])
        return measurements

    def runners(self):
        return {"MemgraphRunner": MemgraphRunner, "NeoRunner": NeoRunner}

    def groups(self):
        return ["pokec"]


class _LongRunningRunner:
    def __init__(self, args, database, num_client_workers):
        self.log = logging.getLogger("_LongRunningRunner")
        self.database = database
        self.query_client = QueryClient(args, num_client_workers)
        self.long_running_client = LongRunningClient(args, num_client_workers)

    def start(self):
        self.database.start()

    def setup(self, queries, num_client_workers=None):
        return self.query_client(queries, self.database, num_client_workers)

    def run(self, config, duration, num_client_workers=None):
        return self.long_running_client(
            config, self.database, duration, num_client_workers)

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
        argp.add_argument("--runner-config", default=get_absolute_path(
                "benchmarking.conf", "config"),
                help="Path to memgraph config")
        argp.add_argument("--num-database-workers", type=int, default=8,
                          help="Number of workers")
        argp.add_argument("--num-client-workers", type=int, default=24,
                          help="Number of clients")
        self.args, remaining_args = argp.parse_known_args(args)
        assert not APOLLO or self.args.num_database_workers, \
            "--num-database-workers is obligatory flag on apollo"
        assert not APOLLO or self.args.num_client_workers, \
            "--num-client-workers is obligatory flag on apollo"
        database = Memgraph(remaining_args, self.args.runner_config,
                            self.args.num_database_workers)
        super(MemgraphRunner, self).__init__(
                remaining_args, database, self.args.num_client_workers)


class NeoRunner(_LongRunningRunner):
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
