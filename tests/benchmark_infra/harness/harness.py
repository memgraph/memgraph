#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import os
from os import path
import time
import itertools
import json
from subprocess import check_output
from argparse import ArgumentParser
from collections import OrderedDict
from collections import defaultdict
import tempfile
import shutil

import jail_faker as jail
from bolt_client import WALL_TIME
from perf import Perf

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
    summary = "Summary:\n{:>30}{:>30}{:>30}{:>30}{:>30}\n".format(
            "scenario_name", "query_parsing_time", "query_planning_time",
            "query_plan_execution_time", WALL_TIME)

    def __init__(self, args):
        self.perf = Perf()
        argp = ArgumentParser(description=__doc__)
        argp.add_argument("--perf", help="Run perf on memgraph binary.",
                          action="store_true")
        args, _ = argp.parse_known_args(args)
        self.perf = Perf() if args.perf else None

    class Loader:
        """
        Loads file contents. Supported types are:
            .py - executable that prints out Cypher queries
            .cypher - contains Cypher queries in textual form
            .json - contains a configuration

        A QueryLoader object is callable.
        A call to it returns a generator that yields loaded data
        (Cypher queries, configuration). In that sense one
        QueryLoader is reusable. The generator approach makes it possible
        to generated different queries each time when executing a .py file.
        """
        def __init__(self, file_path):
            self.file_path = file_path

        def _queries(self, data):
            """ Helper function for breaking down and filtering queries"""
            for element in filter(
                    None, map(str.strip, data.replace("\n", " ").split(";"))):
                yield element

        def __call__(self):
            """ Yields queries found in the given file_path one by one """
            log.debug("Generating queries from file_path: %s",
                      self.file_path)
            _, extension = path.splitext(self.file_path)
            if extension == ".cypher":
                with open(self.file_path) as f:
                    return self._queries(f.read())
            elif extension == ".py":
                return self._queries(check_output(
                    ["python3", self.file_path]).decode("ascii"))
            elif extension == ".json":
                with open(self.file_path) as f:
                    return [json.load(f)].__iter__()
            else:
                raise Exception("Unsupported filetype {} ".format(extension))

        def __repr__(self):
            return "(QuerySuite.Loader<%s>)" % self.file_path

    @staticmethod
    def scenarios(args):
        """
        Scans through folder structure starting with groups_root and
        loads query scenarios.
        Expected folder structure is:
            groups_root/
                groupname1/
                    config.json
                    common.py
                    setup.FILE_TYPE
                    teardown.FILE_TYPE
                    itersetup.FILE_TYPE
                    iterteardown.FILE_TYPE
                    scenario1.config.json
                    scenario1.run.FILE_TYPE-------(mandatory)
                    scenario1.setup.FILE_TYPE
                    scenario1.teardown.FILE_TYPE
                    scenario1.itersetup.FILE_TYPE
                    scenario1.iterteardown.FILE_TYPE
                    scenario2...
                                ...
                groupname2/
                            ...

        Per query configs (setup, teardown, itersetup, iterteardown)
        override group configs for that scenario. Group configs must have one
        extension (.FILE_TYPE) and
        scenario configs must have 2 extensions (.scenario_name.FILE_TYPE).
        See `QueryLoader` documentation to see which file types are supported.

        Args:
            args: additional args parsed by this function
            group_paths: str, root folder that contains group folders
        Return:
            {group: (scenario, {config: query_generator_function})
        """
        argp = ArgumentParser("QuerySuite.scenarios argument parser")
        argp.add_argument("--query-scenarios-root", default=path.join(
            path.dirname(path.dirname(path.realpath(__file__))), "groups"),
            dest="root")
        args, _ = argp.parse_known_args()
        log.info("Loading query scenarios from root: %s", args.root)

        def fill_config_dict(config_dict, base, config_files):
            for config_file in config_files:
                log.debug("Processing config file %s", config_file)
                config_name = config_file.split(".")[-2]
                config_dict[config_name] = QuerySuite.Loader(
                    path.join(base, config_file))

            # validate that the scenario does not contain any illegal
            # keys (defense against typos in file naming)
            unknown_keys = set(config_dict) - QuerySuite.KNOWN_KEYS
            if unknown_keys:
                raise Exception("Unknown QuerySuite config elements: '%r'" %
                                unknown_keys)

        def dir_content(root, predicate):
            return [p for p in os.listdir(root)
                    if predicate(path.join(root, p))]

        group_scenarios = OrderedDict()
        for group in dir_content(args.root, path.isdir):
            log.info("Loading group: '%s'", group)

            group_scenarios[group] = []
            files = dir_content(path.join(args.root, group),
                                path.isfile)

            # process group default config
            group_config = {}
            fill_config_dict(group_config, path.join(args.root, group),
                             [f for f in files if f.count(".") == 1])

            # group files on scenario
            for scenario_name, scenario_files in itertools.groupby(
                    filter(lambda f: f.count(".") == 2, sorted(files)),
                    lambda x: x.split(".")[0]):
                log.info("Loading scenario: '%s'", scenario_name)
                scenario = dict(group_config)
                fill_config_dict(scenario,
                                 path.join(args.root, group),
                                 scenario_files)
                group_scenarios[group].append((scenario_name, scenario))
                log.debug("Loaded config for scenario '%s'\n%r", scenario_name,
                          scenario)

        return group_scenarios

    def run(self, scenario, scenario_name, runner):
        log.debug("QuerySuite.run() with scenario: %s", scenario)
        scenario_config = scenario.get("config")
        scenario_config = next(scenario_config()) if scenario_config else {}

        def execute(config_name, num_client_workers=1):
            queries = scenario.get(config_name)
            return runner.execute(queries(), num_client_workers) if queries \
                else None

        measurements = []

        measurement_sums = defaultdict(float)

        def add_measurement(dictionary, iteration, key):
            if key in dictionary:
                measurement = {"target": key,
                               "value": float(dictionary[key]),
                               "unit": "s",
                               "type": "time",
                               "iteration": iteration}
                measurements.append(measurement)
                try:
                    measurement_sums[key] += float(dictionary[key])
                except:
                    pass

        pid = runner.start()
        execute("setup")

        # warmup phase
        for _ in range(min(scenario_config.get("iterations", 1),
                           scenario_config.get("warmup", 3))):
            execute("itersetup")
            execute("run", scenario_config.get("num_client_workers", 1))
            execute("iterteardown")

        if self.perf:
            self.perf.start(pid)

        # TODO per scenario/run runner configuration
        num_iterations = scenario_config.get("iterations", 1)
        for iteration in range(num_iterations):
            # TODO if we didn't have the itersetup it would be trivial
            # to move iteration to the bolt_client script, so we would not
            # have to start and stop the client for each iteration, it would
            # most likely run faster
            execute("itersetup")
            # TODO measure CPU time (expose it from the runner)
            run_result = execute("run",
                                 scenario_config.get("num_client_workers", 1))
            add_measurement(run_result, iteration, WALL_TIME)
            if len(run_result.get("metadatas", [])) == 1:
                add_measurement(run_result["metadatas"][0], iteration,
                                "query_parsing_time")
                add_measurement(run_result["metadatas"][0], iteration,
                                "query_plan_execution_time")
                add_measurement(run_result["metadatas"][0], iteration,
                                "query_planning_time")
            execute("iterteardown")

        if self.perf:
            self.perf.stop()

        # TODO value outlier detection and warning across iterations
        execute("teardown")
        runner.stop()
        self.append_scenario_summary(scenario_name, measurement_sums,
                                     num_iterations)
        return measurements

    def append_scenario_summary(self, scenario_name, measurement_sums,
                                num_iterations):
        self.summary += "{:>30}".format(scenario_name)
        for key in ("query_parsing_time", "query_planning_time",
                    "query_plan_execution_time", WALL_TIME):
            if key not in measurement_sums:
                time = "-"
            else:
                time = "{:.10f}".format(measurement_sums[key] / num_iterations)
            self.summary += "{:>30}".format(time)
        self.summary += "\n"

    def runners(self):
        """ Which runners can execute a QuerySuite scenario """
        assert False, "This is a base class, use one of derived suites"

    def groups(self):
        """ Which groups can be executed by a QuerySuite scenario """
        assert False, "This is a base class, use one of derived suites"
        return ["create", "match", "expression", "aggregation", "return",
                "update", "delete", "hardcoded"]


class QuerySuite(_QuerySuite):
    def __init__(self, args):
        _QuerySuite.__init__(self, args)

    def runners(self):
        return ["MemgraphRunner", "NeoRunner"]

    def groups(self):
        return ["create", "match", "expression", "aggregation", "return",
                "update", "delete"]


class QueryParallelSuite(_QuerySuite):
    def __init__(self, args):
        _QuerySuite.__init__(self, args)

    def runners(self):
        return ["MemgraphRunner", "NeoRunner"]

    def groups(self):
        return ["aggregation_parallel", "create_parallel"]


class _BaseRunner:
    """
    Knows how to start and stop database (backend) some client frontend (bolt),
    and execute a cypher query.
    Execution returns benchmarking data (execution times, memory
    usage etc).
    Inherited class should implement start method and initialise database_bin
    and bolt_client members of type Process.
    """
    def __init__(self, args):
        self.log = logging.getLogger("_BaseRunner")

    def _get_argparser(self):
        argp = ArgumentParser("RunnerArgumentParser")
        # TODO: These two options should be passed two database and client, not
        # only client as we are doing at the moment.
        argp.add_argument("--RunnerUri", default="localhost:7687")
        argp.add_argument("--RunnerEncryptBolt", action="store_true")
        return argp

    def start(self):
        raise NotImplementedError(
                "This method should be implemented in derivded class")

    def execute(self, queries, num_client_workers):
        self.log.debug("execute('%s')", str(queries))
        client_args = [path.join(path.dirname(__file__), "bolt_client.py")]
        client_args += ["--endpoint", self.args.RunnerUri]
        client_args += ["--num-workers", str(num_client_workers)]
        if self.args.RunnerEncryptBolt:
            client_args.append("--ssl-enabled")
        queries_fd, queries_path = tempfile.mkstemp()
        try:
            queries_file = os.fdopen(queries_fd, "w")
            queries_file.write("\n".join(queries))
            queries_file.close()
        except:
            queries_file.close()
            os.remove(queries_path)
            raise Exception("Writing queries to temporary file failed")

        # TODO make the timeout configurable per query or something
        return_code = self.bolt_client.run_and_wait(
            "python3", client_args, timeout=10000, stdin=queries_path)
        os.remove(queries_path)
        if return_code != 0:
            with open(self.bolt_client.get_stderr()) as f:
                stderr = f.read()
            self.log.error("Error while executing queries '%s'. "
                           "Failed with return_code %d and stderr:\n%s",
                           str(queries), return_code, stderr)
            raise Exception("BoltClient execution failed")
        with open(self.bolt_client.get_stdout()) as f:
            return json.loads(f.read())

    def stop(self):
        self.log.info("stop")
        self.bolt_client.send_signal(jail.SIGKILL)
        self.bolt_client.wait()
        self.database_bin.send_signal(jail.SIGKILL)
        self.database_bin.wait()


class MemgraphRunner(_BaseRunner):
    """
    Knows how to start and stop Memgraph (backend) some client frontent
    (bolt), and execute a cypher query.
    Execution returns benchmarking data (execution times, memory
    usage etc).
    """
    def __init__(self, args):
        super(MemgraphRunner, self).__init__(args)
        self.log = logging.getLogger("MemgraphRunner")
        argp = self._get_argparser()
        argp.add_argument("--RunnerBin",
                          default=os.path.join(os.path.dirname(__file__),
                                               "../../../build/memgraph"))
        argp.add_argument("--RunnerConfig",
                          default=os.path.join(
                              os.path.dirname(__file__),
                              "../../../config/benchmarking.conf"))
        # parse args
        self.log.info("Initializing Runner with arguments %r", args)
        self.args, _ = argp.parse_known_args(args)
        self.database_bin = jail.get_process()
        self.bolt_client = jail.get_process()

    def start(self):
        self.log.info("start")
        environment = os.environ.copy()
        environment["MEMGRAPH_CONFIG"] = self.args.RunnerConfig
        self.database_bin.run(self.args.RunnerBin, env=environment,
                              timeout=10000)
        # TODO change to a check via SIGUSR
        time.sleep(1.0)
        return self.database_bin.get_pid()


class NeoRunner(_BaseRunner):
    def __init__(self, args):
        super(NeoRunner, self).__init__(args)
        self.log = logging.getLogger("NeoRunner")
        argp = self._get_argparser()
        argp.add_argument(
            "--RunnerConfigDir",
            default=path.join(path.dirname(path.realpath(__file__)),
                              "neo4j_config"))
        argp.add_argument(
            "--RunnerHomeDir",
            default=path.join(path.dirname(path.realpath(__file__)),
                              "neo4j_home"))
        # parse args
        self.log.info("Initializing Runner with arguments %r", args)
        self.args, _ = argp.parse_known_args(args)
        self.database_bin = jail.get_process()
        self.bolt_client = jail.get_process()

    def start(self):
        self.log.info("start")
        environment = os.environ.copy()
        environment["NEO4J_CONF"] = self.args.RunnerConfigDir
        environment["NEO4J_HOME"] = self.args.RunnerHomeDir
        neo4j_data_path = path.join(environment["NEO4J_HOME"], "data")
        if path.exists(neo4j_data_path):
            shutil.rmtree(neo4j_data_path)
        self.database_bin.run("/usr/share/neo4j/bin/neo4j", args=["console"],
                              env=environment, timeout=10000)
        # TODO change to a check via SIGUSR
        time.sleep(5.0)
        return self.database_bin.get_pid()


def parse_known_args():
    argp = ArgumentParser(description=__doc__)
    # positional, mandatory args
    argp.add_argument("suite", help="Suite to run.")
    argp.add_argument("runner", help="Engine to use.")
    # named, optional arguments
    argp.add_argument("--groups", nargs="+", help="Groups to run. If none are"
                      " provided, all available grups are run.")
    argp.add_argument("--scenarios", nargs="+", help="Scenarios to run. If "
                      "none are provided, all available are run.")
    argp.add_argument("--logging", default="INFO", choices=["INFO", "DEBUG"],
                      help="Logging level")
    argp.add_argument("--additional-run-fields", default={}, type=json.loads,
                      help="Additional fields to add to the 'run', in JSON")
    return argp.parse_known_args()


def main():
    args, remaining_args = parse_known_args()
    if args.logging:
        logging.basicConfig(level=args.logging)
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("neo4j.bolt").setLevel(logging.WARNING)
    log.info("Memgraph benchmark suite harness")
    log.info("Executing for suite '%s', runner '%s'", args.suite, args.runner)

    # Create suite
    suites = {"QuerySuite": QuerySuite,
              "QueryParallelSuite": QueryParallelSuite}
    if args.suite not in suites:
        raise Exception(
            "Suite '{}' isn't registered. Registered suites are: {}".format(
                args.suite, suites))
    suite = suites[args.suite](remaining_args)

    # Load scenarios
    group_scenarios = suites[args.suite].scenarios(remaining_args)
    log.info("Loaded %d groups, with a total of %d scenarios",
             len(group_scenarios),
             sum([len(x) for x in group_scenarios.values()]))

    # Create runner
    runners = {"MemgraphRunner": MemgraphRunner, "NeoRunner": NeoRunner}
    # TODO if make runner argument optional, then execute all runners
    if args.runner not in suite.runners():
        raise Exception("Runner '{}' not registered for suite '{}'".format(
            args.runner, args.suite))
    runner = runners[args.runner](remaining_args)

    # Validate groups (if provided)
    if args.groups:
        for group in args.groups:
            if group not in suite.groups():
                raise Exception("Group '{}' isn't registered for suite '{}'".
                                format(group, suite))
        groups = args.groups
    else:
        # No groups provided, use all suite group
        groups = suite.groups()

    # TODO enable scenario filtering on regex
    filtered_scenarios = OrderedDict()
    for group, scenarios in group_scenarios.items():
        if group not in groups:
            log.info("Skipping group '%s'", group)
            continue
        for scenario_name, scenario in scenarios:
            if args.scenarios and scenario_name not in args.scenarios:
                continue
            filtered_scenarios[(group, scenario_name)] = scenario

    if len(filtered_scenarios) == 0:
        log.info("No scenarios to execute")
        return

    log.info("Executing %d scenarios", len(filtered_scenarios))
    results = []
    for (group, scenario_name), scenario in filtered_scenarios.items():
        log.info("Executing group.scenario '%s.%s' with elements %s",
                 group, scenario_name, list(scenario.keys()))
        for iter_result in suite.run(scenario, scenario_name, runner):
            iter_result["group"] = group
            iter_result["scenario"] = scenario_name
            results.append(iter_result)
    run = dict()
    run["suite"] = args.suite
    run["runner"] = runner.__class__.__name__
    run["runner_config"] = vars(runner.args)
    run.update(args.additional_run_fields)
    for result in results:
        jail.store_data(json.dumps(result))
    print("\n\n{}\n".format(suite.summary))


if __name__ == "__main__":
    main()
