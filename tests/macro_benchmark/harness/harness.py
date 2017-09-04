#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import os
import time
import itertools
import json
import subprocess
from argparse import ArgumentParser
from collections import OrderedDict
from collections import defaultdict
import tempfile
import shutil
from statistics import median

from perf import Perf

try:
    import jail
    APOLLO = True
except:
    import jail_faker as jail
    APOLLO = False

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
WALL_TIME = "wall_time"
CPU_TIME = "cpu_time"

log = logging.getLogger(__name__)


def get_absolute_path(path, base=""):
    if base == "build":
        extra = "../../../build"
    elif base == "build_release":
        extra = "../../../build_release"
    elif base == "libs":
        extra = "../../../libs"
    elif base == "config":
        extra = "../../../config"
    else:
        extra = ""
    return os.path.normpath(os.path.join(DIR_PATH, extra, path))


def wait_for_server(port, delay=1.0):
    cmd = ["nc", "-z", "-w", "1", "127.0.0.1", port]
    while subprocess.call(cmd) != 0:
        time.sleep(0.5)
    time.sleep(delay)


def load_scenarios(args):
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
    Each suite doesn't need to implement all query steps and filetypes.
    See documentation in each suite for supported ones.

    Args:
        args: additional args parsed by this function
        group_paths: str, root folder that contains group folders
    Return:
        {group: (scenario, {config: query_generator_function})
    """
    argp = ArgumentParser("QuerySuite.scenarios argument parser")
    argp.add_argument("--query-scenarios-root",
                      default=get_absolute_path("groups"), dest="root")
    args, _ = argp.parse_known_args()
    log.info("Loading query scenarios from root: %s", args.root)

    def fill_config_dict(config_dict, base, config_files):
        for config_file in config_files:
            log.debug("Processing config file %s", config_file)
            config_name = config_file.split(".")[-2]
            config_dict[config_name] = QuerySuite.Loader(
                os.path.join(base, config_file))

        # validate that the scenario does not contain any illegal
        # keys (defense against typos in file naming)
        unknown_keys = set(config_dict) - QuerySuite.KNOWN_KEYS
        if unknown_keys:
            raise Exception("Unknown QuerySuite config elements: '%r'" %
                            unknown_keys)

    def dir_content(root, predicate):
        return [p for p in os.listdir(root)
                if predicate(os.path.join(root, p))]

    group_scenarios = OrderedDict()
    for group in dir_content(args.root, os.path.isdir):
        log.info("Loading group: '%s'", group)

        group_scenarios[group] = []
        files = dir_content(os.path.join(args.root, group),
                            os.path.isfile)

        # process group default config
        group_config = {}
        fill_config_dict(group_config, os.path.join(args.root, group),
                         [f for f in files if f.count(".") == 1])

        # group files on scenario
        for scenario_name, scenario_files in itertools.groupby(
                filter(lambda f: f.count(".") == 2, sorted(files)),
                lambda x: x.split(".")[0]):
            log.info("Loading scenario: '%s'", scenario_name)
            scenario = dict(group_config)
            fill_config_dict(scenario,
                             os.path.join(args.root, group),
                             scenario_files)
            group_scenarios[group].append((scenario_name, scenario))
            log.debug("Loaded config for scenario '%s'\n%r", scenario_name,
                      scenario)

    return group_scenarios


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
              "{:>16}", "{:>16}"]
    FULL_FORMAT = "".join(FORMAT) + "\n"
    summary = FULL_FORMAT.format(
                      "group_name", "scenario_name", "parsing_time",
                      "planning_time", "plan_execution_time",
                      WALL_TIME, CPU_TIME)

    def __init__(self, args):
        if not APOLLO:
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
            _, extension = os.path.splitext(self.file_path)
            if extension == ".cypher":
                with open(self.file_path) as f:
                    return self._queries(f.read())
            elif extension == ".py":
                return self._queries(subprocess.check_output(
                    ["python3", self.file_path]).decode("ascii"))
            elif extension == ".json":
                with open(self.file_path) as f:
                    return [json.load(f)].__iter__()
            else:
                raise Exception("Unsupported filetype {} ".format(extension))

        def __repr__(self):
            return "(QuerySuite.Loader<%s>)" % self.file_path

    def run(self, scenario, group_name, scenario_name, runner):
        log.debug("QuerySuite.run() with scenario: %s", scenario)
        scenario_config = scenario.get("config")
        scenario_config = next(scenario_config()) if scenario_config else {}

        def execute(config_name, num_client_workers=1):
            queries = scenario.get(config_name)
            return runner.execute(queries(), num_client_workers) if queries \
                else None

        measurements = []

        measurement_lists = defaultdict(list)

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

        pid = runner.start()
        execute("setup")

        # warmup phase
        for _ in range(min(scenario_config.get("iterations", 1),
                           scenario_config.get("warmup", 2))):
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
            run_result = execute("run",
                                 scenario_config.get("num_client_workers", 1))
            add_measurement(run_result, iteration, WALL_TIME)
            add_measurement(run_result, iteration, CPU_TIME)
            for measurement in ["parsing_time",
                                "plan_execution_time",
                                "planning_time"] :
                for i in range(len(run_result.get("metadatas", []))):
                    add_measurement(run_result["metadatas"][i], iteration,
                                    measurement)
            execute("iterteardown")

        if self.perf:
            self.perf.stop()

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
                    "plan_execution_time", WALL_TIME, CPU_TIME)):
            if key not in measurement_lists:
                time = "-"
            else:
                # Median is used instead of avg to avoid effect of outliers.
                time = "{:.10f}".format(median(measurement_lists[key]))
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
        return ["MemgraphRunner", "NeoRunner"]

    def groups(self):
        return ["1000_create", "unwind_create", "match", "dense_expand",
                "expression", "aggregation", "return", "update", "delete"]


class QueryParallelSuite(_QuerySuite):
    def __init__(self, args):
        _QuerySuite.__init__(self, args)

    def runners(self):
        return ["MemgraphRunner", "NeoRunner"]

    def groups(self):
        return ["aggregation_parallel", "create_parallel"]


# Database wrappers.

class Memgraph:
    """
    Knows how to start and stop memgraph.
    """
    def __init__(self, args, cpus):
        self.log = logging.getLogger("MemgraphRunner")
        argp = ArgumentParser("MemgraphArgumentParser", add_help=False)
        argp.add_argument("--runner-bin",
                          default=get_absolute_path("memgraph", "build"))
        argp.add_argument("--runner-config",
                          default=get_absolute_path("benchmarking_latency.conf", "config"))
        argp.add_argument("--port", default="7687",
                          help="Database and client port")
        self.log.info("Initializing Runner with arguments %r", args)
        self.args, _ = argp.parse_known_args(args)
        self.database_bin = jail.get_process()
        self.database_bin.set_cpus(cpus)

    def start(self):
        self.log.info("start")
        env = {"MEMGRAPH_CONFIG": self.args.runner_config}
        database_args = ["--port", self.args.port]

        # find executable path
        runner_bin = self.args.runner_bin
        if not os.path.exists(runner_bin):
            # Apollo builds both debug and release binaries on diff
            # so we need to use the release binary if the debug one
            # doesn't exist
            runner_bin = get_absolute_path("memgraph", "build_release")

        # start memgraph
        self.database_bin.run(runner_bin, database_args, env=env, timeout=600)
        wait_for_server(self.args.port)
        return self.database_bin.get_pid() if not APOLLO else None

    def stop(self):
        self.database_bin.send_signal(jail.SIGTERM)
        self.database_bin.wait()


class Neo:
    def __init__(self, args, cpus):
        self.log = logging.getLogger("NeoRunner")
        argp = ArgumentParser("NeoArgumentParser", add_help=False)
        argp.add_argument("--runner-bin", default=get_absolute_path(
                          "neo4j/bin/neo4j", "libs"))
        argp.add_argument("--runner-config",
                          default=get_absolute_path("config/neo4j.conf"))
        argp.add_argument("--port", default="7687",
                          help="Database and client port")
        argp.add_argument("--http-port", default="7474",
                          help="Database and client port")
        self.log.info("Initializing Runner with arguments %r", args)
        self.args, _ = argp.parse_known_args(args)
        self.database_bin = jail.get_process()
        self.database_bin.set_cpus(cpus)

    def start(self):
        self.log.info("start")

        # create home directory
        self.neo4j_home_path = tempfile.mkdtemp(dir="/dev/shm")

        try:
            os.symlink(os.path.join(get_absolute_path("neo4j", "libs"), "lib"),
                       os.path.join(self.neo4j_home_path, "lib"))
            neo4j_conf_dir = os.path.join(self.neo4j_home_path, "conf")
            neo4j_conf_file = os.path.join(neo4j_conf_dir, "neo4j.conf")
            os.mkdir(neo4j_conf_dir)
            shutil.copyfile(self.args.runner_config, neo4j_conf_file)
            with open(neo4j_conf_file, "a") as f:
                f.write("\ndbms.connector.bolt.listen_address=:" +
                        self.args.port + "\n")
                f.write("\ndbms.connector.http.listen_address=:" +
                        self.args.http_port + "\n")

            # environment
            cwd = os.path.dirname(self.args.runner_bin)
            env = {"NEO4J_HOME": self.neo4j_home_path}

            self.database_bin.run(self.args.runner_bin, args=["console"],
                                  env=env, timeout=600, cwd=cwd)
        except:
            shutil.rmtree(self.neo4j_home_path)
            raise Exception("Couldn't run Neo4j!")

        wait_for_server(self.args.http_port, 2.0)
        return self.database_bin.get_pid() if not APOLLO else None

    def stop(self):
        self.database_bin.send_signal(jail.SIGTERM)
        self.database_bin.wait()
        if os.path.exists(self.neo4j_home_path):
            shutil.rmtree(self.neo4j_home_path)


class Postgres:
    """
    Knows how to start and stop PostgreSQL.
    """
    def __init__(self, args, cpus):
        self.log = logging.getLogger("PostgresRunner")
        argp = ArgumentParser("PostgresArgumentParser", add_help=False)
        argp.add_argument("--init-bin", default=get_absolute_path(
                          "postgresql/bin/initdb", "libs"))
        argp.add_argument("--runner-bin", default=get_absolute_path(
                          "postgresql/bin/postgres", "libs"))
        argp.add_argument("--port", default="5432",
                          help="Database and client port")
        self.log.info("Initializing Runner with arguments %r", args)
        self.args, _ = argp.parse_known_args(args)
        self.username = "macro_benchmark"
        self.database_bin = jail.get_process()
        self.database_bin.set_cpus(cpus)

    def start(self):
        self.log.info("start")
        self.data_path = tempfile.mkdtemp(dir="/dev/shm")
        init_args = ["-D", self.data_path, "-U", self.username]
        self.database_bin.run_and_wait(self.args.init_bin, init_args)

        # args
        runner_args = ["-D", self.data_path, "-c", "port=" + self.args.port,
                       "-c", "ssl=false", "-c", "max_worker_processes=1"]

        try:
            self.database_bin.run(self.args.runner_bin, args=runner_args,
                                  timeout=600)
        except:
            shutil.rmtree(self.data_path)
            raise Exception("Couldn't run PostgreSQL!")

        wait_for_server(self.args.port)
        return self.database_bin.get_pid() if not APOLLO else None

    def stop(self):
        self.database_bin.send_signal(jail.SIGTERM)
        self.database_bin.wait()
        if os.path.exists(self.data_path):
            shutil.rmtree(self.data_path)


class _HarnessClientRunner:
    """
    Knows how to start and stop database (backend) some client frontend (bolt),
    and execute a cypher query.
    Execution returns benchmarking data (execution times, memory
    usage etc).
    Inherited class should implement start method and initialise database_bin
    and bolt_client members of type Process.
    """
    def __init__(self, args, database, cpus=None):
        if cpus is None: cpus = [2, 3]
        self.log = logging.getLogger("_HarnessClientRunner")
        self.database = database
        argp = ArgumentParser("RunnerArgumentParser", add_help=False)
        self.args, _ = argp.parse_known_args()
        self.bolt_client = jail.get_process()
        self.bolt_client.set_cpus(cpus)

    def start(self):
        self.database.start()

    def execute(self, queries, num_client_workers):
        self.log.debug("execute('%s')", str(queries))

        client_path = "tests/macro_benchmark/harness_client"
        client = get_absolute_path(client_path, "build")
        if not os.path.exists(client):
            # Apollo builds both debug and release binaries on diff
            # so we need to use the release client if the debug one
            # doesn't exist
            client = get_absolute_path(client_path, "build_release")

        queries_fd, queries_path = tempfile.mkstemp()
        try:
            queries_file = os.fdopen(queries_fd, "w")
            queries_file.write("\n".join(queries))
            queries_file.close()
        except:
            queries_file.close()
            os.remove(queries_path)
            raise Exception("Writing queries to temporary file failed")

        output_fd, output = tempfile.mkstemp()
        os.close(output_fd)

        client_args = ["--port", self.database.args.port,
                       "--num-workers", str(num_client_workers),
                       "--output", output]

        cpu_time_start = self.database.database_bin.get_usage()["cpu"]
        # TODO make the timeout configurable per query or something
        return_code = self.bolt_client.run_and_wait(
            client, client_args, timeout=600, stdin=queries_path)
        cpu_time_end = self.database.database_bin.get_usage()["cpu"]
        os.remove(queries_path)
        if return_code != 0:
            with open(self.bolt_client.get_stderr()) as f:
                stderr = f.read()
            self.log.error("Error while executing queries '%s'. "
                           "Failed with return_code %d and stderr:\n%s",
                           str(queries), return_code, stderr)
            raise Exception("BoltClient execution failed")

        with open(output) as f:
            data = json.loads(f.read())
        data[CPU_TIME] = cpu_time_end - cpu_time_start

        os.remove(output)
        return data

    def stop(self):
        self.log.info("stop")
        self.bolt_client.wait()
        self.database.stop()


class MemgraphRunner(_HarnessClientRunner):
    def __init__(self, args, client_cpus=None, database_cpus=None):
        if database_cpus is None: database_cpus = [1]
        database = Memgraph(args, database_cpus)
        super(MemgraphRunner, self).__init__(args, database, cpus=client_cpus)


class NeoRunner(_HarnessClientRunner):
    def __init__(self, args, client_cpus=None, database_cpus=None):
        if database_cpus is None: database_cpus = [1]
        database = Neo(args, database_cpus)
        super(NeoRunner, self).__init__(args, database, cpus=client_cpus)


def main():
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
    argp.add_argument("--no-strict", default=False, action="store_true",
                      help="Ignores nonexisting groups instead of raising an "
                           "exception")
    args, remaining_args = argp.parse_known_args()

    if args.logging:
        logging.basicConfig(level=args.logging)
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("neo4j.bolt").setLevel(logging.WARNING)
    log.info("Memgraph benchmark suite harness")
    log.info("Executing for suite '%s', runner '%s'", args.suite, args.runner)

    # Create suites.
    suites = {"QuerySuite": QuerySuite,
              "QueryParallelSuite": QueryParallelSuite}
    if args.suite not in suites:
        raise Exception(
            "Suite '{}' isn't registered. Registered suites are: {}".format(
                args.suite, suites))
    suite = suites[args.suite](remaining_args)

    # Load scenarios.
    group_scenarios = load_scenarios(remaining_args)
    log.info("Loaded %d groups, with a total of %d scenarios",
             len(group_scenarios),
             sum([len(x) for x in group_scenarios.values()]))

    # Create runners.
    runners = {"MemgraphRunner": MemgraphRunner, "NeoRunner": NeoRunner}
    if args.runner not in suite.runners():
        raise Exception("Runner '{}' not registered for suite '{}'".format(
            args.runner, args.suite))
    runner = runners[args.runner](remaining_args)

    # Validate groups (if provided).
    groups = []
    if args.groups:
        for group in args.groups:
            if group not in suite.groups():
                msg = "Group '{}' isn't registered for suite '{}'".format(
                        group, suite)
                if args.no_strict:
                    log.warn(msg)
                else:
                    raise Exception(msg)
            else:
                groups.append(group)
    else:
        # No groups provided, use all suite group
        groups = suite.groups()

    # Filter scenarios.
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

    # Run scenarios.
    log.info("Executing %d scenarios", len(filtered_scenarios))
    results = []
    for (group, scenario_name), scenario in sorted(filtered_scenarios.items()):
        log.info("Executing group.scenario '%s.%s' with elements %s",
                 group, scenario_name, list(scenario.keys()))
        for iter_result in suite.run(scenario, group, scenario_name, runner):
            iter_result["group"] = group
            iter_result["scenario"] = scenario_name
            results.append(iter_result)

    # Save results.
    run = dict()
    run["suite"] = args.suite
    run["runner"] = runner.__class__.__name__
    run["runner_config"] = vars(runner.args)
    run.update(args.additional_run_fields)
    for result in results:
        jail.store_data(result)

    # Print summary.
    print("\n\nMacro benchmark summary:")
    print("{}\n".format(suite.summary))
    with open(get_absolute_path(".harness_summary"), "w") as f:
        print(suite.summary, file=f)


if __name__ == "__main__":
    main()
