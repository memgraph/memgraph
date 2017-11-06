import logging
import os
import subprocess
from argparse import ArgumentParser
from collections import defaultdict
import tempfile
import shutil
import time
from common import get_absolute_path, set_cpus

try:
    import jail
except:
    import jail_faker as jail


def wait_for_server(port, delay=0.1):
    cmd = ["nc", "-z", "-w", "1", "127.0.0.1", port]
    while subprocess.call(cmd) != 0:
        time.sleep(0.01)
    time.sleep(delay)


class Memgraph:
    """
    Knows how to start and stop memgraph.
    """
    def __init__(self, args, config, num_workers):
        self.log = logging.getLogger("MemgraphRunner")
        argp = ArgumentParser("MemgraphArgumentParser")
        argp.add_argument("--runner-bin",
                          default=get_absolute_path("memgraph", "build"))
        argp.add_argument("--port", default="7687",
                          help="Database and client port")
        self.log.info("Initializing Runner with arguments %r", args)
        self.args, _ = argp.parse_known_args(args)
        self.config = config
        self.num_workers = num_workers
        self.database_bin = jail.get_process()
        set_cpus("database-cpu-ids", self.database_bin, args)

    def start(self):
        self.log.info("start")
        env = {"MEMGRAPH_CONFIG": self.config}
        database_args = ["--port", self.args.port]
        if self.num_workers:
            database_args += ["--num_workers", str(self.num_workers)]

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

    def stop(self):
        self.database_bin.send_signal(jail.SIGTERM)
        self.database_bin.wait()


class Neo:
    """
    Knows how to start and stop neo4j.
    """
    def __init__(self, args, config):
        self.log = logging.getLogger("NeoRunner")
        argp = ArgumentParser("NeoArgumentParser")
        argp.add_argument("--runner-bin", default=get_absolute_path(
                          "neo4j/bin/neo4j", "libs"))
        argp.add_argument("--port", default="7687",
                          help="Database and client port")
        argp.add_argument("--http-port", default="7474",
                          help="Database and client port")
        self.log.info("Initializing Runner with arguments %r", args)
        self.args, _ = argp.parse_known_args(args)
        self.config = config
        self.database_bin = jail.get_process()
        set_cpus("database-cpu-ids", self.database_bin, args)

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
            shutil.copyfile(self.config, neo4j_conf_file)
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
        argp = ArgumentParser("PostgresArgumentParser")
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
        set_cpus("database-cpu-ids", self.database_bin, args)

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

    def stop(self):
        self.database_bin.send_signal(jail.SIGTERM)
        self.database_bin.wait()
        if os.path.exists(self.data_path):
            shutil.rmtree(self.data_path)
