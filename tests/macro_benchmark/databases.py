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
import shutil
import subprocess
import tempfile
import time
from argparse import ArgumentParser
from collections import defaultdict

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

    def __init__(self, args, num_workers):
        self.log = logging.getLogger("MemgraphRunner")
        argp = ArgumentParser("MemgraphArgumentParser")
        argp.add_argument("--runner-bin", default=get_absolute_path("memgraph", "build"))
        argp.add_argument("--port", default="7687", help="Database and client port")
        argp.add_argument("--data-directory", default=None)
        argp.add_argument("--storage-snapshot-on-exit", action="store_true")
        argp.add_argument("--data-recovery-on-startup", action="store_true")
        self.log.info("Initializing Runner with arguments %r", args)
        self.args, _ = argp.parse_known_args(args)
        self.num_workers = num_workers
        self.database_bin = jail.get_process()
        self.name = "memgraph"
        set_cpus("database-cpu-ids", self.database_bin, args)

    def start(self):
        self.log.info("start")
        database_args = ["--bolt-port", self.args.port, "--query-execution-timeout-sec", "0"]
        if self.num_workers:
            database_args += ["--bolt-num-workers", str(self.num_workers)]
        if self.args.data_directory:
            database_args += ["--data-directory", self.args.data_directory]
        if self.args.storage_recover_on_startup:
            database_args += ["--data-recovery-on-startup"]
        if self.args.storage_snapshot_on_exit:
            database_args += ["--storage-snapshot-on-exit"]

        # find executable path
        runner_bin = self.args.runner_bin

        # start memgraph
        self.database_bin.run(runner_bin, database_args, timeout=600)
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
        argp.add_argument("--runner-bin", default=get_absolute_path("neo4j/bin/neo4j", "libs"))
        argp.add_argument("--port", default="7687", help="Database and client port")
        argp.add_argument("--http-port", default="7474", help="Database and client port")
        self.log.info("Initializing Runner with arguments %r", args)
        self.args, _ = argp.parse_known_args(args)
        self.config = config
        self.database_bin = jail.get_process()
        self.name = "neo4j"
        set_cpus("database-cpu-ids", self.database_bin, args)

    def start(self):
        self.log.info("start")

        # create home directory
        self.neo4j_home_path = tempfile.mkdtemp(dir="/dev/shm")

        try:
            os.symlink(
                os.path.join(get_absolute_path("neo4j", "libs"), "lib"), os.path.join(self.neo4j_home_path, "lib")
            )
            neo4j_conf_dir = os.path.join(self.neo4j_home_path, "conf")
            neo4j_conf_file = os.path.join(neo4j_conf_dir, "neo4j.conf")
            os.mkdir(neo4j_conf_dir)
            shutil.copyfile(self.config, neo4j_conf_file)
            with open(neo4j_conf_file, "a") as f:
                f.write("\ndbms.connector.bolt.listen_address=:" + self.args.port + "\n")
                f.write("\ndbms.connector.http.listen_address=:" + self.args.http_port + "\n")

            # environment
            cwd = os.path.dirname(self.args.runner_bin)
            env = {"NEO4J_HOME": self.neo4j_home_path}

            self.database_bin.run(self.args.runner_bin, args=["console"], env=env, timeout=600, cwd=cwd)
        except:
            shutil.rmtree(self.neo4j_home_path)
            raise Exception("Couldn't run Neo4j!")

        wait_for_server(self.args.http_port, 2.0)

    def stop(self):
        self.database_bin.send_signal(jail.SIGTERM)
        self.database_bin.wait()
        if os.path.exists(self.neo4j_home_path):
            shutil.rmtree(self.neo4j_home_path)
