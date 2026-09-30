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

import json
import logging
import os
import tempfile
import time

from common import CPU_TIME, MAX_MEMORY, WALL_TIME, get_absolute_path, set_cpus

log = logging.getLogger(__name__)

try:
    import jail
except:
    import jail_faker as jail


# This could be a function, not a class, but we want to reuse jail process since
# we can instantiate only 8 of them.
class QueryClient:
    def __init__(self, args, default_num_workers):
        self.log = logging.getLogger("QueryClient")
        self.client = jail.get_process()
        set_cpus("client-cpu-ids", self.client, args)
        self.default_num_workers = default_num_workers

    def __call__(self, queries, database, num_workers=None):
        if num_workers is None:
            num_workers = self.default_num_workers
        self.log.debug("execute('%s')", str(queries))

        client_path = "tests/macro_benchmark/query_client"
        client = get_absolute_path(client_path, "build")

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

        client_args = ["--port", database.args.port, "--num-workers", str(num_workers), "--output", output]

        cpu_time_start = database.database_bin.get_usage()["cpu"]
        # TODO make the timeout configurable per query or something
        return_code = self.client.run_and_wait(
            client, client_args, timeout=600, stdin=queries_path, env=os.environ.copy()
        )
        usage = database.database_bin.get_usage()
        cpu_time_end = usage["cpu"]
        os.remove(queries_path)
        if return_code != 0:
            with open(self.client.get_stderr()) as f:
                stderr = f.read()
            self.log.error(
                "Error while executing queries '%s'. " "Failed with return_code %d and stderr:\n%s",
                str(queries),
                return_code,
                stderr,
            )
            raise Exception("BoltClient execution failed")

        data = {"groups": []}
        with open(output) as f:
            for line in f:
                data["groups"].append(json.loads(line))
        data[CPU_TIME] = cpu_time_end - cpu_time_start
        data[MAX_MEMORY] = usage["max_memory"]

        os.remove(output)
        return data


class LongRunningClient:
    def __init__(self, args, default_num_workers, workload):
        self.log = logging.getLogger("LongRunningClient")
        self.client = jail.get_process()
        set_cpus("client-cpu-ids", self.client, args)
        self.default_num_workers = default_num_workers
        self.workload = workload

    # TODO: This is quite similar to __call__ method of QueryClient. Remove
    # duplication.
    def __call__(self, config, database, duration, client, num_workers=None):
        if num_workers is None:
            num_workers = self.default_num_workers
        self.log.debug("execute('%s')", config)

        client_path = "tests/macro_benchmark/{}".format(client)
        client = get_absolute_path(client_path, "build")

        config_fd, config_path = tempfile.mkstemp()
        try:
            config_file = os.fdopen(config_fd, "w")
            print(json.dumps(config, indent=4), file=config_file)
            config_file.close()
        except:
            config_file.close()
            os.remove(config_path)
            raise Exception("Writing config to temporary file failed")

        output_fd, output = tempfile.mkstemp()
        os.close(output_fd)

        client_args = [
            "--port",
            database.args.port,
            "--num-workers",
            str(num_workers),
            "--output",
            output,
            "--duration",
            str(duration),
            "--db",
            database.name,
            "--scenario",
            self.workload,
        ]

        return_code = self.client.run_and_wait(
            client, client_args, timeout=600, stdin=config_path, env=os.environ.copy()
        )
        os.remove(config_path)
        if return_code != 0:
            with open(self.client.get_stderr()) as f:
                stderr = f.read()
            self.log.error(
                "Error while executing config '%s'. " "Failed with return_code %d and stderr:\n%s",
                str(config),
                return_code,
                stderr,
            )
            raise Exception("BoltClient execution failed")

        # TODO: We shouldn't wait for process to finish to start reading output.
        # We should implement periodic reading of data and stream data when it
        # becomes available.
        data = []
        with open(output) as f:
            for line in f:
                data.append(json.loads(line))

        os.remove(output)
        return data
