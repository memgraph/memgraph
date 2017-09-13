import logging
import os
import time
import json
import tempfile
from common import get_absolute_path, WALL_TIME, CPU_TIME, MAX_MEMORY

log = logging.getLogger(__name__)

try:
    import jail
    APOLLO = True
except:
    import jail_faker as jail
    APOLLO = False


# This could be a function, not a class, but we want to reuse jail process since
# we can instantiate only 8 of them.
class QueryClient:
    def __init__(self, args, cpus=None):
        self.log = logging.getLogger("QueryClient")
        self.client = jail.get_process()
        if cpus:
            self.client.set_cpus(cpus)

    def __call__(self, queries, database, num_client_workers):
        self.log.debug("execute('%s')", str(queries))

        client_path = "tests/macro_benchmark/query_client"
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

        client_args = ["--port", database.args.port,
                       "--num-workers", str(num_client_workers),
                       "--output", output]

        cpu_time_start = database.database_bin.get_usage()["cpu"]
        # TODO make the timeout configurable per query or something
        return_code = self.client.run_and_wait(
            client, client_args, timeout=600, stdin=queries_path)
        usage = database.database_bin.get_usage()
        cpu_time_end = usage["cpu"]
        os.remove(queries_path)
        if return_code != 0:
            with open(self.client.get_stderr()) as f:
                stderr = f.read()
            self.log.error("Error while executing queries '%s'. "
                           "Failed with return_code %d and stderr:\n%s",
                           str(queries), return_code, stderr)
            raise Exception("BoltClient execution failed")

        with open(output) as f:
            data = json.loads(f.read())
        data[CPU_TIME] = cpu_time_end - cpu_time_start
        data[MAX_MEMORY] = usage["max_memory"]

        os.remove(output)
        return data


class LongRunningClient:
    def __init__(self, args, cpus=None):
        self.log = logging.getLogger("LongRunningClient")
        self.client = jail.get_process()
        if cpus:
            self.client.set_cpus(cpus)

    # TODO: This is quite similar to __call__ method of QueryClient. Remove
    # duplication.
    def __call__(self, config, database, duration, num_client_workers):
        self.log.debug("execute('%s')", config)

        client_path = "tests/macro_benchmark/long_running_client"
        client = get_absolute_path(client_path, "build")
        if not os.path.exists(client):
            # Apollo builds both debug and release binaries on diff
            # so we need to use the release client if the debug one
            # doesn't exist
            client = get_absolute_path(client_path, "build_release")

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

        client_args = ["--port", database.args.port,
                       "--num-workers", str(num_client_workers),
                       "--output", output,
                       "--duration", str(duration)]

        return_code = self.client.run_and_wait(
            client, client_args, timeout=600, stdin=config_path)
        os.remove(config_path)
        if return_code != 0:
            with open(self.client.get_stderr()) as f:
                stderr = f.read()
            self.log.error("Error while executing config '%s'. "
                           "Failed with return_code %d and stderr:\n%s",
                           str(config), return_code, stderr)
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
