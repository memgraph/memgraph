import json
import os
import time

# to change the size of the cluster, just change this parameter
NUM_MACHINES = 3

# test setup
SCENARIOS = ["point_lookup", "create_tx"]
DURATION = 300
WORKERS = 6

# constants
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
MEMGRAPH_BINARY = "memgraph"
CLIENT_BINARY   = "tests/macro_benchmark/card_fraud_client"
BINARIES = [MEMGRAPH_BINARY, CLIENT_BINARY]

# wrappers
class WorkerWrapper:
    def __init__(self, address, worker):
        self._address = address
        self._worker = worker
        self._tid = worker.get_jail()

    def get_address(self):
        return self._address

    def __getattr__(self, name):
        if name in ["allocate_file", "read_file", "store_label"]:
            return getattr(self._worker, name)
        def func(*args, **kwargs):
            args = [self._tid] + list(args)
            return getattr(self._worker, name)(*args, **kwargs)
        return func

class MgCluster:
    def __init__(self, machine_ids, workers):
        # create wrappers
        self._master = WorkerWrapper(os.environ[machine_ids[0]],
                workers[machine_ids[0]])
        self._workers = []
        for machine_id in machine_ids[1:]:
            self._workers.append(WorkerWrapper(os.environ[machine_id],
                    workers[machine_id]))

    def start(self):
        # start memgraph master
        self._master.start(MEMGRAPH_BINARY, [
            "--master",
            "--master-host", self._master.get_address(),
            "--master-port", "10000",
            "--durability-directory", os.path.join(SCRIPT_DIR, "snapshots",
                                                   "worker_0"),
            "--db-recover-on-startup",
            "--query-vertex-count-to-expand-existing", "-1",
            "--num-workers", str(WORKERS),
            "--rpc-num-workers", str(WORKERS),
        ])

        # sleep to allow the master to startup
        time.sleep(5)

        # start memgraph workers
        for i, worker in enumerate(self._workers, start=1):
            worker.start(MEMGRAPH_BINARY, [
                "--worker", "--worker-id", str(i),
                "--worker-host", worker.get_address(),
                "--worker-port", str(10000 + i),
                "--master-host", self._master.get_address(),
                "--master-port", "10000",
                "--durability-directory", os.path.join(SCRIPT_DIR, "snapshots",
                                                       "worker_" + str(i)),
                "--db-recover-on-startup",
                "--num-workers", str(WORKERS),
                "--rpc-num-workers", str(WORKERS),
            ])

        # sleep to allow the workers to startup
        time.sleep(5)

        # store initial usage
        self._usage_start = [self._master.get_usage()]
        for worker in self._workers:
            self._usage_start.append(worker.get_usage())
        self._usage_start_time = time.time()

    def get_master_address(self):
        return self._master.get_address()

    def check_status(self):
        if not self._master.check_status():
            return False
        for worker in self._workers:
            if not worker.check_status():
                return False
        return True

    def stop(self):
        # store final usage
        self._usage_stop = [self._master.get_usage()]
        for worker in self._workers:
            self._usage_stop.append(worker.get_usage())
        self._usage_stop_time = time.time()

        # stop the master
        self._master.stop()

        # wait to allow the master and workers to die
        time.sleep(5)

        # stop the workers
        for worker in self._workers:
            worker.stop()

        # wait to allow the workers to die
        time.sleep(5)

    def get_usage(self):
        ret = []
        tdelta = self._usage_stop_time - self._usage_start_time
        for val_start, val_stop in zip(self._usage_start, self._usage_stop):
            data = {
                "cpu": (val_stop["cpu"] - val_start["cpu"]) / tdelta,
                "memory": val_stop["max_memory"] / 1024,
                "threads": val_stop["max_threads"],
                "network": {}
            }
            net_start = val_start["network"]["eth0"]
            net_stop = val_stop["network"]["eth0"]
            for i in ["bytes", "packets"]:
                data["network"][i] = {}
                for j in ["rx", "tx"]:
                    data["network"][i][j] = (net_stop[i][j] -
                            net_start[i][j]) / tdelta
            ret.append(data)
        return ret

    def store_label(self, label):
        self._master.store_label(label)
        for worker in self._workers:
            worker.store_label(label)

def write_scenario_summary(scenario, throughput, usage, output):
    output.write("Scenario **{}** throughput !!{:.2f}!! queries/s.\n\n".format(
            scenario, throughput))
    headers = ["Memgraph", "CPU", "Max memory", "Max threads",
            "Network RX", "Network TX"]
    output.write("<table>\n<tr>")
    for header in headers:
        output.write("<th>{}</th>".format(header))
    output.write("</tr>\n")
    for i, current in enumerate(usage):
        name = "master" if i == 0 else "worker" + str(i)
        output.write("<tr><td>{}</td>".format(name))
        for key, unit in [("cpu", "s/s"), ("memory", "MiB"), ("threads", "")]:
            fmt = ".2f" if key != "threads" else ""
            output.write(("<td>{:" + fmt + "} {}</td>").format(
                    current[key], unit).strip())
        for key in ["rx", "tx"]:
            output.write("<td>{:.2f} packets/s</td>".format(
                    current["network"]["packets"][key]))
        output.write("</tr>\n")
    output.write("</table>\n\n")

# main test function
def run(machine_ids, workers):
    # create output directory
    output_dir = os.path.join(SCRIPT_DIR, "output")
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    # create memgraph cluster and client
    mg_cluster = MgCluster(machine_ids, workers)
    mg_client = WorkerWrapper(os.environ[machine_ids[0]],
            workers[machine_ids[0]])

    # execute the tests
    stats = {}
    for scenario in SCENARIOS:
        output_file = os.path.join(output_dir, scenario + ".json")

        print("Starting memgraph cluster")
        mg_cluster.store_label("Start: cluster")
        mg_cluster.start()

        print("Starting client scenario:", scenario)
        mg_cluster.store_label("Start: " + scenario)
        mg_client.start(CLIENT_BINARY, [
            "--address", mg_cluster.get_master_address(),
            "--group", "card_fraud",
            "--scenario", scenario,
            "--duration", str(DURATION),
            "--num-workers", str(WORKERS),
            "--output", output_file,
        ])

        # wait for the client to terminate and check the cluster status
        while mg_client.check_status():
            assert mg_cluster.check_status(), "The memgraph cluster has died!"
            time.sleep(2)

        # stop everything
        mg_client.wait()
        mg_cluster.store_label("Stop: " + scenario)
        mg_cluster.stop()
        mg_cluster.store_label("Stop: cluster")

        # process the stats
        data = json.loads(list(filter(lambda x: x.strip(),
                open(output_file).read().split("\n")))[-1])
        throughput = data["num_executed_queries"] / data["elapsed_time"]
        usage = mg_cluster.get_usage()
        stats[scenario] = (throughput, usage)

    # dump the stats
    stats_file = open(os.path.join(output_dir, ".card_fraud_summary"), "w")
    stats_file.write("==== Distributed card fraud summary: ====\n\n")
    for scenario in SCENARIOS:
        throughput, usage = stats[scenario]
        write_scenario_summary(scenario, throughput, usage, stats_file)
    stats_file.close()
