import logging
import os
import time
import xmlrpc.client

NUM_MACHINES = 2

# binaries to run
CLIENT_BINARY = "tests/distributed/raft/example_client"
SERVER_BINARY = "tests/distributed/raft/example_server"


def run(machine_ids, workers):
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger("example_test")
    log.info("Start")

    # define interfaces and ports for binaries
    server_interface = os.environ[machine_ids[1]]
    server_port = str(10000)
    client_interface = os.environ[machine_ids[0]]
    client_port = str(10010)

    # start binaries
    log_abs_path = workers[machine_ids[1]].allocate_file()
    server_tid = workers[machine_ids[1]].get_jail()
    server_args = ["--interface", server_interface]
    server_args += ["--port", server_port]
    server_args += ["--log", log_abs_path]
    workers[machine_ids[1]].start(server_tid, SERVER_BINARY, server_args)

    client_tid = workers[machine_ids[0]].get_jail()
    client_args = ["--interface", client_interface]
    client_args += ["--port", client_port]
    client_args += ["--server-interface", server_interface]
    client_args += ["--server-port", server_port]
    workers[machine_ids[0]].start(client_tid, CLIENT_BINARY, client_args)

    # crash server
    workers[machine_ids[1]].stop(server_tid)
    time.sleep(5)
    workers[machine_ids[1]].start(server_tid, SERVER_BINARY, server_args)

    # wait for test to finish
    time.sleep(5)

    # stop binaries
    workers[machine_ids[0]].stop(client_tid)
    workers[machine_ids[1]].stop(server_tid)

    # fetch log
    result = workers[machine_ids[1]].read_file(log_abs_path)
    if result is not None:
        local_log = "local_log.txt"
        result = result.data.decode('ascii')
        if result.splitlines() == ["{}".format(x) for x in range(1, 101)]:
            log.warn("Test successful")
        else:
            raise Exception("Test failed")

    log.info("End")
