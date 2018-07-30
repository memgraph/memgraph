#!/usr/bin/env python3
import atexit
import importlib
import logging
import os
import signal
import subprocess
import time
import xmlrpc.client

# workaround for xmlrpc max/min integer size
xmlrpc.client.MAXINT = 2**100
xmlrpc.client.MININT = -2**100

from argparse import ArgumentParser
from jail_service import JailService


def parse_args():
    """
    Parse command line arguments
    """
    argp = ArgumentParser(description=__doc__)
    argp.add_argument("--test-suite", default="card_fraud",
                      help="Tests suite")
    argp.add_argument("--test", default="example_test",
                      help="Test specification in python module")
    argp.add_argument("--machines-num", default="4",
                      help="Number of machines in cluster")
    return argp.parse_args()


def wait_for_server(interface, port, delay=0.1):
    cmd = ["nc", "-z", "-w", "1", interface, port]
    while subprocess.call(cmd) != 0:
        time.sleep(0.01)
    time.sleep(delay)


def main(args):
    workers = {}
    machine_ids = []
    machines_num = int(args.machines_num)

    # initialize workers
    for i in range(machines_num):
        id = i + 1
        machine_id = "MACHINE{id}".format(id=id)
        machine_ids.append(machine_id)
        machine_interface = os.environ[machine_id]
        machine_port = 8000 + id * 100

        if (id == 1):
            worker = JailService()
        else:
            host = "http://{interface}:{port}".format(
                interface=machine_interface,
                port=str(machine_port))
            worker = xmlrpc.client.ServerProxy(host)
            wait_for_server(machine_interface, str(machine_port))

        workers[machine_id] = worker

    # cleanup at exit
    @atexit.register
    def cleanup():
        for machine_id in machine_ids[1:]:
            try:
                workers[machine_id].shutdown()
            except ConnectionRefusedError:
                pass

    # run test
    test = importlib.import_module(
        "{suite}.{test}".format(suite=args.test_suite, test=args.test))
    test.run(machine_ids, workers)


if __name__ == "__main__":
    args = parse_args()
    main(args)
