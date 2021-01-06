import copy
import os
import subprocess
import sys
import tempfile
import time

import mgclient

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", ".."))
BUILD_DIR = os.path.join(PROJECT_DIR, "build")
MEMGRAPH_BINARY = os.path.join(BUILD_DIR, "memgraph")


def wait_for_server(port, delay=0.01):
    cmd = ["nc", "-z", "-w", "1", "127.0.0.1", str(port)]
    count = 0
    while subprocess.call(cmd) != 0:
        time.sleep(0.01)
        if count > 10 / 0.01:
            print("Could not wait for server on port", port, "to startup!")
            sys.exit(1)
        count += 1
    time.sleep(delay)


def extract_bolt_port(args):
    # TODO(gitbuda): Handle when args is --bolt-port=PORT.
    try:
        return int(args[args.index('--bolt-port') + 1])
    except ValueError:
        pass
    return 7687


class MemgraphInstanceRunner():
    def __init__(self, binary_path=MEMGRAPH_BINARY, args=[]):
        self.host = '127.0.0.1'
        self.bolt_port = extract_bolt_port(args)
        self.binary_path = binary_path
        self.args = args
        self.proc_mg = None
        self.conn = None

    def query(self, query):
        cursor = self.conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()

    def start(self, restart=False, args=[]):
        if args == self.args and self.is_running():
            return
        self.stop()
        self.args = copy.deepcopy(args)
        self.data_directory = tempfile.TemporaryDirectory()
        args_mg = [self.binary_path,
                   "--data-directory", self.data_directory.name,
                   "--storage-wal-enabled",
                   "--storage-snapshot-interval-sec", "300",
                   "--storage-properties-on-edges"] + self.args
        self.bolt_port = extract_bolt_port(args_mg)
        self.proc_mg = subprocess.Popen(args_mg)
        wait_for_server(self.bolt_port)
        self.conn = mgclient.connect(
            host=self.host,
            port=self.bolt_port)
        self.conn.autocommit = True
        assert self.is_running(), "The Memgraph process died!"

    def is_running(self):
        if self.proc_mg is None:
            return False
        if self.proc_mg.poll() is not None:
            return False
        return True

    def stop(self):
        if not self.is_running():
            return
        self.proc_mg.terminate()
        code = self.proc_mg.wait()
        assert code == 0, "The Memgraph process exited with non-zero!"
