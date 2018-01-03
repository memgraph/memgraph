#!/usr/bin/env python3
import logging
import os
import signal
import subprocess
import sys
import tempfile
import traceback
import uuid
import xmlrpc.client
from common import get_absolute_path
from xmlrpc.server import SimpleXMLRPCServer

try:
    import jail
except:
    import jail_faker as jail


class XMLRPCServer(SimpleXMLRPCServer):
    def _dispatch(self, method, params):
        try:
            return super()._dispatch(method, params)
        except:
            traceback.print_exc()
            raise


class JailService:
    """
    Knows how to start and stop binaries
    """
    def __init__(self):
        logging.basicConfig(level=logging.INFO)
        self.log = logging.getLogger("JailService")
        self.log.info("Initializing Jail Service")
        self.processes = {}
        self._generated_filenames = []
        self.tempdir = tempfile.TemporaryDirectory()

    def start(self, tid, binary_name, binary_args=None):
        self.log.info("Starting Binary: {binary}".format(binary=binary_name))
        self.log.info("With args: {args}".format(args=binary_args))
        # find executable path
        binary = get_absolute_path(binary_name, "build")
        if not os.path.exists(binary):
            # Apollo builds both debug and release binaries on diff
            # so we need to use the release binary if the debug one
            # doesn't exist
            binary = get_absolute_path(binary_name, "build_release")

        # fetch process
        proc = self.processes[tid]

        # start binary
        proc.run(binary, args=binary_args, timeout=600)

        msg = "Binary {binary} successfully started with tid {tid}".format(
            binary=binary_name, tid=proc._tid)
        self.log.info(msg)

    def stop(self, tid):
        self.log.info("Stopping binary with tid {tid}".format(tid=tid))
        if tid not in self.processes:
            raise Exception(
                "Binary with tid {tid} does not exist".format(tid=tid))
        proc = self.processes[tid]
        proc.send_signal(jail.SIGTERM)
        proc.wait()
        self.log.info("Binary with tid {tid} stopped".format(tid=tid))

    def allocate_file(self, extension=""):
        if extension != "" and not extension.startswith("."):
            extension = "." + extension
        tmp_name = str(uuid.uuid4())
        while tmp_name in self._generated_filenames:
            tmp_name = str(uuid.uuid4())
        self._generated_filenames.append(tmp_name)
        absolute_path = os.path.join(self.tempdir.name, tmp_name + extension)
        return absolute_path

    def read_file(self, absolute_path):
        with open(absolute_path, "rb") as handle:
            return xmlrpc.client.Binary(handle.read())

    def get_jail(self):
        proc = jail.get_process()
        self.processes[proc._tid] = proc
        return proc._tid


def main():
    # set port dynamically
    port = os.environ["CURRENT_MACHINE"][len("MACHINE"):]
    port = 8000 + (int(port) * 100)
    interface = os.environ[os.environ["CURRENT_MACHINE"]]

    # init server
    server = XMLRPCServer((interface, port), allow_none=True, logRequests=False)
    server.register_introspection_functions()
    server.register_instance(JailService())

    # signal handler
    def signal_sigterm(signum, frame):
        server.server_close()
        sys.exit()

    try:
        signal.signal(signal.SIGTERM, signal_sigterm)
        server.serve_forever()
    except KeyboardInterrupt:
        server.server_close()


if __name__ == "__main__":
    main()
