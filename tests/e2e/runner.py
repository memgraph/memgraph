from argparse import ArgumentParser
import atexit
import logging
import os
from pathlib import Path
import subprocess
import yaml

from memgraph import MemgraphInstanceRunner

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", ".."))
BUILD_DIR = os.path.join(PROJECT_DIR, "build")
MEMGRAPH_BINARY = os.path.join(BUILD_DIR, "memgraph")

log = logging.getLogger("memgraph.tests.e2e")


def load_args():
    parser = ArgumentParser()
    parser.add_argument("--workloads-root-directory", required=True)
    parser.add_argument("--workload-name", default=None, required=False)
    return parser.parse_args()


def load_workloads(root_directory):
    workloads = []
    for file in Path(root_directory).rglob('*.yaml'):
        with open(file, "r") as f:
            workloads.extend(yaml.load(f, Loader=yaml.FullLoader)['workloads'])
    return workloads


def run(args):
    workloads = load_workloads(args.workloads_root_directory)
    for workload in workloads:
        workload_name = workload['name']
        if args.workload_name is not None and \
                args.workload_name != workload_name:
            continue
        log.info("%s STARTED.", workload_name)
        # Setup.
        mg_instances = {}

        @atexit.register
        def cleanup():
            for mg_instance in mg_instances.values():
                mg_instance.stop()
        for name, config in workload['cluster'].items():
            mg_instance = MemgraphInstanceRunner(MEMGRAPH_BINARY)
            mg_instances[name] = mg_instance
            log_file_path = os.path.join(BUILD_DIR, 'logs', config['log_file'])
            binary_args = config['args'] + ["--log-file", log_file_path]
            if 'proc' in workload:
              procdir = "--query-modules-directory=" + os.path.join(BUILD_DIR, workload['proc'])
              binary_args.append(procdir)

            mg_instance.start(args=binary_args)
            for query in config['setup_queries']:
                mg_instance.query(query)
        # Test.
        mg_test_binary = os.path.join(BUILD_DIR, workload['binary'])
        subprocess.run(
            [mg_test_binary] + workload['args'],
            check=True,
            stderr=subprocess.STDOUT)
        # Validation.
        for name, config in workload['cluster'].items():
            for validation in config['validation_queries']:
                mg_instance = mg_instances[name]
                data = mg_instance.query(validation['query'])[0][0]
                assert data == validation['expected']
        cleanup()
        log.info("%s PASSED.", workload_name)


if __name__ == '__main__':
    args = load_args()
    logging.basicConfig(
        level=logging.INFO,
        format='%(levelname)s %(asctime)s %(name)s] %(message)s')
    run(args)
