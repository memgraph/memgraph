import subprocess
import sys
from subprocess import CalledProcessError

import log
from benchmark_context import BenchmarkContext


def check_requirements(benchmark_context: BenchmarkContext):

    if "docker" in benchmark_context.vendor_name:
        log.info("Checking requirements ... ")
        command = ["docker", "info"]
        try:
            subprocess.run(command, check=True, capture_output=True, text=True)
        except CalledProcessError:
            log.error("Docker is not installed or not running")
            return False

        if sys.version_info.major < 3 or sys.version_info.minor < 6:
            log.error("Python version 3.6 or higher is required")
            return False

    return True
