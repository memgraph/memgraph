import subprocess

import log
from benchmark_context import BenchmarkContext


def check_requirements(benchmark_context: BenchmarkContext):

    if "docker" in benchmark_context.vendor_name:

        log.info("Check requirements")
        command = ["docker", "info"]
        subprocess.run(command, check=True, capture_output=True, text=True)

    return True
