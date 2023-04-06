import zipfile
from pathlib import Path

import log


def zip_mgbench():
    log.info("Creating mgbench.zip ...")
    parent = Path(__file__).resolve().parent
    zip = zipfile.ZipFile("./mgbench.zip", "w")
    zip.write(parent / "benchmark.py", "mgbench/benchmark.py")
    zip.write(parent / "setup.py", "mgbench/setup.py")
    zip.write(parent / "log.py", "mgbench/log.py")
    zip.write(parent / "benchmark_context.py", "mgbench/benchmark_context.py")
    zip.write(parent / "validation.py", "mgbench/validation.py")
    zip.write(parent / "compare_results.py", "mgbench/compare_results.py")
    zip.write(parent / "runners.py", "mgbench/runners.py")
    zip.write(parent / "helpers.py", "mgbench/helpers.py")
    zip.write(parent / "graph_bench.py", "mgbench/graph_bench.py")
    zip.write(parent / "README.md", "mgbench/README.md")
    zip.write(parent / "how_to_use_mgbench.md", "mgbench/how_to_use_mgbench.md")
    zip.write(parent / "workloads/__init__.py", "mgbench/workloads/__init__.py")
    zip.write(parent / "workloads/base.py", "mgbench/workloads/base.py")
    zip.write(parent / "workloads/demo.py", "mgbench/workloads/demo.py")

    zip.close()


if __name__ == "__main__":
    zip_mgbench()

    if Path("./mgbench.zip").is_file():
        log.success("mgbench.zip created successfully")
    else:
        log.error("mgbench.zip was not created")
