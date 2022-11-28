# Copyright 2022 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import argparse
import json
import sys
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

import matplotlib.pyplot as plt


class Operation(Enum):
    CONTAINS = "contains"
    FIND = "find"
    INSERT = "insert"
    RANDOM = "random"
    REMOVE = "remove"

    @classmethod
    def to_list(cls) -> List[str]:
        return list(map(lambda c: c.value, cls))

    @staticmethod
    def get(s: str) -> Optional["Operation"]:
        try:
            return Operation[s.upper()]
        except ValueError:
            return None

    def __str__(self):
        return str(self.value)


@dataclass(frozen=True)
class BenchmarkRow:
    name: str
    datastructure: str
    operation: Operation
    real_time: int
    cpu_time: int
    iterations: int
    time_unit: str
    run_arg: Optional[Any]


class GoogleBenchmarkResult:
    def __init__(self):
        self._operation = None
        self._datastructures: Dict[str, List[BenchmarkRow]] = dict()

    def add_result(self, row: BenchmarkRow) -> None:
        if self._operation is None:
            self._operation = row.operation
        assert self._operation is row.operation
        if row.datastructure not in self._datastructures:
            self._datastructures[row.datastructure] = [row]
        else:
            self._datastructures[row.datastructure].append(row)

    @property
    def operation(self) -> Optional[Operation]:
        return self._operation

    @property
    def datastructures(self) -> Dict[str, List[BenchmarkRow]]:
        return self._datastructures


def get_operation(s: str) -> Operation:
    for op in Operation.to_list():
        if op.lower() in s.lower():
            operation_enum = Operation.get(op)
            if operation_enum is not None:
                return operation_enum
            else:
                print("Operation not found!")
                sys.exit(1)
    print("Operation not found!")
    sys.exit(1)


def get_row_data(line: Dict[str, Any]) -> BenchmarkRow:
    """
    Naming is very important, first must come an Operation name, and then a data
    structure to test.
    """
    full_name = line["name"].split("BM_Benchmark")[1]
    name_with_run_arg = full_name.split("/")
    operation = get_operation(name_with_run_arg[0])
    datastructure = name_with_run_arg[0].split(operation.value.capitalize())[1]

    run_arg = None
    if len(name_with_run_arg) > 1:
        run_arg = name_with_run_arg[1]

    return BenchmarkRow(
        name_with_run_arg[0],
        datastructure,
        operation,
        line["real_time"],
        line["cpu_time"],
        line["iterations"],
        line["time_unit"],
        run_arg,
    )


def get_benchmark_res(args) -> Optional[GoogleBenchmarkResult]:
    file_path = Path(args.log_file)
    if not file_path.exists():
        print("Error file {file_path} not found!")
        return None
    with file_path.open("r") as file:
        data = json.load(file)
        res = GoogleBenchmarkResult()
        assert "benchmarks" in data, "There must be a benchmark list inside"
        for benchmark in data["benchmarks"]:
            res.add_result(get_row_data(benchmark))
        return res


def plot_operation(results: GoogleBenchmarkResult, save: bool) -> None:
    colors = ["red", "green", "blue", "yellow", "purple", "brown"]
    assert results.operation is not None
    fig = plt.figure()
    for ds, benchmarks in results.datastructures.items():
        if benchmarks:
            # Print line chart
            x_axis = [elem.real_time for elem in benchmarks]
            y_axis = [elem.run_arg for elem in benchmarks]
            plt.plot(x_axis, y_axis, marker="", color=colors.pop(0), linewidth="2", label=f"{ds}")
            plt.title(f"Benchmark results for operation {results.operation.value}")
            plt.xlabel(f"Time [{benchmarks[0].time_unit}]")
            plt.legend()
        else:
            print(f"Nothing to do for {ds}...")
    if save:
        plt.savefig(f"{results.operation.value}.png")
        plt.close(fig)
    else:
        plt.show()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process benchmark results.")
    parser.add_argument("--log_file", type=str)
    parser.add_argument("--save", type=bool, default=True)
    return parser.parse_args()


def main():
    args = parse_args()
    res = get_benchmark_res(args)
    if res is None:
        print("Failed to get results from log file!")
        sys.exit(1)
    plot_operation(res, args.save)


if __name__ == "__main__":
    main()
