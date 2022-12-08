#!/usr/bin/env python3

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
from pathlib import Path
from typing import Any, Dict, List


def adapt_to_benchmark_action(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    output = []
    keys = list(data.keys())
    assert len(keys) == 1
    dataset_name = keys[0]

    dataset_keys = list(data[dataset_name])
    assert len(dataset_keys) == 1
    dataset_type = dataset_keys[0]

    for execution_type, execution_data in data[dataset_name][dataset_type].items():
        if execution_type == "__import__":
            continue
        for type, type_data in execution_data.items():
            assert "throughput" in type_data, f"There is no throughput in {type_data}!"
            output.append(
                {
                    "name": f"{dataset_name}_{dataset_type}_{execution_type}",
                    "unit": "QPS",
                    "value": type_data["throughput"],
                }
            )
    return output


ADAPTERS = {
    "github_benchmark": adapt_to_benchmark_action,
}


def convert_input(input: Dict[str, Any], adapter: str) -> List[Dict[str, Any]]:
    return ADAPTERS[adapter](input)


def parse_args():
    parser = argparse.ArgumentParser(
        description="JSON adapter.", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("-i", "--input", help="Input JSON file.")
    parser.add_argument("-o", "--output", help="Output JSON file.")
    parser.add_argument(
        "--adapter", default="github_benchmark", choices=ADAPTERS.keys(), help="Adapter function to use."
    )

    return parser.parse_args()


def read_input(input: str) -> Dict[str, Any]:
    with Path(input).open() as input_file:
        return json.load(input_file)


def output_results(content: List[Dict[str, Any]], output: str):
    with Path(output).open("w") as output_file:
        output_file.write(json.dumps(content))


def main():
    args = parse_args()
    content = read_input(args.input)
    output_results(convert_input(content, args.adapter), args.output)


if __name__ == "__main__":
    main()
