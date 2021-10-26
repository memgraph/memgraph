#!/usr/bin/env python3

# Copyright 2021 Memgraph Ltd.
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


FIELDS = [
    {
        "name": "throughput",
        "positive_diff_better": True,
        "scaling": 1,
        "unit": "QPS",
        "diff_treshold": 0.05,  # 5%
    },
    {
        "name": "duration",
        "positive_diff_better": False,
        "scaling": 1,
        "unit": "s",
    },
    {
        "name": "parsing_time",
        "positive_diff_better": False,
        "scaling": 1000,
        "unit": "ms",
    },
    {
        "name": "planning_time",
        "positive_diff_better": False,
        "scaling": 1000,
        "unit": "ms",
    },
    {
        "name": "plan_execution_time",
        "positive_diff_better": False,
        "scaling": 1000,
        "unit": "ms",
    },
    {
        "name": "memory",
        "positive_diff_better": False,
        "scaling": 1 / 1024 / 1024,
        "unit": "MiB",
        "diff_treshold": 0.02,  # 2%
    },
]


def load_results(fname):
    with open(fname) as f:
        return json.load(f)


def compute_diff(value_from, value_to):
    if value_from is None:
        return {"value": value_to}
    diff = (value_to - value_from) / value_from
    return {"value": value_to, "diff": diff}


def recursive_get(data, *args, value=None):
    for arg in args:
        if arg not in data:
            return value
        data = data[arg]
    return data


def compare_results(results_from, results_to, fields):
    ret = {}
    for dataset, variants in results_to.items():
        for variant, groups in variants.items():
            for group, scenarios in groups.items():
                if group == "__import__":
                    continue
                for scenario, summary_to in scenarios.items():
                    summary_from = recursive_get(
                        results_from, dataset, variant, group, scenario,
                        value={})
                    if len(summary_from) > 0 and \
                            summary_to["count"] != summary_from["count"] or \
                            summary_to["num_workers"] != \
                            summary_from["num_workers"]:
                        raise Exception("Incompatible results!")
                    testcode = "/".join([dataset, variant, group, scenario,
                                         "{:02d}".format(
                                             summary_to["num_workers"])])
                    row = {}
                    performance_changed = False
                    for field in fields:
                        key = field["name"]
                        if key in summary_to:
                            row[key] = compute_diff(
                                summary_from.get(key, None),
                                summary_to[key])
                        elif key in summary_to["database"]:
                            row[key] = compute_diff(
                                recursive_get(summary_from, "database", key,
                                              value=None),
                                summary_to["database"][key])
                        else:
                            row[key] = compute_diff(
                                recursive_get(summary_from, "metadata", key,
                                              "average", value=None),
                                summary_to["metadata"][key]["average"])
                        if "diff" not in row[key] or \
                                ("diff_treshold" in field and
                                 abs(row[key]["diff"]) >=
                                 field["diff_treshold"]):
                            performance_changed = True
                    if performance_changed:
                        ret[testcode] = row
    return ret


def generate_remarkup(fields, data):
    ret = "==== Benchmark summary: ====\n\n"
    if len(data) > 0:
        ret += "<table>\n"
        ret += "  <tr>\n"
        ret += "    <th>Testcode</th>\n"
        ret += "\n".join(map(lambda x: "    <th>{}</th>".format(
            x["name"].replace("_", " ").capitalize()), fields)) + "\n"
        ret += "  </tr>\n"
        for testcode in sorted(data.keys()):
            ret += "  <tr>\n"
            ret += "    <td>{}</td>\n".format(testcode)
            for field in fields:
                result = data[testcode][field["name"]]
                value = result["value"] * field["scaling"]
                if "diff" in result:
                    diff = result["diff"]
                    arrow = "arrow-up" if diff >= 0 else "arrow-down"
                    if not (field["positive_diff_better"] ^ (diff >= 0)):
                        color = "green"
                    else:
                        color = "red"
                    sign = "{{icon {} color={}}}".format(arrow, color)
                    ret += "    <td>{:.3f}{} //({:+.2%})// {}</td>\n".format(
                        value, field["unit"], diff, sign)
                else:
                    ret += "    <td>{:.3f}{} //(new)// " \
                           "{{icon plus color=blue}}</td>\n".format(
                               value, field["unit"])
            ret += "  </tr>\n"
        ret += "</table>\n"
    else:
        ret += "No performance change detected.\n"
    return ret


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Compare results of multiple benchmark runs.")
    parser.add_argument("--compare", action="append", nargs=2,
                        metavar=("from", "to"),
                        help="compare results between `from` and `to` files")
    parser.add_argument("--output", default="", help="output file name")
    args = parser.parse_args()

    if args.compare is None or len(args.compare) == 0:
        raise Exception("You must specify at least one pair of files!")

    data = {}
    for file_from, file_to in args.compare:
        results_from = load_results(file_from)
        results_to = load_results(file_to)
        data.update(compare_results(results_from, results_to, FIELDS))

    remarkup = generate_remarkup(FIELDS, data)
    if args.output:
        with open(args.output, "w") as f:
            f.write(remarkup)
    else:
        print(remarkup, end="")
