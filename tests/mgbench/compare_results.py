#!/usr/bin/env python3

# Copyright 2023 Memgraph Ltd.
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


def compare_results(results_from, results_to, fields, ignored, different_vendors):
    ret = {}
    for dataset, variants in results_to.items():
        if dataset == "__run_configuration__":
            continue
        for variant, groups in variants.items():
            for group, scenarios in groups.items():
                if group == "__import__":
                    continue
                for scenario, summary_to in scenarios.items():
                    if scenario in ignored:
                        continue

                    summary_from = recursive_get(results_from, dataset, variant, group, scenario, value={})
                    summary_from = summary_from["without_fine_grained_authorization"]
                    summary_to = summary_to["without_fine_grained_authorization"]
                    if (
                        len(summary_from) > 0
                        and (summary_to["count"] != summary_from["count"] and not different_vendors)
                        or summary_to["num_workers"] != summary_from["num_workers"]
                    ):
                        raise Exception("Incompatible results!")
                    testcode = "/".join(
                        [
                            dataset,
                            variant,
                            group,
                            scenario,
                            "{:02d}".format(summary_to["num_workers"]),
                        ]
                    )
                    row = {}
                    performance_changed = False
                    for field in fields:
                        key = field["name"]
                        if key in summary_to:
                            row[key] = compute_diff(summary_from.get(key, None), summary_to[key])
                        elif key in summary_to["database"]:
                            row[key] = compute_diff(
                                recursive_get(summary_from, "database", key, value=None),
                                summary_to["database"][key],
                            )
                        elif summary_to.get("latency_stats") != None and key in summary_to["latency_stats"]:
                            row[key] = compute_diff(
                                recursive_get(summary_from, "latency_stats", key, value=None),
                                summary_to["latency_stats"][key],
                            )
                        elif not different_vendors:
                            row[key] = compute_diff(
                                recursive_get(summary_from, "metadata", key, "average", value=None),
                                summary_to["metadata"][key]["average"],
                            )
                        if row.get(key) != None and (
                            "diff" not in row[key]
                            or ("diff_treshold" in field and abs(row[key]["diff"]) >= field["diff_treshold"])
                        ):
                            performance_changed = True
                    if performance_changed:
                        ret[testcode] = row
    return ret


def _generate_th_tag(others_len):
    ret = "<th>Reference vendor</th>\n"
    for i in range(others_len):
        ret += f"<th>Vendor {i}</th>\n"
    return ret


def _generate_td_tag(reference, others, property):
    ret = f"<td>{reference[property]}</td>\n"
    for other in others:
        ret += f"<td>{other[property]}</td>\n"
    return ret


def generate_remarkup(fields, data, results_reference=None, results_other=None):
    ret = "<html>\n"
    ret += """
        <style>
            table, th, td {
            border: 1px solid black;
            }
        </style>
        """
    ret += "<h1>Benchmark comparison</h1>\n"
    if results_reference and results_other:
        ret += """
        <h2>Benchmark configuration</h2>
        <table>
            <tr>
                <th>Configuration</th>
                {}
            </tr>
            <tr>
                <td>Vendor name</td>
                {}
            </tr>
            <tr>
                <td>Vendor condition</td>
                {}
            </tr>
            <tr>
                <td>Number of workers</td>
                {}
            </tr>
            <tr>
                <td>Single threaded runtime</td>
                {}
            </tr>
            <tr>
                <td>Platform</td>
                {}
            </tr>
        </table>
        """.format(
            _generate_th_tag(len(results_other)),
            _generate_td_tag(results_reference, results_other, "vendor"),
            _generate_td_tag(results_reference, results_other, "condition"),
            _generate_td_tag(results_reference, results_other, "num_workers_for_benchmark"),
            _generate_td_tag(results_reference, results_other, "single_threaded_runtime_sec"),
            _generate_td_tag(results_reference, results_other, "platform"),
        )
        ret += """
        <h2>How to read benchmark results</h2>
        <b> Throughput and latency values:</b>
        <p> If the given vendor is faster than the reference vendor <b> {}</b>, the result for throughput and latency are show in <b style="color:#008000">green </b>, otherwise <b style="color:#FF0000">red</b>. Percentage difference is visible relative to reference vendor {}. </p>
        <b> Memory usage:</b>
        <p> If the given vendor uses less memory then the reference vendor <b> {}</b>, the result is shown in  <b style="color:#008000">green </b>, otherwise <b style="color:#FF0000">red </b>. Percentage difference for memory is visible relative to reference vendor {}.
        """.format(
            results_reference["vendor"],
            results_reference["vendor"],
            results_reference["vendor"],
            results_reference["vendor"],
        )

    wanted_stats = ["throughput", "memory", "max", "p99", "p90", "mean"]

    ret += "<html>\n"
    ret += "<table>\n"
    ret += "  <tr>\n"
    ret += "    <th>Testcode</th>\n"
    for vendor_id in range(len(data)):
        ret += f"    <th>Vendor {vendor_id}</th>\n"
    ret += "  </tr>\n"

    # Iterate over each wanted stat
    for wanted_stat in wanted_stats:
        ret += f"  <tr>\n    <td colspan='{len(data) + 1}'><h2>Benchmark results: Comparison for {wanted_stat.capitalize()}</h2></td>\n  </tr>\n"

        # Iterate over each testcode for the first vendor to ensure all vendors have the same testcodes
        for testcode in sorted(data[0].keys()):
            ret += "  <tr>\n"
            ret += f"    <td>{testcode}</td>\n"

            # Iterate over each vendor and compare their stats for the current wanted_stat
            for vendor_id, comparison in enumerate(data):
                vendor_stat = comparison.get(testcode)

                if vendor_stat is not None and wanted_stat in vendor_stat:
                    value_diff = vendor_stat[wanted_stat]["diff"]
                    value = vendor_stat[wanted_stat]["value"]
                    ret += f"    <td>{value} ({value_diff:.3f})</td>\n"
                else:
                    ret += "    <td>No data available</td>\n"

            ret += "  </tr>\n"

    ret += "</table>\n"
    ret += "</html>\n"

    for vendor_id, comparison in enumerate(data):
        ret += f"<h2>Benchmark results: Reference vendor vs Vendor {vendor_id}</h2>\n"
        if len(comparison) > 0:
            ret += "<table>\n"
            ret += "  <tr>\n"
            ret += "    <th>Testcode</th>\n"
            ret += (
                "\n".join(
                    map(
                        lambda x: "    <th>{}</th>".format(x["name"].replace("_", " ").capitalize()),
                        fields,
                    )
                )
                + "\n"
            )
            ret += "  </tr>\n"
            for testcode in sorted(comparison.keys()):
                ret += "  <tr>\n"
                ret += "    <td>{}</td>\n".format(testcode)
                for field in fields:
                    result = comparison[testcode].get(field["name"])
                    if result != None:
                        value = result["value"] * field["scaling"]
                        if "diff" in result:
                            diff = result["diff"]
                            arrow = "arrow-up" if diff >= 0 else "arrow-down"
                            if not (field["positive_diff_better"] ^ (diff >= 0)):
                                color = "green"
                            else:
                                color = "red"
                            sign = "{{icon {} color={}}}".format(arrow, color)
                            ret += '    <td bgcolor="{}">{:.3f}{} ({:+.2%})</td>\n'.format(
                                color, value, field["unit"], diff
                            )
                        else:
                            ret += '<td bgcolor="blue">{:.3f}{} //(new)// </td>\n'.format(value, field["unit"])
                ret += "  </tr>\n"
            ret += "</table>\n"
            ret += "</html>\n"
        else:
            ret += "No performance change detected.\n"
    return ret


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compare results of multiple benchmark runs.")
    parser.add_argument(
        "--compare",
        action="append",
        nargs="+",
        metavar=("from", "to"),
        help="compare results between `from` and `to` files",
    )
    parser.add_argument("--output", default="", help="output file name")
    # file is read line by line, each representing one test name
    parser.add_argument("--exclude_tests_file", help="file listing test names to be excluded")

    parser.add_argument(
        "--different-vendors",
        action="store_true",
        default=False,
        help="Comparing different vendors, there is no need for metadata, duration, count check.",
    )
    parser.add_argument(
        "--difference-threshold",
        type=float,
        default=0.02,
        help="Difference threshold for memory and throughput, 0.02 = 2% ",
    )

    args = parser.parse_args()

    fields = [
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
        {
            "name": "max",
            "positive_diff_better": False,
            "scaling": 1000,
            "unit": "ms",
        },
        {
            "name": "p99",
            "positive_diff_better": False,
            "scaling": 1000,
            "unit": "ms",
        },
        {
            "name": "p90",
            "positive_diff_better": False,
            "scaling": 1000,
            "unit": "ms",
        },
        {
            "name": "p75",
            "positive_diff_better": False,
            "scaling": 1000,
            "unit": "ms",
        },
        {
            "name": "p50",
            "positive_diff_better": False,
            "scaling": 1000,
            "unit": "ms",
        },
        {
            "name": "mean",
            "positive_diff_better": False,
            "scaling": 1000,
            "unit": "ms",
        },
    ]

    if args.compare is None or len(args.compare) == 0:
        raise Exception("You must specify at least one pair of files!")

    if args.exclude_tests_file:
        with open(args.exclude_tests_file, "r") as f:
            ignored = [line.rstrip("\n") for line in f]
    else:
        ignored = []

    cleaned = []
    if args.different_vendors:
        ignore_on_different_vendors = {"duration", "parsing_time", "planning_time", "plan_execution_time"}
        for field in fields:
            key = field["name"]
            if key in ignore_on_different_vendors:
                continue
            else:
                cleaned.append(field)
        fields = cleaned

    if args.difference_threshold > 0.01:
        for field in fields:
            if "diff_treshold" in field.keys():
                field["diff_treshold"] = args.difference_threshold

    data = []
    result_files = args.compare[0]
    result_reference = load_results(result_files[0])
    results_other = []
    for file in result_files[1:]:
        results_other.append(load_results(file))
        data.append(compare_results(result_reference, results_other[-1], fields, ignored, args.different_vendors))

    results_reference_config = (
        result_reference["__run_configuration__"] if "__run_configuration__" in result_reference.keys() else None
    )
    results_other_config = []
    for results in results_other:
        results_other_config.append(
            results["__run_configuration__"] if "__run_configuration__" in results.keys() else None
        )

    remarkup = generate_remarkup(
        fields, data, results_reference=results_reference_config, results_other=results_other_config
    )
    if args.output:
        with open(args.output, "w") as f:
            f.write(remarkup)
    else:
        print(remarkup, end="")
