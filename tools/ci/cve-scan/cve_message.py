import argparse
import json
import os
from typing import List, Tuple

from format_cve_table import format_cyclonedx_data

import requests

CVE_DIR = os.getenv("CVE_DIR", os.getcwd())


def read_ignore_list() -> List[str]:
    """
    Reads the `cve-ignore-list` file from this directory and returns a list of CVEs to ignore.
    """
    fname = f"{os.path.dirname(__file__)}/cve-ignore-list"

    with open(fname, "r") as f:
        lines = f.readlines()

    out = []
    for line in lines:
        line = line.strip().upper()
        if "#" in line:
            line = line.split("#")[0].strip()
        if line.startswith("CVE-"):
            out.append(line)
    return out


def read_summary_file(filename: str) -> List[str] | dict:
    """
    Read the contents of a file and return it as a list or dict.
    """

    found = os.path.isfile(filename)
    if not found:
        print(f"{filename} summary not found")
        return None

    print(f"Found {filename}")
    with open(filename, "r") as f:
        if filename.endswith(".json"):
            data = json.load(f)
        else:
            data = f.readlines()
        return data


def parse_cve_report(filename: str) -> List[dict]:
    """
    Parse the CVE report and return a list of dictionaries.
    """
    data = read_summary_file(filename)
    cves = format_cyclonedx_data(data["vulnerabilities"], data["components"])
    ignore_list = read_ignore_list()
    out = []
    for cve in cves:
        if cve["vulnerabilityID"] not in ignore_list:
            out.append(cve)
    return out


def summarize_cves(cves: List[dict]) -> dict:
    """
    Summarize the CVEs by severity.

    Inputs
    ======
    cves: List[dict]
        A list of dictionaries, each containing a CVE.

    Returns
    =======
    Tuple[dict, str]
        A tuple containing the counts of each vulnerability severity.

    """

    summary = {}
    for cve in cves:
        severity = cve["severity"].upper()
        if severity not in summary:
            summary[severity] = 0
        summary[severity] += 1

    keys = ["UNKNOWN", "NEGLIGIBLE", "LOW", "MEDIUM", "HIGH", "CRITICAL"]
    emojis = [
        ":interrobang:",
        ":grinning_face_with_star_eyes:" * 2,
        ":grinning:" * 3,
        ":sweat_smile:" * 4,
        ":melting_face:" * 5,
        ":mushroom_cloud:" * 6,
    ]

    # build a little table
    msg = "```\n"
    msg += "| Severity   | Counts |\n"
    msg += "|------------|--------|\n"
    for key in keys:
        msg += f"| {key:10s} | {summary.get(key, 0):6} |\n"
    msg += "\n```\n"

    # find worst
    emoji = emojis[0]
    for i, key in enumerate(keys):
        if summary.get(key, 0) > 0:
            emoji = emojis[i]

    msg += f"Overal Status: {emoji}\n"

    return summary, msg


def severity_summary(data: List[dict]) -> dict:
    """
    count the total number of CVEs per severity level
    """
    summary = {}
    for cve in data:
        severity = cve["severity"]
        if severity not in summary:
            summary[severity] = 0
        summary[severity] += 1

    return summary


def create_slack_message(arch: str, image_type: str, cves: List[dict]) -> str:
    """
    Formats the Slack message to be sent.

    Inputs
    ======
    arch: str
        The architecture of the image to be scanned.
    image_type: str, choices=["memgraph", "mage"]
        The type of image to be scanned.
    cves: List[dict]
        A list of dictionaries, each containing a CVE.

    Returns
    =======
    str: The formatted Slack message.
    """

    arch_str = "x86_64" if arch == "amd" else "aarch64"
    name = "Memgraph" if image_type == "memgraph" else "MAGE"

    msg_start = f"Vulnerability Scan Results for *{name}* (Docker {arch_str})...\n\n"

    _, summary = summarize_cves(cves)

    msg = "\n".join([msg_start, summary])

    return msg


def post_message(msg: str) -> None:
    """
    Post message to Slack webhook.

    Inputs
    ======
    msg: str
        Message containing vulnerability summary.
    """

    url = os.getenv("INFRA_WEBHOOK_URL")
    try:
        response = requests.post(url, json={"text": msg})
    except Exception:
        print(f"Response: {response.status_code}")


def main(arch: str, image_type: str, send_slack_message: bool) -> None:
    """
    Collect vulnerability results and send a Slack message.

    Inputs
    ======
    arch: str
        The architecture of the image to be scanned.
    image_type: str
        The type of image to be scanned.

    """

    # collect results
    cves = parse_cve_report(f"{CVE_DIR}/combined_report.json")

    msg = create_slack_message(arch, image_type, cves)
    if send_slack_message:
        post_message(msg)
    else:
        print(msg)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("arch", type=str, default="amd")
    parser.add_argument("image_type", type=str, choices=["memgraph", "mage"], default="mage")
    parser.add_argument("send_message", type=str, default="true")
    args = parser.parse_args()

    main(args.arch, args.image_type, args.send_message == "true")
