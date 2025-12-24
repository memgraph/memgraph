import argparse
import json
import os
from io import StringIO
from typing import Mapping, Optional, Sequence

from rich.console import Console
from rich.table import Table


def read_json_file(filename):
    if not os.path.exists(filename):
        return None

    with open(filename, "r") as f:
        data = json.load(f)
    return data


def format_table(data):
    table = Table(title="Vulnerabilities")
    table.add_column("Package", justify="left")
    table.add_column("Version", justify="left")
    table.add_column("VulnerabilityID", justify="left")
    table.add_column("Severity", justify="left")
    table.add_column("Type", justify="left")
    table.add_column("PURL", justify="left")
    for item in data:
        table.add_row(
            item["package"],
            item["version"],
            item["vulnerabilityID"],
            item["severity"],
            item["type"],
            item["purl"],
        )
    return table


def choose_severity(
    vendor_ratings: Sequence[Mapping],
    *,
    vulnerability_id: Optional[str] = None,
    data_source: Optional[str] = None,
    fallback_severity: Optional[str] = "UNKNOWN",
) -> str:
    """Choose a single severity using Trivy’s `auto` priority.

    Args:
        vendor_ratings: Each dict must have at least
            {"source": {"name": "<vendor>"},
             "severity": "<level>"}
        vulnerability_id: Used to detect GHSA IDs (GitHub gets higher priority).
        data_source: The primary data source ID (e.g., "redhat" or "ubuntu").
        fallback_severity: Optional general severity to return when nothing else matches.
    """

    def normalize(entry: Mapping) -> Optional[tuple[str, str]]:
        src = entry.get("source", {}).get("name", "").lower()
        severity = entry.get("severity", "").upper()
        return src, severity.upper()

    entries = [normalize(entry) for entry in vendor_ratings]
    entries = [entry for entry in entries if entry is not None]  # type: ignore[misc]
    entries_map = {}
    for name, severity in entries:
        entries_map.setdefault(name, []).append(severity)

    def first_for(name: str) -> Optional[str]:
        variants = entries_map.get(name.lower())
        if variants:
            return variants[0]
        return None

    precedence = []
    if data_source:
        precedence.append(data_source)
    if vulnerability_id and vulnerability_id.upper().startswith("GHSA-"):
        precedence.append("ghsa")
    precedence.append("nvd")

    for source_name in precedence:
        if not source_name:
            continue
        matched = first_for(source_name)
        if matched:
            return matched

    # Try other vendors in alphabetical order to mimic “other data sources”
    remaining = sorted(name for name in entries_map if name not in {s.lower() for s in precedence if s})
    for name in remaining:
        matched = first_for(name)
        if matched:
            return matched

    if fallback_severity:
        return fallback_severity.upper()

    return "UNKNOWN"


def format_cyclonedx_data(vulnerabilities, components):
    cves = []
    for item in vulnerabilities:
        cves.append(
            {
                "affects": [x["ref"] for x in item["affects"]],
                "cve": item["id"],
                "severity": choose_severity(item["ratings"], data_source=item.get("source", {}).get("name")),
            }
        )
    out = []
    for cve in cves:
        for affect in cve["affects"]:
            for component in components:
                if affect in [component.get("purl"), component.get("bom-ref")]:
                    out.append(
                        {
                            "type": component["type"],
                            "vulnerabilityID": cve["cve"],
                            "severity": cve["severity"],
                            "package": component["name"],
                            "version": component["version"],
                            "purl": component["purl"],
                        }
                    )

    # deduplicate by package, version and vulnerabilityID
    keep_inds = []
    keys = []
    for i, item in enumerate(out):
        key = (item["package"], item["version"], item["vulnerabilityID"])
        if key not in keys:
            keys.append(key)
            keep_inds.append(i)
    out = [out[i] for i in keep_inds]

    # sort items by type, then package name
    out.sort(key=lambda x: (x["type"], x["package"]))
    return out


def save_table_to_file(table, filename):
    console = Console(file=StringIO(), width=None, force_terminal=False)
    console.print(table)
    output = console.file.getvalue()
    with open(filename, "w", encoding="utf-8") as f:
        f.write(output)


def main():
    # take in a single positional argument, the combined report file
    parser = argparse.ArgumentParser()
    parser.add_argument("combined_file", type=str)
    args = parser.parse_args()

    combined_data = read_json_file(args.combined_file)
    formatted_data = format_cyclonedx_data(combined_data["vulnerabilities"], combined_data["components"])
    table = format_table(formatted_data)
    outdir = os.getenv("CVE_DIR", os.getcwd())
    save_table_to_file(table, f"{outdir}/combined_report.txt")


if __name__ == "__main__":
    main()
