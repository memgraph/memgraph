import json
import os
import subprocess
from typing import List
from urllib.parse import quote


def list_build_files(date: int, mock: bool = False) -> List[str]:
    """
    Lists the files in s3 for the current build date

    Input
    =====
    date: int
        Date in the format yyyymmdd

    Returns
    =======
    files: list[str]
        list of package s3 keys for this date
    """
    p = subprocess.run(
        [
            "aws",
            "s3",
            "ls",
            f"s3://deps.memgraph.io/daily-build/memgraph{'_mock' if mock else ''}/{date}/",
            "--recursive",
        ],
        capture_output=True,
        text=True,
    )

    # extract the file keys found
    files = [line.split()[3] for line in p.stdout.splitlines()]

    return files


def build_package_json(files: List[str], return_url: bool = True) -> dict:
    """
    Extracts the OS and CPU architecture and builds the dict/json used by the
    daily-builds workflow

    Inputs
    ======
    files: List[str]
        list of s3 keys
    return_url: bool
        If True, the URL is returned, otherwise the s3 key

    Returns
    =======
    out: dict
        dictionary of the format:
        {
            "ubuntu-24.04: {
                "x86_64": "https://.....",
                "arm64": "https://....."
            }
        }
    """
    out = {}
    for file in files:
        if return_url:
            url = quote(f"https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/{file}", safe=":/")
        else:
            url = file

        if "aarch64" in file:
            arch = "arm64"
        else:
            arch = "x86_64"

        if "relwithdebinfo" in file:
            arch = f"{arch}-debug"

        if "malloc" in file:
            arch = f"{arch}-malloc"

        os = file.split("/")[3].replace("-malloc", "").replace("-aarch64", "").replace("-relwithdebinfo", "")

        if os not in out:
            out[os] = {}

        out[os][arch] = url

    return out


def list_daily_release_packages(date: int, return_url: bool = True, mock: bool = False) -> dict:
    """
    returns dict containing all packages for a specific date

    Inputs
    ======
    date: int
        Date in the format yyyymmdd
    return_url: bool
        If True, the URL is returned, otherwise the s3 key

    Returns
    =======
    out: dict
        dictionary of the format:
        {
            "ubuntu-24.04: {
                "x86_64": "https://.....",
                "arm64": "https://....."
            }
        }
    """

    files = list_build_files(date, mock)
    packages = build_package_json(files, return_url)

    return packages


def main() -> None:
    """
    Collect BUILD_TEST_RESULTS, CURRENT_BUILD_DATE, s3 keys of packages and
    build a JSON payload to be sent to the daily build repo workflow

    The structure of the payload will be:
    {
        "event_type": "daily-build-update",
        "client_payload": {
            "date": 20250405,
            "tests": "pass",
            "packages": {
                "ubuntu-24.04": {
                    "arm64": "https://s3.eu-west-1.....",
                    "x86_64": "https://s3.eu-west-1....."
                }
            }
        }
    }
    """
    date = int(os.getenv("CURRENT_BUILD_DATE"))

    # TODO: add individual test results and URL to each one
    tests = os.getenv("TEST_INDIVIDUAL_RESULT")

    # collect packages part of the payload
    packages = list_daily_release_packages(date)

    # build the payload dict, print the JSON dump
    payload = {
        "event_type": "trigger_update_index",
        "client_payload": {
            "table": "memgraph",
            "limit": 42,
            "build_data": {"date": date, "tests": tests, "packages": packages},
        },
    }
    payload = json.dumps(payload)
    print(payload)


if __name__ == "__main__":
    main()
