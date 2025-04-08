import subprocess
import json
from typing import List
import os

def list_build_files(date: int) -> List[str]:
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
        ["aws","s3","ls",f"s3://deps.memgraph.io/daily-builds/memgraph/{date}/","--recursive"],
        capture_output=True,
        text=True
    )

    # extract the file keys found
    files = [line.split()[3] for line in p.stdout.splitlines()]

    return files

def build_package_json(files: List[str]) -> dict:
    """
    Extracts the OS and CPU architecture and builds the dict/json used by the
    daily-builds workflow

    Input
    =====
    files: List[str]
        list of s3 keys

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
        url = f"https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/{file}"

        if "aarch64" in file:
            arch = "arm64"
        else:
            arch = "x86_64"
        
        if "relwithdebinfo" in file:
            arch = f"{arch}-debug"
        
        if "malloc" in file:
            arch = f"{arch}-malloc"

        os = file.split("/")[2].replace(
            "-malloc",""
        ).replace(
            "aarch64",""
        ).replace(
            "relwithdebinfo",
            ""
        )

        if not os in out:
            out[os] = {}
        
        out[os][arch] = url

    return out


def main() -> None:
    """
    Collect BUILD_TEST_RESULTS, CURRENT_BUILD_DATE, s3 keys of packages and
    build a JSON payload to be sent to the daily build repo workflow

    The structure of the payload will be:
    {
        "date": 20250405,
        "tests": "pass",
        "packages": {
            "ubuntu-24.04": {
                "arm64": "https://s3.eu-west-1.....",
                "x86_64": "https://s3.eu-west-1....."
            }
        }
    }
    """
    date = int(os.getenv("CURRENT_BUILD_DATE"))

    # TODO: add individual test results and URL to each one
    tests = os.getenv("BUILD_TEST_RESULTS")

    # collect packages part of the payload
    files = list_build_files(date)
    packages = build_package_json(files)

    # build the payload dict, print the JSON dump
    payload = {
        "date": date,
        "tests": tests,
        "packages": packages
    }
    payload = json.dumps(payload)
    print(payload)

if __name__ == "__main__":
    main()