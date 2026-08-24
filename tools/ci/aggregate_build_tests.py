import argparse
import json
import os
import re
import subprocess
from typing import List
from urllib.parse import quote


def list_build_files(date: int, image_type: str = "mage", mock: bool = False) -> List[str]:
    """
    Lists the files in s3 for the current build date

    Input
    =====
    date: int
        Date in the format yyyymmdd
    image_type: str
        `memgraph` or `mage`
    mock: bool
        If True, the mock build is listed
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
            f"s3://deps.memgraph.io/daily-build/{image_type}{'_mock' if mock else ''}/{date}/",
            "--recursive",
        ],
        capture_output=True,
        text=True,
    )

    # extract the file keys found
    files = [line.split()[3] for line in p.stdout.splitlines()]

    return files


def parse_file_os_arch(file, image_type):
    """
    Extracts the OS and CPU architecture from a file name
    Inputs
    ======
    file: str
        s3 key of the package file name
    image_type: str
        `memgraph` or `mage`

    Returns
    =======
    os, arch: strings
        OS and CPU architecture, respectively, respectively
    """

    if image_type == "mage":
        # MAGE packages sit in a flat S3 prefix (no per-os subdir), so the os is
        # derived from the file type: Docker tarballs -> Docker (<arch>), debs ->
        # ubuntu-24.04, and rpms -> the distro encoded in the filename dist-tag
        # slot (memgraph-mage[-debuginfo]-<ver>-1.<os>.<arch>[<suffix>].rpm).
        is_deb = file.endswith(".deb")
        is_rpm = file.endswith(".rpm")
        is_run = file.endswith(".run")

        # rpms use `aarch64`, debs/docker tarballs use `arm64`
        if "aarch64" in file or "arm64" in file:
            base_arch = "arm64"
        else:
            base_arch = "x86_64"

        arch = base_arch

        if "relwithdebinfo" in file:
            arch = f"{arch}-relwithdebinfo"

        if "malloc" in file:
            arch = f"{arch}-malloc"

        if "cuda" in file:
            arch = f"{arch}-cuda"

        if "cugraph" in file:
            arch = f"{arch}-cugraph"

        if "-debuginfo" in file:
            arch = f"{arch}-debuginfo"

        if is_rpm:
            os = "RPM"
        elif is_deb:
            os = "DEB"
        elif is_run:
            os = "Offline Installer"
        else:
            os = f"Docker ({base_arch})"

    elif image_type == "memgraph":
        if "aarch64" in file:
            arch = "arm64"
        else:
            arch = "x86_64"

        if "relwithdebinfo" in file:
            arch = f"{arch}-relwithdebinfo"

        if "malloc" in file:
            arch = f"{arch}-malloc"

        # debuginfo rpms share the os dir + arch of the main package, so suffix
        # the arch to give them their own key (e.g. centos-10.x86_64-debuginfo)
        # instead of overwriting the main package. `-debuginfo` (with the hyphen)
        # never matches `-relwithdebinfo`.
        if "-debuginfo" in file:
            arch = f"{arch}-debuginfo"

        # `os` is not really an OS anymore - it's basically a packaging format
        os_path = file.split("/")[3].replace("-malloc", "").replace("-aarch64", "").replace("-relwithdebinfo", "")
        os = "unknown"
        for substr in ["docker", "rpm", "deb"]:
            if substr in os_path:
                os = substr
                break
    else:
        raise ValueError(f"Unsupported image_type: {image_type}")

    return os, arch


def build_package_json(files: List[str], return_url: bool = True, image_type: str = "mage") -> dict:
    """
    Extracts the OS and CPU architecture and builds the dict/json used by the
    daily-builds workflow

    Inputs
    ======
    files: List[str]
        list of s3 keys
    return_url: bool
        If True, the URL is returned, otherwise the s3 key
    image_type: str
        `memgraph` or `mage`

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

        # skip .tsv - this is used for tracking debug info for releases
        if url.endswith(".tsv"):
            continue

        os, arch = parse_file_os_arch(file, image_type)

        if os not in out:
            out[os] = {}

        out[os][arch] = url

    return out


def list_daily_release_packages(
    date: int, return_url: bool = True, image_type: str = "mage", mock: bool = False
) -> dict:
    """
    returns dict containing all packages for a specific date

    Inputs
    ======
    date: int
        Date in the format yyyymmdd
    return_url: bool
        If True, the URL is returned, otherwise the s3 key
    image_type: str
        `memgraph` or `mage`
    mock: bool
        If True, the mock build is listed
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

    files = list_build_files(date, image_type, mock)

    packages = build_package_json(files, return_url, image_type)

    return packages


def main(image_type: str, mock: bool = False) -> None:
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
    tests = os.getenv("TEST_RESULT")

    # collect packages part of the payload
    packages = list_daily_release_packages(date, image_type=image_type, mock=mock)

    # build the payload dict, print the JSON dump
    payload = {
        "event_type": "trigger_update_index",
        "client_payload": {
            "table": image_type,
            "limit": 42,
            "build_data": {"date": date, "tests": tests, "packages": packages},
        },
    }
    payload = json.dumps(payload)
    print(payload)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("image_type", type=str, choices=["memgraph", "mage"], default="mage")
    parser.add_argument("--mock", action="store_true", default=False)
    args = parser.parse_args()

    main(args.image_type, args.mock)
