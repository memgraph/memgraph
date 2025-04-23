from aggregate_build_tests import list_daily_release_packages
from typing import List
import re
import os
import argparse


def extract_commit_hash(filename):
    """
    Attempts to extract a commit hash from the given filename.
    The regex looks for a delimiter, then 8 to 12 hexadecimal characters,
    followed by another delimiter.
    """
    # This regex looks for one of the delimiters (. _ + ~ -)
    # then captures a group of 8-12 hex digits,
    # and ensures it is followed by a delimiter like - or _ or .
    pattern = re.compile(r"[._+~-](?P<hash>[0-9a-f]{8,12})(?=[-_\.])")
    match = pattern.search(filename)
    if match:
        return match.group("hash")
    return None


def get_daily_commit_hash(date: int) -> List[str]:
    """
    parse the daily build file names for their commit hashes and return as a list

    """

    packages = list_daily_release_packages(date, return_url=False)

    hashes = []
    for _, variants in packages.items():
        for _, key in variants.items():
            file = os.path.basename(key)
            hash = extract_commit_hash(file)
            hashes.append(hash)

    return hashes


def main():
    parser = argparse.ArgumentParser(
        description="Extract list of commit hashes form daily build"
    )
    parser.add_argument("date", type=str, help="Date as a string (e.g., '20250409').")

    args = parser.parse_args()

    hashes = get_daily_commit_hash(int(args.date))

    print(" ".join(hashes))
