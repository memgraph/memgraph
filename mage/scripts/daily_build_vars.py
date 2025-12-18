import argparse
import json
import os
import re
import subprocess
import urllib.request


def get_latest_build() -> int:
    p = subprocess.run(
        ["aws", "s3", "ls", "s3://deps.memgraph.io/daily-build/memgraph/", "--recursive"],
        capture_output=True,
        text=True,
    )

    # extract the file keys found
    files = [line.split()[3] for line in p.stdout.splitlines()]

    # get the dates
    keydates = list(set([file.split("/")[2] for file in files]))
    dates = []
    for keydate in keydates:
        try:
            dates.append(int(keydate))
        except ValueError:
            pass

    return max(dates)


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


def get_memgraph_version(date):
    p = subprocess.run(
        ["aws", "s3", "ls", f"s3://deps.memgraph.io/daily-build/memgraph/{date:08d}/ubuntu-24.04/", "--recursive"],
        capture_output=True,
        text=True,
    )

    # extract the file key found - there should only be one!
    file = [line.split()[3] for line in p.stdout.splitlines()][0]

    # remove the path, the first and last parts of the filename which should
    # always be the same to reveal the daily build version
    basename = os.path.basename(file)
    version = basename[9:-12]
    hash = extract_commit_hash(file)

    return version, hash


def daily_build_vars(payload):
    if "date" in payload:
        date = payload["date"]
    else:
        date = get_latest_build()

    memgraph_version, memgraph_commit = get_memgraph_version(date)

    mage_version = get_mage_version()

    return mage_version, memgraph_version, memgraph_commit, date


def get_commit():
    p = subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True)

    return p.stdout.strip()


def get_pr():
    p = subprocess.run(
        r"git log --pretty=%B | grep -oP '(?<=\(#)\d+(?=\))' | head -n 1", shell=True, capture_output=True, text=True
    )

    return p.stdout.strip()


def get_tag():
    with urllib.request.urlopen("https://api.github.com/repos/memgraph/mage/tags") as response:
        # Read the JSON data from GitHub
        tags = json.load(response)

    # Find the first tag whose name does not contain 'rc'
    try:
        latest = next(tag["name"][1:] for tag in tags if "rc" not in tag["name"])
    except StopIteration:
        latest = "?.?.?"
    return latest


def get_mage_version():
    commit = get_commit()
    pr = get_pr()
    tag = get_tag()

    mage_version = f"{tag}_pr{pr}_{commit}"
    return mage_version


def main() -> None:
    parser = argparse.ArgumentParser(description="Read payload from Memgraph daily build workflow")

    parser.add_argument("payload", type=str, nargs="?", default="", help="JSON data from build workflow (optional)")

    args = parser.parse_args()
    if args.payload:
        try:
            payload = json.loads(args.payload)
        except json.JSONDecodeError:
            payload = {}
    else:
        payload = {}

    mage_version, memgraph_version, memgraph_commit, date = daily_build_vars(payload)
    print(f"{mage_version} {memgraph_version} {memgraph_commit} {date}")


if __name__ == "__main__":
    main()
