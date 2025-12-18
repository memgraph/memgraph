import argparse
import json
import os
import re
import subprocess
import urllib.request


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


def get_memgraph_version(pr):
    p = subprocess.run(
        [
            "aws",
            "s3",
            "ls",
            f"s3://deps.memgraph.io/pr-build/memgraph/pr{pr}/ubuntu-24.04-relwithdebinfo/",
            "--recursive",
        ],
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


def pr_build_vars(payload):
    pr = payload["pr"]
    if pr.startswith("pr"):
        pr = pr[2:]

    memgraph_version, memgraph_commit = get_memgraph_version(pr)

    mage_version = get_mage_version()

    return mage_version, memgraph_version, memgraph_commit, pr


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
    parser = argparse.ArgumentParser(description="Read payload from Memgraph PR build workflow")

    parser.add_argument("payload", type=str, nargs="?", default="", help="JSON data from build workflow (optional)")

    args = parser.parse_args()
    if args.payload:
        try:
            payload = json.loads(args.payload)
        except json.JSONDecodeError:
            payload = {}
    else:
        payload = {}

    mage_version, memgraph_version, memgraph_commit, pr = pr_build_vars(payload)
    print(f"{mage_version} {memgraph_version} {memgraph_commit} {pr}")


if __name__ == "__main__":
    main()
