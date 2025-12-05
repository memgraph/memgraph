#!/usr/bin/env python3
import argparse
import json
import os
import re
import socket
import subprocess
import sys
import threading
import time
import urllib.request
from functools import wraps

# This script is used to determine the current version of Memgraph. The script
# determines the current version using `git` automatically. The user can also
# manually specify a version override and then the supplied version will be
# used instead of the version determined by `git`. All versions (automatically
# detected and manually specified) can have a custom version suffix added to
# them.
#
# NOTE: If the script is unable to connect to GitHub, it will attempt to use the
# latest version found locally in tags. If there are no tags, the script will
# use the fallback version 0.0.0.
#
# The current version can be one of either:
#   - release version
#   - development version
#
# The release version is either associated with a `release/X.Y` branch
# (automatic detection) or is manually specified. When the version is
# automatically detected from the branch a `.0` is appended to the version.
# Example 1:
#   - branch: release/0.50
#   - version: 0.50.0
# Example 2:
#   - manually supplied version: 0.50.1
#   - version: 0.50.1
#
# The development version is always determined using `git` in the following
# way:
#   - release version - nearest (older) `release/X.Y` branch version
#   - distance from the release branch - Z commits
#   - the current commit short hash
# Example:
#   - release version: 0.50.0 (nearest older branch `release/0.50`)
#   - distance from the release branch: 42 (commits)
#   - current commit short hash: 7e1eef94
#
# The script then uses the collected information to generate the versions that
# will be used in the binary, DEB package and RPM package.  All of the versions
# have different naming conventions that have to be met and they differ among
# each other.
#
# The binary version is determined using the following two templates:
#   Release version:
#     <VERSION>-<OFFERING>[-<SUFFIX>]
#   Development version:
#     <VERSION>+<DISTANCE>~<SHORTHASH>-<OFFERING>[-<SUFFIX>]
# Examples:
#   Release version:
#     0.50.1-open-source
#     0.50.1
#     0.50.1-veryimportantcustomer
#   Development version (master, 12 commits after release/0.50):
#     0.50.0+12~7e1eef94-open-source
#     0.50.0+12~7e1eef94
#     0.50.0+12~7e1eef94-veryimportantcustomer
#
# The DEB package version is determined using the following two templates:
#   Release version:
#     <VERSION>-<OFFERING>[-<SUFFIX>]-1
#   Development version (master, 12 commits after release/0.50):
#     <VERSION>+<DISTANCE>~<SHORTHASH>-<OFFERING>[-<SUFFIX>]-1
# Examples:
#   Release version:
#     0.50.1-open-source-1
#     0.50.1-1
#     0.50.1-veryimportantcustomer-1
#   Development version (master, 12 commits after release/0.50):
#     0.50.0+12~7e1eef94-open-source-1
#     0.50.0+12~7e1eef94-1
#     0.50.0+12~7e1eef94-veryimportantcustomer-1
# For more documentation about the DEB package naming conventions see:
#   https://www.debian.org/doc/debian-policy/ch-controlfields.html#version
#
# The RPM package version is determined using the following two templates:
#   Release version:
#     <VERSION>_1.<OFFERING>[.<SUFFIX>]
#   Development version:
#     <VERSION>_0.<DISTANCE>.<SHORTHASH>.<OFFERING>[.<SUFFIX>]
# Examples:
#   Release version:
#     0.50.1_1.open-source
#     0.50.1_1
#     0.50.1_1.veryimportantcustomer
#   Development version:
#     0.50.0_0.12.7e1eef94.open-source
#     0.50.0_0.12.7e1eef94
#     0.50.0_0.12.7e1eef94.veryimportantcustomer
# For more documentation about the RPM package naming conventions see:
#   https://docs.fedoraproject.org/en-US/packaging-guidelines/Versioning/
#   https://fedoraproject.org/wiki/Package_Versioning_Examples


def can_connect_to_github(url="https://www.github.com", timeout=3):
    """
    Check if we can connect to GitHub with a proper timeout that includes DNS resolution.

    The issue with urllib.request.urlopen() is that its timeout only applies to the
    socket connection and reading, not DNS resolution. DNS can take 30+ seconds if
    the DNS server is unreachable. This function uses socket.create_connection() with a timeout
    to ensure the entire operation (including DNS) is bounded.
    """
    t0 = time.time()
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        hostname = parsed.hostname
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        sock = socket.create_connection((hostname, port), timeout=timeout)
        sock.close()
        t1 = time.time()
        elapsed = t1 - t0
        return True
    except Exception as e:
        t1 = time.time()
        elapsed = t1 - t0
        exc_type = type(e).__name__
        print(f"Could not connect to GitHub in {elapsed:.2f} seconds: {exc_type}", flush=True, file=sys.stderr)
        return False
def retry(retry_limit, timeout=100):
    def inner_func(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for _ in range(retry_limit):
                try:
                    return func(*args, **kwargs)
                except Exception:
                    time.sleep(timeout)
            return func(*args, **kwargs)

        return wrapper

    return inner_func


@retry(3)
def get_output(*cmd, multiple=False):
    shell = any(["|" in x for x in cmd])
    cmd = " ".join(cmd) if shell else cmd
    ret = subprocess.run(cmd, stdout=subprocess.PIPE, check=True, shell=shell)
    if multiple:
        return list(map(lambda x: x.strip(), ret.stdout.decode("utf-8").strip().split("\n")))
    return ret.stdout.decode("utf-8").strip()


def request_repo_tags(token=None):
    api_url = "https://api.github.com/repos/memgraph/memgraph/tags"
    req = urllib.request.Request(api_url)
    req.add_header("User-Agent", "Memgraph-Version-Script/1.0")
    if token:
        req.add_header("Authorization", f"token {token}")
    try:
        with urllib.request.urlopen(req) as response:
            return json.loads(response.read().decode())
    except Exception as e:
        print(f"Warning: Could not fetch tags from original repository: {e}", file=sys.stderr)
        return None


def fetch_memgraph_repo_version():
    """Fetch version information from the original Memgraph repository via GitHub API."""
    try:
        # Check for GitHub token in environment variables
        github_token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GITHUB_API_TOKEN")

        # try authenticated request first, to avoid rate limiting
        tags_data = None
        if github_token:
            tags_data = request_repo_tags(github_token)

        # if authenticated request fails, try unauthenticated request
        if tags_data is None:
            tags_data = request_repo_tags()

        if tags_data is None:
            return None, None

        # Look for version tags in format vx.y.z, excluding rc tags
        version_tags = []
        for tag in tags_data:
            tag_name = tag["name"]
            # Match vx.y.z format but exclude rc tags
            if re.match(r"^v[0-9]+\.[0-9]+\.[0-9]+$", tag_name) and "rc" not in tag_name:
                version = tuple(map(int, tag_name[1:].split(".")))  # Remove 'v' prefix
                version_tags.append((version, tag_name))

        if version_tags:
            # Sort by version and get the latest
            version_tags.sort(reverse=True)
            latest_version, latest_tag = version_tags[0]
            return latest_version, latest_tag

        return None, None

    except Exception as e:
        print(f"Warning: Could not fetch version from original repository: {e}", file=sys.stderr)
        return None, None


def format_version(variant, version, offering, distance=None, shorthash=None, suffix=None):
    if not distance:
        # This is a release version.
        if variant == "deb":
            # <VERSION>-<OFFERING>[-<SUFFIX>]-1
            ret = "{}{}".format(version, "-" + offering if offering else "")
            if suffix:
                ret += "-" + suffix
            ret += "-1"
            return ret
        elif variant == "rpm":
            # <VERSION>_1.<OFFERING>[.<SUFFIX>]
            ret = "{}_1{}".format(version, "." + offering if offering else "")
            if suffix:
                ret += "." + suffix
            return ret
        else:
            # <VERSION>-<OFFERING>[-<SUFFIX>]
            ret = "{}{}".format(version, "-" + offering if offering else "")
            if suffix:
                ret += "-" + suffix
            return ret
    else:
        # This is a development version.
        if variant == "deb":
            # <VERSION>+<DISTANCE>~<SHORTHASH>-<OFFERING>[-<SUFFIX>]-1
            ret = "{}+{}~{}{}".format(version, distance, shorthash, "-" + offering if offering else "")
            if suffix:
                ret += "-" + suffix
            ret += "-1"
            return ret
        elif variant == "rpm":
            # <VERSION>_0.<DISTANCE>.<SHORTHASH>.<OFFERING>[.<SUFFIX>]
            ret = "{}_0.{}.{}{}".format(version, distance, shorthash, "." + offering if offering else "")
            if suffix:
                ret += "." + suffix
            return ret
        else:
            # <VERSION>+<DISTANCE>~<SHORTHASH>-<OFFERING>[-<SUFFIX>]
            ret = "{}+{}~{}{}".format(version, distance, shorthash, "-" + offering if offering else "")
            if suffix:
                ret += "-" + suffix
            return ret


# Parse arguments.
parser = argparse.ArgumentParser(description="Get the current version of Memgraph.")
parser.add_argument("--open-source", action="store_true", help="set the current offering to 'open-source'")
parser.add_argument("version", help="manual version override, if supplied the version isn't " "determined using git")
parser.add_argument("suffix", help="custom suffix for the current version being built")
parser.add_argument(
    "--variant",
    choices=("binary", "deb", "rpm"),
    default="binary",
    help="which variant of the version string should be generated",
)
parser.add_argument(
    "--memgraph-root-dir", help="The root directory of the checked out " "Memgraph repository.", default="."
)
args = parser.parse_args()

if not os.path.isdir(args.memgraph_root_dir):
    raise Exception("The root directory ({}) is not a valid directory".format(args.memgraph_root_dir))

os.chdir(args.memgraph_root_dir)

offering = "open-source" if args.open_source else None

# Check whether the version was manually supplied.
if args.version:
    if not re.match(r"^[0-9]+\.[0-9]+\.[0-9]+$", args.version):
        raise Exception("Invalid version supplied '{}'!".format(args.version))
    print(format_version(args.variant, args.version, offering, suffix=args.suffix), end="")
    sys.exit(0)

# check if we can connect to GitHub
can_connect = can_connect_to_github()

# Within CI, after the regular checkout, master is sometimes (e.g. in the case
# of an epic or task branch) NOT created as a local branch. cpack depends on
# variables generated by calling this script during the cmake phase. This
# script needs master to be the local branch. `git fetch origin master:master`
# is creating the local master branch without checking it out. Does nothing if
# master is already there.
try:
    current_branch = get_output("git", "rev-parse", "--abbrev-ref", "HEAD")
    if current_branch != "master":
        branches = get_output("git", "branch", "-r", "--list", "origin/master")
        if "origin/master" in branches:
            # If master is present locally, the fetch is allowed to fail
            # because this script will still be able to compare against the
            # master branch.
            try:
                if can_connect:
                    get_output("git", "fetch", "origin", "master")
                else:
                    print("WARNING: Could not connect to GitHub. Unable to fetch master branch.", file=sys.stderr)
            except Exception:
                pass
        else:
            # If master is not present locally, the fetch command has to
            # succeed because something else will fail otherwise.
            if can_connect:
                get_output("git", "fetch", "origin", "master")
            else:
                print("WARNING: Could not connect to GitHub. Unable to fetch master branch.", file=sys.stderr)
                print("Fatal error while ensuring local master branch.", file=sys.stderr)
                sys.exit(1)
except Exception:
    print("Fatal error while ensuring local master branch.", file=sys.stderr)
    sys.exit(1)

# Get current commit hashes.
current_hash = get_output("git", "rev-parse", "HEAD")

# --short option can be of variable length, this will fix the length to 12
current_hash_short = get_output("git", "rev-parse", "HEAD", "|", "cut", "-c1-12")

# We want to find branches that exist on some remote and that are named
# `release/[0-9]+\.[0-9]+`.
branch_regex = re.compile(r"^remotes/[a-zA-Z0-9]+/release/([0-9]+\.[0-9]+)$")

# Find all existing versions.
versions = []
branches = get_output("git", "branch", "--all", multiple=True)
for branch in branches:
    match = branch_regex.match(branch)
    if match is not None:
        version = tuple(map(int, match.group(1).split(".")))
        master_branch_merge = get_output("git", "merge-base", "origin/master", branch)
        versions.append((version, branch, master_branch_merge))
versions.sort(reverse=True)

# Check which existing version branch is closest to the current commit. We are
# only interested in branches that were branched out before this commit was
# created.
current_version = None
for version in versions:
    version_tuple, branch, master_branch_merge = version
    current_branch_merge = get_output("git", "merge-base", current_hash, branch)
    master_current_merge = get_output("git", "merge-base", current_hash, "origin/master")
    # The first check checks whether this commit is a child of `master` and
    # the version branch was created before us.
    # The second check checks whether this commit is a child of the version
    # branch.
    if master_branch_merge == current_branch_merge or master_branch_merge == master_current_merge:
        current_version = version
        break

# Determine current version.
if current_version is None:
    # Fallback: try to get version information from the original Memgraph repository
    print("No local release branches found. Fetching version from original repository...", file=sys.stderr)

    if can_connect:
        original_version, original_branch = fetch_memgraph_repo_version()
    else:
        print(
            "WARNING: Could not connect to GitHub. Unable to fetch version from original repository.", file=sys.stderr
        )
        original_version = None
        original_branch = None

    if original_version is not None:
        # Use the version from the original repository
        version_str = ".".join(map(str, original_version)) + ".0"

        # Calculate distance from master branch as a development version
        try:
            distance = int(get_output("git", "rev-list", "--count", "--first-parent", "origin/master.." + current_hash))
        except Exception:
            distance = 0

        if distance == 0:
            print(format_version(args.variant, version_str, offering, suffix=args.suffix), end="")
        else:
            print(
                format_version(
                    args.variant,
                    version_str,
                    offering,
                    distance=distance,
                    shorthash=current_hash_short,
                    suffix=args.suffix,
                ),
                end="",
            )
        sys.exit(0)
    else:
        print(
            "Unable to determine version. No local release branches found and could not fetch from "
            "original repository. This repository may need to be updated or may not be a standard "
            "Memgraph repository."
        )

        # just use latest version found locally in tags
        tag = get_output("git", "tag", "|", "grep", "-v", "rc", "|", "tail", "-n", "1")
        if tag:
            # regex match for x.y.z format
            match = re.match(r"^v[0-9]+\.[0-9]+\.[0-9]+$", tag)
            if match:
                version_str = match.group(0)[1:]
            else:
                # fallback
                print("WARNING: Unable to determine version from tag. Using fallback version 0.0.0.", file=sys.stderr)
                version_str = "0.0.0"
        else:
            print("WARNING: Unable to determine version from tag. Using fallback version 0.0.0.", file=sys.stderr)
            version_str = "0.0.0"
        print(format_version(args.variant, version_str, offering, suffix=args.suffix), end="")
        sys.exit(0)

version, branch, master_branch_merge = current_version
distance = int(get_output("git", "rev-list", "--count", "--first-parent", master_branch_merge + ".." + current_hash))
version_str = ".".join(map(str, version)) + ".0"
if distance == 0:
    print(format_version(args.variant, version_str, offering, suffix=args.suffix), end="")
else:
    print(
        format_version(
            args.variant, version_str, offering, distance=distance, shorthash=current_hash_short, suffix=args.suffix
        ),
        end="",
    )
