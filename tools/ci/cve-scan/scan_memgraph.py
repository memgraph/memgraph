import argparse
import hashlib
import json
import os
import random
import stat
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List

from tqdm import tqdm

"""
This script helps to parallelize the binary scanning using cve-bin-tool.

1. Collect binaries to be scanned by `cve-bin-tool` - mostly within
`/usr/lib/memgraph` but also included a few specific files in `/usr/bin`.

2. Reorder the list of files such that a few binaries known to be slowest to
scan are started first (so the quicker ones are done in parallel, assuming we
assign enough threads). The `memgraph` and `mg_import_csv` binaries are by far
the slowest to scan (~20 minutes on Ryzen 9 3900, perhaps a little longer in CI)

3. Run a scan on each file in it's own thread using `ThreadPoolExecutor`, where
each scan will produce a temporary JSON file containing results.

4. Aggregate temporary files into a single JSON file.
"""

CVE_DIR = os.getenv("CVE_DIR", os.getcwd())


def file_hash(output_dir: str) -> str:
    """
    generate a random temporary filename

    Inputs
    ======
    output_dir: str
       The directory to save the hash file in.

    Returns
    =======
    hash: str
       The temporary file name.
    """

    hash = hashlib.sha256(str(random.random()).encode("utf-8")).hexdigest()

    return f"{output_dir}/{hash}.json"


def find_memgraph_files(start_dir: str) -> list:
    """
    Walk a directory and return a list of all the files in it.

    Inputs
    ======
    start_dir: str
        The directory to start the search from.

    Returns
    =======
    list: A list of all the files in the directory.
    """
    matches = []
    for dirpath, _, filenames in os.walk(start_dir):
        for filename in filenames:
            fullpath = f"{dirpath}/{filename}"
            try:
                st = os.lstat(fullpath)
            except OSError:
                # Skip files we can’t stat
                continue

            is_symlink = stat.S_ISLNK(st.st_mode)

            # Only consider regular files; skip symlinks
            if is_symlink:
                continue

            matches.append(fullpath)
    return matches


def run_cve_scan(target: str, output_dir: str) -> dict:
    """
    Run cve-bin-tool on a single target with JSON output to a file,
    using '-u never' and '-f json'. Returns the JSON string read from the file.
    Captures stdout/stderr so as not to interfere with the progress bar.

    Inputs
    ======
    target: str
        The target that is to be scanned.
    output_dir: str
        The directory where the JSON output will be written.

    Returns
    =======
    str:
        The JSON string read from the file.
    """
    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Construct a safe filename from the directory path
    # e.g., "/usr/local/bin" → "usr_local_bin.json"
    output_path = file_hash(output_dir)

    cmd = [
        "cve-bin-tool",
        "-u",
        "never",  # Never update the local CVE database
        "-f",
        "json",  # Output format: JSON
        "-o",
        output_path,  # Write JSON results to this file
        target,  # target to scan
    ]

    # Run the command, capturing stdout/stderr
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    # do not try to do anything clever with the proc.returncode here, because
    # it only ever returns 0 if there are 0 vulnerabilities!

    # Some targets produce no JSON at all; check before opening.
    if not os.path.isfile(output_path):
        raise RuntimeError(f"Expected JSON not found for {target!r}; no file at {output_path}")

    try:
        with open(output_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        raise RuntimeError(f"Could not read JSON for {target!r}: {e}")

    return data


def scan_directories_with_progress(
    dirs_to_scan: List[str], output_dir: str = f"{CVE_DIR}/tmp", max_workers: int = 20
) -> None:
    """
    Given a list of directories, scan each one in parallel using cve-bin-tool.
    - Uses '-u never' and '-f json'.
    - Writes each JSON result into 'output_dir'.
    - Shows a tqdm progress bar that advances as each scan completes.
    - Returns a dict mapping directory → (json_str or None, output_file_path).
      If a scan fails, json_str will be None, and the exception is printed.

    Inputs
    ======
    dirs_to_scan: List[str]
      A list of directories to scan.
    output_dir: str
      The directory to write the JSON results into.
    max_workers: int
      The maximum number of threads to use. Defaults to 20.
    """
    # Prepare the results dictionary
    results = []

    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)

    # Submit one task per directory
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_dir = {executor.submit(run_cve_scan, d, output_dir): d for d in dirs_to_scan}

        # Wrap as_completed in tqdm for a live progress bar
        for future in tqdm(
            as_completed(future_to_dir), total=len(future_to_dir), desc="Scanning directories", unit="dir"
        ):
            directory = future_to_dir[future]
            try:
                json_data = future.result()
                if isinstance(json_data, list):
                    results.extend(json_data)
                else:
                    results.append(json_data)
            except Exception as exc:
                print(f"Error scanning {directory!r}: {exc}")

    with open(f"{CVE_DIR}/cve-bin-tool-memgraph-summary.json", "w") as f:
        json.dump(results, f, indent=2)


def place_slowest_first(rootfs: str, directories: List[str]) -> List[str]:
    """
    Move the slowest binaries to be scanned to be first in the list, so that
    quicker ones can be scanned in parallel.

    Inputs
    ======
    rootfs: str
        The rootfs directory to scan.
    directories: List[str]
        The directories/files to scan.

    Returns
    =======
    List[str]
        The directories/files to scan, with the slowest ones first.
    """

    # these directories are the slowest to scan, so to speed things up a tiny bit,
    # let's scan them first so they are being done while other threads deal with
    # the quick ones (assumes that we have more threads than slow ones!)
    slow_dirs = [
        "usr/lib/memgraph/memgraph",
        "usr/bin/mg_import_csv",
        "usr/bin/mg_dump",
        "usr/bin/mgconsole",
    ]

    outdirs = []
    for sd in slow_dirs:
        slow_dir = f"{rootfs}/{sd}"
        if os.path.isdir(slow_dir) or os.path.isfile(slow_dir):
            outdirs.append(slow_dir)
        if slow_dir in directories:
            directories.remove(slow_dir)

    outdirs = outdirs + directories
    return outdirs


def main(rootfs: str, max_workers: int) -> None:
    """
    Scan for CVEs in memgraph-specific directories/files.

    Inputs
    ======
    rootfs: str
        The directory where the container root filesystem was extracted to
    max_workers: int
        The maximum number of workers to use
    """
    files = find_memgraph_files(f"{rootfs}/usr/lib/memgraph")
    files = place_slowest_first(rootfs, files)
    scan_directories_with_progress(files, max_workers=max_workers)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("rootfs", type=str, help="docker container root path to scan")
    parser.add_argument("max_workers", type=int, help="maximum number of workers to use")
    args = parser.parse_args()

    main(args.rootfs, args.max_workers)
