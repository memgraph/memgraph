import argparse
import os
import subprocess

from cve_bin_tool.parsers.parse import valid_files as cbt_valid_files

"""
This script should speed up the scanning for specific programming-language
package vulnerabilities with cve-bin-tool.

This script doe the following:
1. Walks the extracted root filesystem of the container searching for package
metadata files which cve-bin-tool would normally look for (`valid_files`).

2. Copies these files to a separate directory structure so that cve-bin-tool
   does not have to scan the entire root filesystem, which can be very slow.

3. Runs cve-bin-tool on the copied files, using a triage file to filter out
   false positives, and outputs the results to a JSON file.
"""

CVE_DIR = os.getenv("CVE_DIR", os.getcwd())


def find_files(rootfs: str) -> list[str]:
    """
    Find all language files that CVE-bin-tool scans

    Inputs
    ======
    rootfs: str
        The root directory to search for language files

    Returns
    =======
    matches: list[str]
        A list of paths to metadata files for language packages
    """

    file_checkers = cbt_valid_files.copy()
    file_checkers["METADATA"] = file_checkers["METADATA: "]
    file_checkers["PKG-INFO"] = file_checkers["PKG-INFO: "]
    language_files = list(file_checkers.keys())

    matches = []
    for dirpath, _, filenames in os.walk(rootfs):
        for filename in filenames:
            if filename in language_files and filename != "requirements.txt":
                matches.append(f"{dirpath}/{filename}")
    return matches


def copy_language_files(rootfs: str, langfs: str):
    """
    Copy language files from the root filesystem to a language-specific
    directory structure to avoid scanning everything in the rootfs.
    """

    language_files = find_files(rootfs)

    for file in language_files:
        dir = os.path.dirname(file).replace(rootfs, langfs)
        if not os.path.exists(dir):
            os.makedirs(dir)
        os.system(f"cp {file} -v {dir}")


def run_language_scan(langfs: str) -> str:
    """
    Scan the CVE database using the list of language packages found and save the
    results to a JSON file.

    Inputs
    =======
    langfs: str
        The directory containing the language package metadata files.

    Returns
    =======
    str
        The path to the JSON file containing the CVE scan results for the language packages.
        If the file does not exist, an empty string is returned.
    """

    print("Scanning Language Packages...")
    outfile = f"{CVE_DIR}/cve-bin-tool-lang-summary.json"

    cmd = [
        "cve-bin-tool",
        "-u",
        "never",  # Never update the local CVE database
        "-f",
        "json",  # Output format: JSON
        "-o",
        outfile,  # Write JSON results to this file
        f"{langfs}/",
    ]
    _ = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    # do not try to do anything clever with the result.returncode here, because
    # it only ever returns 0 if there are 0 vulnerabilities!
    return outfile


def main(rootfs: str) -> None:
    """
    Scan the root filesystem for CVEs in the language packages.
    """
    copy_language_files(rootfs, f"{CVE_DIR}/langfs")
    run_language_scan(f"{CVE_DIR}/langfs")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("rootfs", type=str)
    args = parser.parse_args()

    main(args.rootfs)
