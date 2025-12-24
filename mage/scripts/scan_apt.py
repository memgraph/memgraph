import argparse
import json
import os
import subprocess
from typing import List, Tuple

from cve_bin_tool.cvedb import CVEDB

"""
This script is used for scanning a Docker container's installed APT packages, as
part of an effort to speed up the usage of `cve-bin-tool`.

This script does the following:
1. Executes a command within the container to list all installed packages in a
JSON format. This format only includes the `product` and `version`. We also need
`vendor`, which is less easy to fetch.

2. It then imports the CVE database object directly from cve_bin_tool to map
each of the installed packages to a `vendor` (or multiple in some cases).

3. Saves a CSV with columns: vendor, product, version.

4. Calls cve-bin-tool with the `--input-file` (`-i`) argument pointing to the
CSV file. This will do a direct database lookup for each product, vendor and
version, rather than scanning the iamge itself. The output is a JSON file
containing all CVEs for those installed packages.
"""

CVE_DIR = os.getenv("CVE_DIR", os.getcwd())


def get_apt_packages(container: str = "memgraph") -> List[dict]:
    """
    Collect the list of installed apt packages within the container.

    Inputs
    ======
    container: str
       Name of the container to scan

    Returns
    =======
    packages: list of installed packages
    """

    cmd = [
        "docker",
        "exec",
        container,
        "dpkg-query",
        "--show",
        '--showformat={"name": "${binary:Package}", "version": "${Version}"}, ',
    ]

    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,  # so that stdout/stderr come back as Python strings
    )
    # do not try to do anything clever with the result.returncode here, because
    # it only ever returns 0 if there are 0 vulnerabilities!

    output = result.stdout.strip()
    if not output:
        return []

    # Remove trailing comma and wrap in array brackets
    output = output.rstrip(", ")
    packages = json.loads(f"[{output}]")

    print(f"Found {len(packages)} installed DEB packages")
    return packages


def get_package_vendor_pairs(cve_db: CVEDB, packages: List[dict]) -> List[dict]:
    """
    return the list of vendors for a package

    Inputs
    ======
    cve_db: CVEDB
      CVEDB object to use
    packages: List[str]
      list of installed packages

    Returns
    =======
    pairs: list of vendor/product/version dicts
    """

    return cve_db.get_vendor_product_pairs(packages)


def combine_vendor_product_version(packages: List[dict], pairs: List[dict]) -> List[Tuple[str]]:
    """
    create the full list of vendor, product and version for each package

    Inputs
    ======
    packages: List[str]
      list of installed packages
    pairs: List[str]
      list of vendor/product dicts

    Returns
    =======
    out: List[Tuple[str]]
        list of tuples (vendor, product, version)
    """
    prod_vends = {}
    for pair in pairs:
        prod = pair["product"]
        vend = pair["vendor"]
        if prod not in prod_vends:
            prod_vends[prod] = []
        prod_vends[prod].append(vend)

    out = []
    for package in packages:
        prod = package["name"]
        ver = package["version"]

        if prod in prod_vends:
            vends = prod_vends[prod]
            for vend in vends:
                out.append((vend, prod, ver))

    return out


def save_apt_package_csv(packages):
    """
    Save CSV of package vendors, products and versions

    Inputs:
      packages: List[str]
        list of installed packages
    """

    with open(f"{CVE_DIR}/apt-packages.csv", "w") as f:
        f.write("vendor,product,version\n")
        for package in packages:
            f.write(f"{','.join(package)}\n")


def run_scan() -> None:
    """
    Run scan of apt packages and save results to JSON file.
    """

    cmd = [
        "cve-bin-tool",
        "-u",
        "never",  # Never update the local CVE database
        "-f",
        "json",  # Output format: JSON
        "-o",
        f"{CVE_DIR}/cve-bin-tool-apt-summary.json",  # Write JSON results to this file
        "-i",
        f"{CVE_DIR}/apt-packages.csv",
    ]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"cve-bin-tool failed with exit code {result.returncode}: {result.stderr}")


def main(container):
    """
    Scan container packages for CVEs

    Inputs
    ======
    container: str
        container name or ID
    """
    cve_db = CVEDB()

    packages = get_apt_packages(container)
    pairs = get_package_vendor_pairs(cve_db, packages)
    package_info = combine_vendor_product_version(packages, pairs)
    save_apt_package_csv(package_info)
    print(f"Checking {len(package_info)} packages with cve-bin-tool...")
    run_scan()

    # cve_db does unusual things when it exists, so let's catch it
    try:
        del cve_db
    except Exception:
        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("container", type=str)
    args = parser.parse_args()

    main(args.container)
