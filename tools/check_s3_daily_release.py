from aggregate_build_tests import list_daily_release_packages
from typing import Tuple
import argparse
import os

def check_package_exists(date: int, OS: str, arch: str, malloc: bool, relwithdebinfo: bool) -> Tuple[bool,str]:
    """
    Check for the existence of a specific package required for release

    Inputs
    ======
    date: int
        Date in the format of YYYYMMDD
    OS: str
        Name of OS e.g. ubuntu-24.04
    arch: str
        x86_64/arm64
    malloc: bool
        True for malloc build
    relwithdebinfo: bool
        True for debug build

    Returns
    =======
    exists: bool
        True if the package is found in s3
    url: str
        either a URL if exists, otherwise a code
    
    """

    packages = list_daily_release_packages(date,return_url=False)

    if not OS in packages:
        return False, "os_not_found"
    
    arch_key = arch+"64"
    if relwithdebinfo:
        arch_key += "-debug"
    
    if malloc:
        arch_key += "-malloc"

    if not arch_key in packages[OS]:
        return False, "arch_not_found"

    return True, packages[OS][arch_key]

def str2bool(value):
    """
    Convert a string to a boolean.

    Accepted values for True: 'yes', 'true', 't', 'y', '1'
    Accepted values for False: 'no', 'false', 'f', 'n', '0'
    """
    if isinstance(value, bool):
        return value
    value_lower = value.lower()
    if value_lower in ('yes', 'true', 't', 'y', '1'):
        return True
    elif value_lower in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError(f"Boolean value expected. Got '{value}'.")
    
def get_new_key(old_key: str, version: str, OS: str, arch: str, malloc: bool, relwithdebinfo: bool, test: bool) -> str:
    """
    Converts daily build S3 key to the release S3 key e.g.

    daily-build/memgraph/20250409/ubuntu-24.04-aarch64/memgraph_3.1.1+53~c325e60dc-1_arm64.deb
    becomes
    memgraph/v3.1.1/ubuntu-24.04-aarch64/memgraph_3.1.1-1_arm64.deb

    or 

    daily-build/memgraph/20250409/centos-10/memgraph-3.1.1_0.53.c325e60dc-1.x86_64.rpm
    becomes
    memgraph-release-test/v3.1.1/centos-10/memgraph-3.1.1_1-1.x86_64.rpm
    for test = True
    """

    #set the root directory and version
    if test:
        release_dir = f"memgraph-release-test"
    else:
        release_dir = f"memgraph"

    # extension
    ext = os.path.splitext(new_key)

    # alter OS if needed
    os_part = OS
    if arch == "arm":
        os_part = f"{os_part}-aarch64"
    if malloc:
        os_part = f"{os_part}-malloc"

    # start building new key
    join = "_" if ext == ".deb" else "-"
    new_key = f"{release_dir}/v{version}/{os_part}/memgraph{join}{version}"

    if ext == ".deb":
        new_key = f"{new_key}-1_{'amd64' if 'amd' in arch else 'arm64'}"
    elif ext == ".rpm":
        new_key = f"{new_key}_1-1.{'x86_64' if 'amd' in arch else 'aarch64'}.rpm"
    elif OS == "docker":
        if relwithdebinfo:
            new_key = f"{new_key}-docker.tar.gz"
        else:
            new_key = f"{new_key}-relwithdebinfo-docker.tar.gz"
    else:
        raise ValueError(f"Invalid package: {old_key}")
        
    return new_key

def main():
    parser = argparse.ArgumentParser(
        description="Check that a specific package exists in S3, returning new and old S3 keys"
    )
    parser.add_argument("date", type=str, help="Date as a string (e.g., '20250409').")
    parser.add_argument("version", type=str, help="Version string (x.y.z)")
    parser.add_argument("os", type=str, help="Operating system (e.g., 'ubuntu-24.04', 'docker').")
    parser.add_argument("arch", type=str, help="Architecture (e.g., 'amd', 'arm').")
    parser.add_argument("malloc", type=str2bool, help="Malloc flag as a boolean (e.g., true or false).")
    parser.add_argument("relwithdebinfo", type=str2bool, help="relwithdebinfo flag as a boolean (e.g., true or false).")
    parser.add_argument("test", type=str2bool, help="Test flag as a boolean (e.g., true or false).")
    
    args = parser.parse_args()

    exists, old_key = check_package_exists(
        args.date,
        args.os,
        args.arch,
        args.malloc,
        args.relwithdebinfo
    )

    if exists:
        new_key = get_new_key(
            old_key,
            args.version,
            args.os,
            args.arch,
            args.malloc,
            args.relwithdebinfo,
            args.test
        )
    else:
        new_key = "fail"

    # print whether it exists and the old and new keys for capture in bash
    print(f"{str(exists).lower()} {old_key} {new_key}")

