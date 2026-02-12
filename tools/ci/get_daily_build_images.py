import argparse
import os
from typing import Tuple

from aggregate_build_tests import list_daily_release_packages


def get_daily_build_images(date: int, mock: bool = False) -> Tuple[str, str]:
    """
    Get the Releasse docker images for x86_64 and arm64 for the given date
    """
    packages = list_daily_release_packages(date, image_type="memgraph", return_url=False, mock=mock)
    amd_docker = f"https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/{packages['docker']['x86_64']}"
    arm_docker = f"https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/{packages['docker']['arm64']}"
    return amd_docker, arm_docker


def get_daily_build_mage_images(date: int, mock: bool = False) -> Tuple[str, str]:
    """
    Get the Releasse Memgraph MAGE docker images for x86_64 and arm64 for the given date
    """
    packages = list_daily_release_packages(date, image_type="mage", return_url=False, mock=mock)
    amd_docker = f"https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/{packages['docker']['x86_64']}"
    arm_docker = f"https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/{packages['docker']['arm64']}"
    return amd_docker, arm_docker


def main() -> None:
    """
    Print images to be used in ISSU test
    """
    parser = argparse.ArgumentParser(description="Get daily build docker images")
    parser.add_argument(
        "--type", choices=["memgraph", "mage"], default="memgraph", help="Type of image to get (default: memgraph)"
    )
    args = parser.parse_args()

    current_build_date = os.getenv("CURRENT_BUILD_DATE")
    if current_build_date is None:
        raise ValueError("CURRENT_BUILD_DATE is not set")
    date = int(current_build_date)
    mock = os.getenv("MOCK", "false") == "true"

    if args.type == "mage":
        amd_docker, arm_docker = get_daily_build_mage_images(date, mock)
    else:
        amd_docker, arm_docker = get_daily_build_images(date, mock)

    print(f"{amd_docker} {arm_docker}")


if __name__ == "__main__":
    main()
