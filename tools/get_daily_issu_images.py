import os
from typing import List

from aggregate_build_tests import list_daily_release_packages


def get_daily_issu_images(date: int) -> List[str]:
    """
    Get the Releasse docker images for x86_64 and arm64 for the given date
    """
    packages = list_daily_release_packages(date, return_url=False)
    amd_docker = f"https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/{packages['docker']['x86_64']}"
    arm_docker = f"https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/{packages['docker']['arm64']}"
    return amd_docker, arm_docker


def main() -> None:
    """
    Print images to be used in ISSU test
    """
    date = int(os.getenv("CURRENT_BUILD_DATE"))
    mock = os.getenv("MOCK", "false") == "true"
    amd_docker, arm_docker = get_daily_issu_images(date)
    if mock:
        amd_docker = amd_docker.replace("daily-build/memgraph/", "daily-build/memgraph_mock/")
        arm_docker = arm_docker.replace("daily-build/memgraph/", "daily-build/memgraph_mock/")
    print(f"{amd_docker} {arm_docker}")


if __name__ == "__main__":
    main()
