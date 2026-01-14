import argparse
import os

from aggregate_build_tests import list_daily_release_packages


def main(arch: str, image_type: str) -> None:
    """
    print the relevant image URL to be scanned for CVEs

    Inputs
    ======
    arch: str
        the architecture of the image to be scanned for CVEs
    image_type: str
        the type of image to be scanned for CVEs, either 'memgraph' or 'mage'

    """
    date_str = os.getenv("CURRENT_BUILD_DATE")
    if not date_str:
        raise ValueError("CURRENT_BUILD_DATE environment variable is required")
    date = int(date_str)

    packages = list_daily_release_packages(date, image_type=image_type)

    # translate to dict key
    if image_type == "memgraph":
        key = "Memgraph"
        arch_key = "arm64" if arch == "arm64" else "x86_64"
        url = packages["docker"][arch_key]
    elif image_type == "mage":
        key, arch_key = ("Docker (arm64)", "arm64") if arch == "arm64" else ("Docker (x86_64)", "x86_64")
        url = packages[key][arch_key]

    print(url)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("arch", type=str)
    parser.add_argument("image_type", type=str, choices=["memgraph", "mage"], help="type of image to use")
    args = parser.parse_args()

    main(args.arch, args.image_type)
