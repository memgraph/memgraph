"""
Resolve S3 paths for a single daily-build Docker image variant.

Looks up the AMD and ARM image tarballs in the daily-build S3 bucket using
the same parsing as `aggregate_build_tests.list_daily_release_packages`, and
prints `s3_path_amd=...` and `s3_path_arm=...` lines suitable for piping into
`$GITHUB_ENV`.
"""

import argparse
from typing import Tuple

from aggregate_build_tests import list_daily_release_packages


def resolve(
    image_type: str,
    date: int,
    arch_key_amd: str,
    arch_key_arm: str,
    mock: bool,
    bucket: str,
) -> Tuple[str, str]:
    packages = list_daily_release_packages(date, return_url=False, image_type=image_type, mock=mock)

    if image_type == "memgraph":
        amd_pkgs = packages.get("docker", {})
        arm_pkgs = amd_pkgs
    elif image_type == "mage":
        amd_pkgs = packages.get("Docker (x86_64)", {})
        arm_pkgs = packages.get("Docker (arm64)", {})
    else:
        raise ValueError(f"Unsupported image_type: {image_type}")

    amd_key = amd_pkgs.get(arch_key_amd, "") if arch_key_amd else ""
    arm_key = arm_pkgs.get(arch_key_arm, "") if arch_key_arm else ""

    amd_path = f"s3://{bucket}/{amd_key}" if amd_key else ""
    arm_path = f"s3://{bucket}/{arm_key}" if arm_key else ""
    return amd_path, arm_path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("image_type", choices=["memgraph", "mage"])
    parser.add_argument("--date", type=int, required=True, help="Build date (YYYYMMDD)")
    parser.add_argument("--arch-key-amd", default="", help="arch key to look up for amd (e.g. x86_64-relwithdebinfo)")
    parser.add_argument("--arch-key-arm", default="", help="arch key to look up for arm (e.g. arm64-relwithdebinfo)")
    parser.add_argument("--mock", action="store_true", help="Use the mock daily-build prefix")
    parser.add_argument("--bucket", default="deps.memgraph.io")
    args = parser.parse_args()

    amd_path, arm_path = resolve(
        image_type=args.image_type,
        date=args.date,
        arch_key_amd=args.arch_key_amd or "",
        arch_key_arm=args.arch_key_arm or "",
        mock=args.mock,
        bucket=args.bucket,
    )

    print(f"s3_path_amd={amd_path}")
    print(f"s3_path_arm={arm_path}")


if __name__ == "__main__":
    main()
