import argparse
import subprocess
import sys


def check_packages(os, packages):
    """Check which packages are missing from the system"""
    apt_dists = ["debian", "ubuntu"]
    dnf_dists = ["fedora", "centos", "rocky"]

    if any([dist in os for dist in apt_dists]):
        return check_packages_apt(packages)
    elif any([dist in os for dist in dnf_dists]):
        return check_packages_dnf(packages)
    else:
        raise NotImplementedError(f"OS: {os} not supported")


def install_packages(os, packages, dry_run=False):
    """Install missing packages on the system"""
    apt_dists = ["debian", "ubuntu"]
    dnf_dists = ["fedora", "centos", "rocky"]

    if any([dist in os for dist in apt_dists]):
        return install_packages_apt(packages, dry_run)
    elif any([dist in os for dist in dnf_dists]):
        return install_packages_dnf(packages, dry_run)
    else:
        raise NotImplementedError(f"OS: {os} not supported")


def compare_packages(installed, packages):
    """
    Compare installed packages to list of required packages to check for
    """
    return [package for package in packages if not package in installed]


def check_packages_apt(packages):
    """Check which apt packages are missing"""
    # list installed packages
    apt = subprocess.run(["apt", "list", "--installed"], capture_output=True, text=True)
    installed = [item.split("/")[0] for item in apt.stdout.splitlines()]

    return compare_packages(installed, packages)


def check_packages_dnf(packages):
    """Check which dnf packages are missing"""
    # list installed packages
    rpm = subprocess.run(["rpm", "--query", "--all", "--queryformat", "%{NAME}\n"], capture_output=True, text=True)

    installed = rpm.stdout.splitlines()
    return compare_packages(installed, packages)


def install_packages_apt(packages, dry_run=False):
    """Install missing apt packages"""
    # Check which packages are missing
    missing = check_packages_apt(packages)

    if missing and not dry_run:
        print(f"Installing missing packages: {' '.join(missing)}")
        result = subprocess.run(["apt", "install", "-y"] + missing)
        if result.returncode != 0:
            print(f"Error installing packages: {result.stderr}")
            return False

    return True


def install_packages_dnf(packages, dry_run=False):
    """Install missing dnf packages"""
    # Check which packages are missing
    missing = check_packages_dnf(packages)

    if missing and not dry_run:
        print(f"Installing missing packages: {' '.join(missing)}")
        result = subprocess.run(["dnf", "install", "-y"] + missing)
        if result.returncode != 0:
            print(f"Error installing packages: {result.stderr}")
            return False

    return True


if __name__ == "__main__":
    """
    Usage:
    # Check missing packages
    python3 check-packages.py check ubuntu-24.04 package1 package2 package3

    # Install missing packages
    python3 check-packages.py install ubuntu-24.04 package1 package2 package3

    # Dry run (check what would be installed)
    python3 check-packages.py install --dry-run ubuntu-24.04 package1 package2 package3
    """
    parser = argparse.ArgumentParser(description="Check and install packages")
    parser.add_argument("action", choices=["check", "install"], help="Action to perform")
    parser.add_argument("os", help="Operating system identifier")
    parser.add_argument("packages", nargs="+", help="List of packages")
    parser.add_argument("--dry-run", action="store_true", help="Don't actually install packages")

    args = parser.parse_args()

    if args.action == "check":
        missing = check_packages(args.os, args.packages)
        print(" ".join(missing))
    elif args.action == "install":
        success = install_packages(args.os, args.packages, args.dry_run)
        if not success:
            sys.exit(1)
