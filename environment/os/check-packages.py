import sys
import subprocess


def check_packages(os,packages):

    apt_dists = ["debian","ubuntu"]
    dnf_dists = ["fedora","centos","rocky"]

    if any([dist in os for dist in apt_dists]):
        return check_packages_apt(packages)
    elif any([dist in os for dist in dnf_dists]):
        return check_packages_dnf(packages)
    else:
        raise NotImplementedError(f"OS: {os} not supported")


def compare_packages(installed,packages):
    """
    Compared installed packages to list of required packages to check for
    """
    return [package for package in packages if not package in installed]


def check_packages_apt(packages):

    # list installed packages
    apt = subprocess.run(
        ["apt","list","--installed"],
        capture_output=True,
        text=True
    )
    installed = [item.split("/")[0] for item in apt.stdout.splitlines()]

    return compare_packages(installed,packages)

def check_packages_dnf(packages):

    # list installed packages
    rpm = subprocess.run(
        ["rpm","--query","--all","--queryformat","'%{NAME}\n'"],
        capture_output=True,
        test=True
    )
    
    installed = rpm.stdout.splitlines()

    return compare_packages(installed,packages)



if __name__ == "__main__":
    """
    Call me with the OS and a list of packages to check, e.g.
    packages=$(python3 check-packages.py "$os" "${package_list[@]})
    """
    os = sys.argv[1]
    packages = sys.argv[2:]
    missing = check_packages(os,packages)
    print(" ".join(missing))