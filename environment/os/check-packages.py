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

def check_packages_apt(packages):

    # list installed packages
    apt = subprocess.run(
        ["apt","list","--installed"],
        capture_output=True,
        text=True
    )
    installed = [item.split("/")[0] for item in apt.stdout.splitlines()]

    # compare lists of packages and return missing ones
    missing = [package for package in packages if not package in installed]

    return missing

def check_packages_dnf(packages):

    pass



if __name__ == "__main__":
    """
    Call me with the OS and a list of packages to check, e.g.
    packages=$(python3 check-packages.py "$os" "${package_list[@]})
    """
    os = sys.argv[1]
    packages = sys.argv[2:]
    missing = check_packages(os,packages)
    print(" ".join(missing))