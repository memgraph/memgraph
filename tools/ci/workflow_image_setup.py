import argparse
import re

from aggregate_build_tests import list_daily_release_packages

# Compile regex patterns
URL_PATTERN = re.compile(r"^(https?://[\w\-\.]+(?:/[\w\-\./?%&=]*)?)$")
DATE_PATTERN = re.compile(r"^(?P<year>\d{4})(?P<month>0[1-9]|1[0-2])(?P<day>0[1-9]|[12]\d|3[01])$")
DOCKER_PATTERN = re.compile(
    r"^([a-z0-9]+(?:[._\-][a-z0-9]+)*(?:/[a-z0-9]+(?:[._\-][a-z0-9]+)*)*):([A-Za-z0-9][A-Za-z0-9._\-]*)$"
)
VERSION_PATTERN = re.compile(r"^\d+\.\d+(?:\.\d+)?.*$")


def classify_string(s: str) -> str:
    """
    Classify the string provided to see if it is a URL, docker repo tag, or date.

    - [url] for URLs to files (starting with http:// or https://)
    - [date] for dates in the format yyyymmdd
    - [docker] for Docker image name:tag
    - [version] for version e.g. v3.2.0

    If no pattern matches, returns the original string unchanged.
    """
    if URL_PATTERN.match(s):
        return "url"
    elif DATE_PATTERN.match(s):
        return "date"
    elif DOCKER_PATTERN.match(s):
        return "docker"
    elif VERSION_PATTERN.match(s):
        return "version"
    else:
        return s


def get_daily_url(date: str, arch: str, malloc: bool, cuda: bool, relwithdebinfo: bool) -> str:
    """
    Given a date of the format yyyymmdd, find and return the URL of the
    appropriate image.
    """

    packages = list_daily_release_packages(int(date), return_url=True)

    try:
        arch_name = "x86_64" if arch == "amd" else "arm64"
        key = f"Docker ({arch_name})"
        suffixes = ""
        if relwithdebinfo:
            suffixes += "-relwithdebinfo"
        if malloc:
            suffixes += "-malloc"
        if cuda:
            suffixes += "-cuda"
        key_image = f"{arch_name}{suffixes}"
        url = packages[key][key_image]
    except KeyError:
        url = "fail"

    return url


def get_version_docker(version: str, malloc: str, cuda: str, relwithdebinfo: str):
    """
    convert version number to docker image tag

    This will only work for 3.0 onwards, for anything else supply the URL or
    full docker tag to the workflow
    """

    repo_tag = f"memgraph/memgraph-mage:{version}"

    if relwithdebinfo and "relwithdebinfo" not in version:
        repo_tag = f"{repo_tag}-relwithdebinfo"
    if cuda and "cuda" not in version:
        repo_tag = f"{repo_tag}-cuda"
    if malloc and "malloc" not in version:
        repo_tag = f"{repo_tag}-malloc"

    return repo_tag


def string_to_boolean(bool_str: str) -> bool:
    """
    convert string to Boolean
    """

    return True if bool_str.lower() == "true" else False


def main() -> None:
    """
    This function will parse the inputs for the smoke test workflow and
    determine whether the input is a `date`, `URL`, `docker` tag or `version`.

    For...
    `date`:
        attempt to get the appropriate daily build, return URL
    `version`:
        get dockerhub repo:tag - not that this will only work for
        memgraph/MAGE > 3.0 (i.e. since their versions matched)
    `URL`:
        nothing needs to be done here if it's a full URL to an image archive
    `docker`:
        nothing needs to be done as long as it's a valid `repo:tag` string

    Finally, prints either:
        - "url https://...."
        - "docker memgraph/..."
    which is captured by BASH within the workflow to either download the archive
    or pull from dockerhub.
    """
    parser = argparse.ArgumentParser(description="Check image/url/date")

    parser.add_argument("image", type=str, help="Image tag, URL or daily build date")

    parser.add_argument("arch", type=str, help="CPU Arch: arm|amd")

    parser.add_argument("malloc", type=str, help="Is a malloc build: true|false")

    parser.add_argument("cuda", type=str, help="Is a CUDA build: true|false")

    parser.add_argument("relwithdebinfo", type=str, help="Is a RelWithDebInfo build: true|false")

    args = parser.parse_args()

    # classify image
    cls = classify_string(args.image)

    if cls == "date":
        out = get_daily_url(
            args.image,
            args.arch,
            string_to_boolean(args.malloc),
            string_to_boolean(args.cuda),
            string_to_boolean(args.relwithdebinfo),
        )
        cls = "url"
    elif cls == "version":
        out = get_version_docker(
            args.image,
            string_to_boolean(args.malloc),
            string_to_boolean(args.cuda),
            string_to_boolean(args.relwithdebinfo),
        )
        cls = "docker"
    else:
        out = args.image

    print(f"{cls} {out}")


if __name__ == "__main__":
    main()
