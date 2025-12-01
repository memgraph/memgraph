import re
import argparse

# Compile regex patterns
URL_PATTERN = re.compile(
    r'^(https?://[\w\-\.]+(?:/[\w\-\./?%&=]*)?)$'
)
DOCKER_PATTERN = re.compile(
    r'^([a-z0-9]+(?:[._\-][a-z0-9]+)*(?:/[a-z0-9]+(?:[._\-][a-z0-9]+)*)*):([A-Za-z0-9][A-Za-z0-9._\-]*)$'
)
VERSION_PATTERN = re.compile(
    r'^\d+\.\d+(?:\.\d+)?$'
)


def classify_string(s: str) -> str:
    """
    Classify the string provided to see if it is a URL, docker repo tag, or version.

    - [url] for URLs to files (starting with http:// or https://)
    - [docker] for Docker image name:tag
    - [version] for version e.g. v3.2.0

    If no pattern matches, returns the original string unchanged.
    """
    if URL_PATTERN.match(s):
        return "url"
    elif DOCKER_PATTERN.match(s):
        return "docker"
    elif VERSION_PATTERN.match(s):
        return "version"
    else:
        return s



def get_version_docker(version: str):
    """
    convert version number to docker image tag
    """

    repo_tag = f"memgraph/memgraph:{version}"

    return repo_tag


def string_to_boolean(bool_str: str) -> bool:
    """
    convert string to Boolean
    """

    return True if bool_str.lower() == "true" else False


def main() -> None:
    """
    This function will parse the inputs for the smoke test workflow and 
    determine whether the input is a `URL`, `docker` tag or `version`.

    For...
    `version`:
        get dockerhub repo:tag
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
    parser = argparse.ArgumentParser(description="Check image/url")

    parser.add_argument(
        "image",
        type=str,
        help="Image tag, URL"
    )
    args = parser.parse_args()

    # classify image
    cls = classify_string(args.image)

    if cls == "version":
        out = get_version_docker(args.image)
        cls = "docker"
    else:
        out = args.image

    print(f"{cls} {out}")


if __name__ == "__main__":
    main()
