#!/usr/bin/env python3
import argparse
import json
import os
import sys

# Distros the built packages can be smoke tested on; must match the resolver
# in .github/workflows/reusable_package.yaml.
SMOKE_TARGETS = [
    "centos-9",
    "centos-10",
    "debian-12",
    "debian-13",
    "fedora-43",
    "fedora-44",
    "fedora-45",
    "rocky-10",
    "ubuntu-22.04",
    "ubuntu-24.04",
    "ubuntu-26.04",
]
SMOKE_GROUPS = ["all", "all-deb", "all-rpm"]
LABEL_PREFIX = "CI -package="
DEFAULT_OS = "ubuntu-24.04"


class PackageSetup:
    """Turn the triggering event into inputs for the single package job.

    The os input picks the mgbuild container the build runs in (the packages
    are distro-agnostic). PR labels no longer choose a build OS — they define
    which distros the packages are smoke tested on (package_smoke_os).
    """

    def __init__(self, gh_context_path: str):
        self._gh_context_path = gh_context_path
        self._run_package = False
        self._os = DEFAULT_OS
        self._build_docker_image = "none"
        self._workflow_inputs = {}
        self._load_gh_context()

    def _load_gh_context(self) -> None:
        try:
            with open(self._gh_context_path, "r") as gh_context_file:
                self._gh_context = json.load(gh_context_file)
                if not self._get_event_name():
                    raise KeyError

        except FileNotFoundError:
            print(f"Error: file not found {self._gh_context_path}")
            sys.exit(1)
        except json.JSONDecodeError:
            print(f"Error: invalid JSON file {self._gh_context_path}")
            sys.exit(1)
        except KeyError:
            print(f"Error: invalid GitHub context file {self._gh_context_path}")
            sys.exit(1)

    def _get_event_name(self) -> str:
        return self._gh_context.get("event_name")

    def _get_pr_labels(self) -> list:
        return [label.get("name") for label in self._gh_context.get("event").get("pull_request").get("labels")]

    def _get_workflow_dispatch_inputs(self) -> dict:
        return self._gh_context.get("event").get("inputs")

    def _setup_pull_request(self) -> None:
        pr_labels = self._get_pr_labels()
        print(f"PR labels: {pr_labels}")
        values = [label[len(LABEL_PREFIX) :] for label in pr_labels if label.startswith(LABEL_PREFIX)]

        smoke_tokens = []
        for value in values:
            if value == "docker":
                self._build_docker_image = "prod"
            elif value in SMOKE_GROUPS or value in SMOKE_TARGETS:
                smoke_tokens.append(value)
            else:
                print(f"Warning: ignoring unknown package label value '{value}'")

        # Any package label runs the (single) package job; the smoke token
        # labels define where the packages get smoke tested.
        self._run_package = bool(values)
        package_smoke_os = "all" if "all" in smoke_tokens else " ".join(smoke_tokens)

        self._workflow_inputs = {
            "push_to_s3": "false",
            "s3_dest_dir": "",
            "push_to_github": "false",
            "malloc": "false",
            "generate_sbom": "false",
            "run_smoke_tests": "true",
            # Empty means "smoke test on the build container's distro"
            # (resolved in reusable_package.yaml).
            "package_smoke_os": package_smoke_os,
        }

    def _setup_workflow_dispatch(self) -> None:
        inputs = self._get_workflow_dispatch_inputs()
        print(f"Workflow dispatch inputs: {inputs}")
        self._run_package = True
        self._os = inputs.get("os") or DEFAULT_OS
        self._build_docker_image = inputs.get("build_docker_image") or "none"
        self._workflow_inputs = dict(inputs)
        self._workflow_inputs.pop("build_docker_image", None)

    def setup_package_workflow(self) -> None:
        event_name = self._get_event_name()
        print(f"Event name: {event_name}")
        if event_name == "pull_request":
            self._setup_pull_request()
        elif event_name == "workflow_dispatch":
            self._setup_workflow_dispatch()
        else:
            print("Invalid event name")
            sys.exit(1)

    def get_outputs(self) -> dict:
        # The '-arm' suffixed os choices select the same container image on an
        # arm runner; reusable_package.yaml takes os and arch separately.
        package_os = self._os
        package_arch = "amd"
        if package_os.endswith("-arm"):
            package_os = package_os[: -len("-arm")]
            package_arch = "arm"

        outputs = {
            "run_package": self._run_package,
            "package_os": package_os,
            "package_arch": package_arch,
        }
        for key, value in self._workflow_inputs.items():
            outputs[f"workflow_input_{key}"] = value
        outputs["workflow_input_build_docker_image"] = self._build_docker_image
        return outputs


def print_outputs(outputs: dict, set_env_vars: bool = False) -> None:
    gh_output = open(os.environ["GITHUB_OUTPUT"], "a") if set_env_vars else None
    try:
        for key, value in outputs.items():
            print(f"{key}={value}")
            if gh_output:
                gh_output.write(f"{key}={value}\n")
    finally:
        if gh_output:
            gh_output.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Setup package workflow inputs")
    parser.add_argument(
        "--gh-context-path",
        type=str,
        required=True,
        help="Path to json file containing the GitHub context for workflow run",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    package_setup = PackageSetup(args.gh_context_path)
    package_setup.setup_package_workflow()
    print_outputs(package_setup.get_outputs(), set_env_vars=True)
