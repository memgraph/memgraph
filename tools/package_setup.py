#!/usr/bin/env python3
import argparse
import json
import os
import sys


class PackageSetup:
    def __init__(self, gh_context_path: str):
        self._gh_context_path = gh_context_path
        self._get_default_package_suite(False)
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

    def get_package_suite(self) -> dict:
        return self._package_suite

    def _get_default_package_suite(self, value: bool) -> None:
        # Define all the package targets that can be controlled via PR labels
        self._package_suite = {
            "centos-9": value,
            "centos-10": value,
            "debian-12": value,
            "debian-12-arm": value,
            "debian-13": value,
            "debian-13-arm": value,
            "docker": value,
            "docker-arm": value,
            "fedora-42": value,
            "fedora-42-arm": value,
            "rocky-10": value,
            "ubuntu-22.04": value,
            "ubuntu-24.04": value,
            "ubuntu-24.04-arm": value,
            "ubuntu-24.04-malloc": value,
        }

    def _check_pr_label(self, package: str, pr_labels: list) -> bool:
        if f"CI -package={package}" in pr_labels:
            return True
        return False

    def _setup_pull_request(self) -> None:
        pr_labels = self._get_pr_labels()
        print(f"PR labels: {pr_labels}")
        for package in self._package_suite.keys():
            self._package_suite[package] = self._check_pr_label(package, pr_labels)

    def _check_workflow_input(self, package: str, workflow_dispatch_inputs: dict) -> bool:
        # For workflow_dispatch, check if the OS input matches the package
        os_input = workflow_dispatch_inputs.get("os", "")
        if os_input == package or os_input == "all":
            return True
        return False

    def get_workflow_inputs(self) -> dict:
        """Get the workflow inputs for passing to reusable workflows"""
        if self._get_event_name() == "workflow_dispatch":
            inputs = self._get_workflow_dispatch_inputs()
            return inputs
        else:
            # For pull_request events, return default values
            return {
                "build_type": "Release",
                "toolchain": "v6",
                "push_to_s3": "false",
                "s3_dest_dir": "",
                "push_to_github": "false",
                "malloc": "false",
            }

    def _setup_workflow_dispatch(self) -> None:
        workflow_dispatch_inputs = self._get_workflow_dispatch_inputs()
        print(f"Workflow dispatch inputs: {workflow_dispatch_inputs}")
        for package in self._package_suite.keys():
            self._package_suite[package] = self._check_workflow_input(package, workflow_dispatch_inputs)

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


def print_package_suite(packages: dict, workflow_inputs: dict, set_env_vars: bool = False) -> None:
    for package, run in packages.items():
        # Convert package names to valid GitHub output names (replace dots and hyphens with underscores)
        output_name = package.replace(".", "_").replace("-", "_")
        print(f"run_package_{output_name}={run}")
        if set_env_vars:
            os.popen(f"echo run_package_{output_name}={run} >> $GITHUB_OUTPUT")

    # Also output workflow inputs
    for key, value in workflow_inputs.items():
        print(f"workflow_input_{key}={value}")
        if set_env_vars:
            os.popen(f"echo workflow_input_{key}={value} >> $GITHUB_OUTPUT")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Setup package workflow package suite")
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
    package_suite = package_setup.get_package_suite()
    workflow_inputs = package_setup.get_workflow_inputs()
    print_package_suite(packages=package_suite, workflow_inputs=workflow_inputs, set_env_vars=True)
