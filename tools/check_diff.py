#!/usr/bin/env python3

import json
import os
import sys


class GHContext:
    def __init__(self, gh_context_path: str):
        self._gh_context_path = gh_context_path
        with open(self._gh_context_path, 'r') as gh_context_file:
            self._gh_context = json.load(gh_context_file)

    def get_event_name(self) -> str:
        return self._gh_context.get("event_name")

    def get_pr_labels(self) -> list:
        return [label.get("name") for label in self._gh_context.get("event").get("pull_request").get("labels")]

    def get_workflow_dispatch_inputs(self) -> dict:
        return self._gh_context.get("event").get("inputs")


def echo_outputs(tests: dict) -> None:
    for build, tests in tests.items():
        for test, run in tests.items():
            print(f"run_{build}_{test}={run}")
            os.popen(f'echo run_{build}_{test}={run} >> $GITHUB_OUTPUT')

def check_diff_workflow(base_branch: str) -> bool:
    for file in os.popen(f"git diff --name-only {base_branch}").read().splitlines():
        if file.startswith(".github/workflows/"):
            if file.startswith(".github/workflows/diff.yml"):
                return True
        else:
            return True
    return False

def get_default_tests(value: bool=False) -> dict:
    return {"community": {"core": value},
            "coverage": {"core": value},
            "debug": {"core": value, "integration": value},
            "jepsen": {"core": value},
            "release": {"core": value, "benchmark": value, "e2e": value, "stress": value}}

def check_tests(gh_context: GHContext) -> dict:
    event_name=gh_context.get_event_name()
    build_tests=get_default_tests(False)
    
    print(f"Event name: {event_name}")
    if event_name=="merge_group":
        build_tests=get_default_tests(True)

    elif event_name=="pull_request":
        pr_labels=gh_context.get_pr_labels()
        print(f"PR labels: {pr_labels}")
        for build, tests in build_tests.items():
            for test in tests.keys():
                if f"CI -build={build} -test={test}" in pr_labels:
                    build_tests[build][test] = True
    
    elif event_name=="workflow_dispatch":
        workflow_dispatch_inputs=gh_context.get_workflow_dispatch_inputs()
        print(f"Workflow dispatch inputs: {workflow_dispatch_inputs}")
        for build, tests in build_tests.items():
            for test in tests.keys():
                if workflow_dispatch_inputs.get(f"{build}_{test}") == 'true':
                    build_tests[build][test] = True
    else:
        print("Invalid event name")
        sys.exit(1)

    return build_tests


if __name__ == "__main__":
    run_diff=check_diff_workflow(base_branch="origin/master")
    if run_diff:
        gh_context=GHContext(gh_context_path=sys.argv[1])
        tests=check_tests(gh_context=gh_context)
    else:
        tests=get_default_tests(False)
    echo_outputs(tests)
    sys.exit(0)
