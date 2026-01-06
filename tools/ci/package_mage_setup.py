import argparse
import json
import os
import sys

PR_BUILDS = ["amd", "arm", "cuda"]

MATRIX_BUILDS = [
    {
        "arch": "amd",
        "build_type": "Release",
        "cuda": False,
        "malloc": False,
    },
    {
        "arch": "arm",
        "build_type": "Release",
        "cuda": False,
        "malloc": False,
    },
    {
        "arch": "amd",
        "build_type": "RelWithDebInfo",
        "cuda": False,
        "malloc": False,
    },
    {
        "arch": "arm",
        "build_type": "RelWithDebInfo",
        "cuda": False,
        "malloc": False,
    },
    {
        "arch": "amd",
        "build_type": "RelWithDebInfo",
        "cuda": True,
        "malloc": False,
    },
    {
        "arch": "amd",
        "build_type": "Release",
        "cuda": False,
        "malloc": True,
    },
]


class PackageMageSetup:
    def __init__(self, gh_context_path: str):
        self._gh_context_path = gh_context_path
        self._package_suite = []
        self._load_gh_context()
        self.event_name = self._get_event_name()
        self.workflow_inputs = self.get_workflow_inputs()
        self.setup_package_workflow()

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

    def get_package_suite(self) -> dict:
        return self._package_suite

    def _check_pr_label(self, package: str, pr_labels: list) -> bool:
        default_args = {
            "malloc": False,
            "memgraph_download_link": "",
            "push_to_s3": False,
            "s3_dest_dir": "mage-unofficial",
            "run_smoke_tests": False,
            "run_tests": False,
            "package_deb": "default",
            "generate_sbom": False,
            "ref": "",
        }
        if f"CI -package=mage-{package}" in pr_labels:
            print(f'Found label for "{package}"')
            out = {"build_type": "Release", "arch": "arm" if package == "arm" else "amd", "cuda": package == "cuda"}
            out.update(default_args)
            return out
        return None

    def _setup_pull_request(self) -> None:
        pr_labels = self._get_pr_labels()
        for package in PR_BUILDS:
            build = self._check_pr_label(package, pr_labels)
            if build:
                self._package_suite.append(build)

    def get_workflow_inputs(self) -> dict:
        if self._get_event_name() in ["workflow_dispatch", "workflow_call"]:
            return self._gh_context.get("event").get("inputs")
        return None

    def _check_workflow_input(self) -> bool:
        if self.workflow_inputs.get(f"matrix_build") == "true":
            return MATRIX_BUILDS
        return [
            {
                "build_type": self.workflow_inputs.get("build_type", "Release"),
                "arch": self.workflow_inputs.get("build_arch", "amd"),
                "cuda": self.workflow_inputs.get("cuda", False),
                "malloc": self.workflow_inputs.get("malloc", False),
                "memgraph_download_link": self.workflow_inputs.get("memgraph_download_link", ""),
                "push_to_s3": self.workflow_inputs.get("push_to_s3", False),
                "s3_dest_dir": self.workflow_inputs.get("s3_dest_dir", "mage-unofficial"),
                "run_smoke_tests": self.workflow_inputs.get("run_smoke_tests", False),
                "run_tests": self.workflow_inputs.get("run_tests", False),
                "package_deb": self.workflow_inputs.get("package_deb", "default"),
                "generate_sbom": self.workflow_inputs.get("generate_sbom", False),
                "ref": self.workflow_inputs.get("ref", ""),
            }
        ]

    def setup_package_workflow(self) -> None:
        print(f"Event name: {self.event_name}")
        if self.event_name == "pull_request":
            self._setup_pull_request()
        elif self.event_name in ["workflow_dispatch", "workflow_call"]:
            self._package_suite = self._check_workflow_input()
        else:
            print("Invalid event name")
            sys.exit(1)

    def get_package_suite(self) -> dict:
        return self._package_suite


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Setup package workflow package suite")
    parser.add_argument(
        "--gh-context-path",
        type=str,
        required=True,
        help="Path to json file containing the GitHub context for workflow run",
    )
    return parser.parse_args()


def set_output(name: str, value: str) -> None:
    out_path = os.environ.get("GITHUB_OUTPUT")
    if not out_path:
        raise RuntimeError("GITHUB_OUTPUT is not set")
    with open(out_path, "a", encoding="utf-8") as f:
        f.write(f"{name}={value}\n")


def print_package_suite(package_suite: dict) -> None:
    for build in package_suite:
        print("--------------------------------")
        print(f"Build: {build}")
        print(f"Build type: {build.get('build_type')}")
        print(f"Arch: {build.get('arch')}")
        print(f"CUDA: {build.get('cuda')}")
        print(f"Malloc: {build.get('malloc')}")
        print(f"Memgraph download link: {build.get('memgraph_download_link')}")
        print(f"Push to S3: {build.get('push_to_s3')}")
        print(f"S3 dest dir: {build.get('s3_dest_dir')}")
        print(f"Run smoke tests: {build.get('run_smoke_tests')}")
        print(f"Run tests: {build.get('run_tests')}")
        print(f"Package deb: {build.get('package_deb')}")
        print(f"Generate SBOM: {build.get('generate_sbom')}")
        print(f"Ref: {build.get('ref')}")

    build_packages = "true" if len(package_suite) > 0 else "false"
    set_output("build_packages", build_packages)

    suite_json = json.dumps(package_suite, separators=(",", ":"))
    set_output("package_suite", suite_json)


if __name__ == "__main__":
    args = parse_args()
    package_mage_setup = PackageMageSetup(args.gh_context_path)
    package_suite = package_mage_setup.get_package_suite()
    print_package_suite(package_suite)
    print(json.dumps(package_suite, indent=2))
