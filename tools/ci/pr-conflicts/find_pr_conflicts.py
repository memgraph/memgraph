"""
Find the other open pull requests that touch the same files as a given PR.

Reads the output of:

    gh pr list --state open --limit <n> --json number,files

and, if any other PR modifies a file this PR also modifies, writes a Markdown
message listing those PRs (most shared files first) to the output path. If
there are no overlaps, the output file is not written.
"""

import argparse
import json


def load_pr_files(json_path: str) -> dict[int, set[str]]:
    """
    Map each PR number in the `gh pr list` JSON to the set of paths it touches.
    """
    with open(json_path, "r") as f:
        prs = json.load(f)

    return {pr["number"]: {file["path"] for file in pr.get("files") or []} for pr in prs}


def find_conflicts(pr_files: dict[int, set[str]], pr_number: int) -> list[tuple[int, int]]:
    """
    Return (other PR number, shared file count) for every other PR sharing at
    least one file with `pr_number`, most shared files first.
    """
    own_files = pr_files.get(pr_number, set())

    conflicts = []
    for other, files in pr_files.items():
        if other == pr_number:
            continue
        shared = len(own_files & files)
        if shared:
            conflicts.append((other, shared))

    conflicts.sort(key=lambda c: (-c[1], c[0]))
    return conflicts


def build_message(conflicts: list[tuple[int, int]]) -> str:
    """
    Build the PR comment listing the conflicting PRs.
    """
    lines = [
        "This PR has potential conflicts with the following other open pull requests which modify the same files:",
        "",
    ]
    for other, shared in conflicts:
        lines.append(f"- #{other}: {shared} file{'s' if shared != 1 else ''}")

    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description="Find open PRs that modify the same files as a given PR.")
    parser.add_argument("json_path", type=str, help="Path to the `gh pr list --json number,files` output.")
    parser.add_argument("pr_number", type=int, help="Number of the PR to check.")
    parser.add_argument("output_path", type=str, help="Where to write the message, if there are conflicts.")
    args = parser.parse_args()

    conflicts = find_conflicts(load_pr_files(args.json_path), args.pr_number)
    if not conflicts:
        print(f"No open PRs share files with #{args.pr_number}.")
        return

    message = build_message(conflicts)
    with open(args.output_path, "w") as f:
        f.write(message)
    print(message)


if __name__ == "__main__":
    main()
