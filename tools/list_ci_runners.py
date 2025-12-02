#!/usr/bin/env python3
import json
import os
import sys
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def fetch_self_hosted_runners(api_url: str, token: str):
    """Return a list of self-hosted runner names for the given repo."""
    runners = []
    page = 1

    while True:
        query = urlencode({"per_page": 100, "page": page})
        url = f"{api_url}/orgs/memgraph/actions/runners?{query}"

        req = Request(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                # Not strictly required, but recommended for newer GitHub APIs
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )

        try:
            with urlopen(req) as resp:
                body = resp.read().decode("utf-8")
        except HTTPError as e:
            print(f"GitHub API HTTP error: {e.code} {e.reason}", file=sys.stderr)
            sys.exit(1)
        except URLError as e:
            print(f"GitHub API connection error: {e.reason}", file=sys.stderr)
            sys.exit(1)

        data = json.loads(body)
        page_runners = [r["name"] for r in data.get("runners", [])]
        runners.extend(page_runners)

        # Basic pagination: stop if less than page size
        if len(page_runners) < 100:
            break

        page += 1

    return runners


def main():
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        print("GITHUB_TOKEN is not set", file=sys.stderr)
        sys.exit(1)

    # From the workflow input (passed in via env)
    runner_input = os.getenv("RUNNER_INPUT", "all").strip()

    all_runners = fetch_self_hosted_runners("https://api.github.com", token)

    if not all_runners:
        print("No self-hosted runners found for this repository.", file=sys.stderr)

    all_set = set(all_runners)

    if runner_input.lower() in ("all", "*"):
        selected = sorted(all_set)
    else:
        requested = {r.strip() for r in runner_input.split(",") if r.strip()}
        selected = sorted(all_set & requested)
        missing = requested - all_set
        if missing:
            print(
                "Warning: requested runners not found: " + ", ".join(sorted(missing)),
                file=sys.stderr,
            )

    # This will be consumed as: fromJson(needs.prepare-matrix.outputs.runners)
    runners_json = json.dumps(selected)

    gh_output = os.getenv("GITHUB_OUTPUT")
    if gh_output:
        # New-style GitHub Actions output
        with open(gh_output, "a", encoding="utf-8") as f:
            f.write(f"runners={runners_json}\n")
    else:
        # Fallback: print to stdout
        print(runners_json)


if __name__ == "__main__":
    main()
