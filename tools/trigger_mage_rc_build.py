import argparse
import os

import requests

"""
This script triggers a MAGE build after a Memgraph release candidate (RC) is created.
"""


def parse_args():
    parser = argparse.ArgumentParser(description="Trigger MAGE build for Memgraph RC.")
    parser.add_argument("--version", required=True, help="Version of the release candidate.")
    parser.add_argument("--rc_number", required=True, help="Release candidate number.")
    parser.add_argument("--branch_name", required=True, help="Branch name for the release candidate.")
    parser.add_argument("--tag_name", required=True, help="Tag name for the release candidate.")
    return parser.parse_args()


def main():
    args = parse_args()

    payload = {
        "event_type": "trigger_rc_build",
        "client_payload": {
            "version": args.version,
            "rc_number": args.rc_number,
            "branch_name": args.branch_name,
            "tag_name": args.tag_name,
        },
    }
    print(payload)

    headers = {
        "Accept": "application/vnd.github.v3+json",
        "Authorization": f"Bearer {os.getenv('GITHUB_TOKEN')}",
    }

    response = requests.post(url="https://api.github.com/repos/memgraph/mage/dispatches", json=payload, headers=headers)

    if response.status_code == 204:
        print("MAGE build triggered successfully.")
    else:
        print(f"Failed to trigger MAGE build: {response.status_code} - {response.text}")


if __name__ == "__main__":
    main()
