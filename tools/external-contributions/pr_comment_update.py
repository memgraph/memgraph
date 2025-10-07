#!/usr/bin/env python3
import os
import sys
import urllib.request
import urllib.parse
import json


def main(comment_api_url: str, pr_number: str, branch_name: str, success_status: str):
    # --- Environment variables from GitHub Actions ---
    repo = os.environ["GITHUB_REPOSITORY"]
    server_url = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    api_url = os.environ.get("GITHUB_API_URL", "https://api.github.com")
    run_id = os.environ["GITHUB_RUN_ID"]
    workflow_name = os.environ.get("GITHUB_WORKFLOW", "workflow")
    token = os.environ["GITHUB_TOKEN"]

    # --- Construct workflow run URL ---
    run_url = f"{server_url}/{repo}/actions/runs/{run_id}"

    # --- Determine emoji based on success status ---
    if success_status.lower() in ['true', 'success', '1']:
        status_emoji = "✅"
        status_text = "completed"
    else:
        status_emoji = "❌"
        status_text = "failed"

    # --- Prepare updated comment body ---
    body = (
        f"Staging branch created: `{branch_name}` :heavy_check_mark:\n\n"
        f"CI **{workflow_name}** run [here]({run_url}) {status_emoji}"
    )

    # --- Send the PATCH request to update the comment ---
    data = json.dumps({"body": body}).encode('utf-8')
    
    request = urllib.request.Request(
        comment_api_url,
        data=data,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "Content-Type": "application/json",
        },
        method="PATCH"
    )
    
    try:
        with urllib.request.urlopen(request) as response:
            status_code = response.getcode()
            response_text = response.read().decode('utf-8')
            
            if status_code == 200:
                print(f"✅ Comment updated successfully with {status_text} status.")
            else:
                print(f"❌ Failed to update comment: {status_code}")
                print(response_text)
                sys.exit(1)
    except urllib.error.HTTPError as e:
        print(f"❌ HTTP Error {e.code}: {e.reason}")
        print(e.read().decode('utf-8'))
        sys.exit(1)
    except urllib.error.URLError as e:
        print(f"❌ URL Error: {e.reason}")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: pr_comment_update.py <comment_api_url> <pr_number> <branch_name> <success_status>")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
