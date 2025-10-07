#!/usr/bin/env python3
import os
import sys
import urllib.request
import urllib.parse
import json


def main(pr_number: str, branch_name: str):
    # --- Environment variables from GitHub Actions ---
    repo = os.environ["GITHUB_REPOSITORY"]
    server_url = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    api_url = os.environ.get("GITHUB_API_URL", "https://api.github.com")
    run_id = os.environ["GITHUB_RUN_ID"]
    workflow_name = os.environ.get("GITHUB_WORKFLOW", "workflow")
    token = os.environ["GITHUB_TOKEN"]

    # --- Construct workflow run URL ---
    run_url = f"{server_url}/{repo}/actions/runs/{run_id}"

    # --- Prepare comment body ---
    body = (
        f"Staging branch created: `{branch_name}` :heavy_check_mark:\n\n"
        f"CI **{workflow_name}** run [here]({run_url}) :hourglass_flowing_sand:"
    )

    # --- API endpoint for PR comments ---
    url = f"{api_url}/repos/{repo}/issues/{pr_number}/comments"

    # --- Send the request ---
    data = json.dumps({"body": body}).encode('utf-8')
    
    request = urllib.request.Request(
        url,
        data=data,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "Content-Type": "application/json",
        },
        method="POST"
    )
    
    try:
        with urllib.request.urlopen(request) as response:
            status_code = response.getcode()
            response_text = response.read().decode('utf-8')
            
            if status_code == 201:
                print("✅ Comment posted successfully.")
                
                # Parse the response to extract the comment ID
                try:
                    response_data = json.loads(response_text)
                    comment_id = response_data.get('id')
                    if comment_id:
                        # Set the COMMENT_ID environment variable
                        os.environ['COMMENT_ID'] = comment_id
                        print(f"🔗 Comment ID: {comment_id}")
                        # Also output the URL in a format that can be captured by shell
                        print(f"COMMENT_ID={comment_id}")
                    else:
                        print("⚠️  Warning: Could not extract comment ID from response")
                except json.JSONDecodeError as e:
                    print(f"⚠️  Warning: Could not parse response JSON: {e}")
            else:
                print(f"❌ Failed to post comment: {status_code}")
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
    if len(sys.argv) != 3:
        print("Usage: post_pr_comment.py <pr_number> <branch_name>")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
