#!/bin/bash

# Script to fetch PR information and generate staging branch name
# Usage: ./get_pr_info.sh <PR_NUMBER> <GITHUB_TOKEN>
# Outputs: pr_number branch_name commit_sha staging_branch short_sha (space-separated)

set -e

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <PR_NUMBER> <GITHUB_TOKEN>"
    echo "Example: $0 123 \${{ github.token }}"
    exit 1
fi

PR_NUMBER="$1"
GITHUB_TOKEN="$2"

# Fetch PR details using GitHub API
PR_INFO=$(curl -s -H "Authorization: token $GITHUB_TOKEN" \
    "https://api.github.com/repos/memgraph/memgraph/pulls/$PR_NUMBER")

# Extract information using jq
BRANCH_NAME=$(echo "$PR_INFO" | jq -r '.head.ref')
COMMIT_SHA=$(echo "$PR_INFO" | jq -r '.head.sha')
REPO_URL=$(echo "$PR_INFO" | jq -r '.head.repo.clone_url')
REPO_OWNER=$(echo "$PR_INFO" | jq -r '.head.user.login')

# Validate that we got the required information
if [ "$BRANCH_NAME" = "null" ] || [ "$COMMIT_SHA" = "null" ]; then
    echo "Error: Could not fetch PR information for PR #$PR_NUMBER"
    echo "Response: $PR_INFO"
    exit 1
fi

# Create staging branch name
STAGING_BRANCH="staging/${COMMIT_SHA:0:7}"
SHORT_SHA="${COMMIT_SHA:0:7}"

# Output all variables in a single line (space-separated)
echo "$PR_NUMBER $BRANCH_NAME $COMMIT_SHA $STAGING_BRANCH $SHORT_SHA $REPO_URL $REPO_OWNER"
