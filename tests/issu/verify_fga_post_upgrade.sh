#!/usr/bin/env bash
set -Eeuo pipefail

# Verifies that FGA grants survive the rolling upgrade.
# Runs on the post-upgrade MAIN pod.
# Usage: verify_fga_post_upgrade.sh <mgconsole_auth_args...>
#   e.g. verify_fga_post_upgrade.sh --username=system_admin_user --password=admin_password

ROLES_TO_CHECK=("system_admin" "tenant1_admin")
EXPECTED_LABEL="LABEL PERMISSION GRANTED"
EXPECTED_EDGE="EDGE_TYPE PERMISSION GRANTED"

FAILED=0
for role in "${ROLES_TO_CHECK[@]}"; do
  echo "Checking FGA for role: ${role}"
  OUTPUT=$(echo "SHOW PRIVILEGES FOR ${role};" | mgconsole "$@" -output_format csv)

  if ! echo "$OUTPUT" | grep -q "$EXPECTED_LABEL"; then
    echo "FAIL: ${role} is missing label FGA grants"
    echo "Output was:"
    echo "$OUTPUT"
    FAILED=1
  fi

  if ! echo "$OUTPUT" | grep -q "$EXPECTED_EDGE"; then
    echo "FAIL: ${role} is missing edge type FGA grants"
    echo "Output was:"
    echo "$OUTPUT"
    FAILED=1
  fi
done

if [ "$FAILED" -eq 1 ]; then
  echo "FGA verification FAILED"
  exit 1
fi

echo "FGA verification PASSED"
