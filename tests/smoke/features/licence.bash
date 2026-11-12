#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/../utils.bash"

# Two containers from the same image with the same flags, one with the license
# and one without. Every probe must be on in the first and off in the second.
LICENCE_PROBE_FLAGS="--audit-enabled=true --auth-password-strength-regex=.{8,}"
LICENCE_PROBE_PASSWORD="probe_password"

# Creating a user turns on authentication for every later query, so it runs last.
LICENCE_PROBE_QUERIES=(
  "CREATE DATABASE probe_db;"
  "CREATE ROLE probe_role;"
  "GRANT READ ON NODES CONTAINING LABELS :Probe TO probe_role;"
  "GRANT IMPERSONATE_USER * TO probe_role;"
  "CREATE PROFILE probe_profile LIMIT SESSIONS 1;"
  "CREATE TENANT PROFILE probe_tenant_profile LIMIT memory_limit 100MB;"
  "ENABLE TTL EVERY '1d' AT '00:00:00';"
  "USING PARALLEL EXECUTION MATCH (n) RETURN count(n);"
  "SHOW METRICS INFO;"
  "SHOW MEMORY INFO;"
  "SET DATABASE SETTING 'storage.snapshot.interval' TO '0 0 * * * *';"
  "CREATE USER probe_user IDENTIFIED BY '$LICENCE_PROBE_PASSWORD';"
)

# Returns 0 on success, 1 if refused for lack of a license, 2 on any other error.
licence_probe_query() {
  local out
  out="$(echo "$2" | $MEMGRAPH_CONSOLE_BINARY --host "$MEMGRAPH_DEFAULT_HOST" --port "$1" "${@:3}" 2>&1)" && { echo "$out"; return 0; }
  grep -qi license <<<"$out" && return 1
  echo "$out" >&2
  return 2
}

licence_probe_module_loaded() {
  licence_probe_query "$1" "CALL mg.procedures() YIELD name WHERE name = 'pagerank_online.set' RETURN name;" \
    --output-format=csv | grep -q pagerank_online.set
}

licence_probe_metrics_served() {
  local out
  out="$(curl -s -w '\n%{http_code}' "http://localhost:$1/metrics")"
  [[ "${out##*$'\n'}" == 200 ]] && return 0
  grep -qi license <<<"$out" && return 1
  echo "$out" >&2
  return 2
}

licence_probe_audit_written() {
  docker exec "$1" test -s /var/lib/memgraph/audit/audit.log
}

licence_probe_password_masked() {
  ! docker logs "$1" 2>&1 | grep -q "$LICENCE_PROBE_PASSWORD"
}

# $1: expected state (on|off), $2: feature, the rest: the probe.
licence_expect() {
  local expected="$1" feature="$2" actual
  shift 2
  "$@" >/dev/null
  case $? in
    0) actual=on ;;
    1) actual=off ;;
    *) echo "FAIL: $feature failed for a reason other than the license"; return 1 ;;
  esac
  [[ "$actual" == "$expected" ]] && return 0
  echo "FAIL: $feature is $actual, expected $expected"
  return 1
}

# $1: container, $2: Bolt port, $3: metrics port, $4: expected state (on|off),
# $5: image type, the rest: extra docker run arguments.
licence_probe_container() {
  local name="$1" bolt="$2" metrics="$3" expected="$4" image_type="$5" failed=0 query
  shift 5
  docker_stop_if_there "$name" >/dev/null 2>&1 || true
  docker run -d --rm -p "$bolt:7687" -p "$metrics:9091" --name "$name" "$@" \
    "$MEMGRAPH_DOCKERHUB_IMAGE" $MEMGRAPH_GENERAL_FLAGS $LICENCE_PROBE_FLAGS >/dev/null \
    || { echo "FAIL: could not start $name"; return 1; }
  wait_for_memgraph "$MEMGRAPH_DEFAULT_HOST" "$bolt" || { docker_stop_if_there "$name" >/dev/null 2>&1; return 1; }

  licence_expect "$expected" "HTTP metrics" licence_probe_metrics_served "$metrics" || failed=1
  if [[ "$image_type" == "mage" ]]; then
    licence_expect "$expected" "enterprise query modules" licence_probe_module_loaded "$bolt" || failed=1
  fi
  for query in "${LICENCE_PROBE_QUERIES[@]}"; do
    licence_expect "$expected" "'$query'" licence_probe_query "$bolt" "$query" || failed=1
  done
  # The audit log flushes every 200ms by default.
  sleep 1
  licence_expect "$expected" "audit log" licence_probe_audit_written "$name" || failed=1
  licence_expect "$expected" "query-log password masking" licence_probe_password_masked "$name" || failed=1

  docker_stop_if_there "$name" >/dev/null 2>&1 || true
  return $failed
}

# $1: image type (memgraph|mage)
test_enterprise_features_need_licence() {
  echo "FEATURE: Enterprise features need a license"
  local failed=0
  licence_probe_container memgraph_smoke_unlicensed 8005 9003 off "$1" || failed=1
  licence_probe_container memgraph_smoke_licensed 8006 9004 on "$1" $MEMGRAPH_ENTERPRISE_DOCKER_ENVS || failed=1
  return $failed
}
