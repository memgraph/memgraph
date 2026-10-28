#!/bin/bash
# =============================================================================
# Smoke test for Memgraph v3.12.0 new features.
#
# Spins up a Memgraph (MAGE) Docker container (and, for HA features, a separate
# coordinator container) and exercises the new v3.12.0 features documented in:
#   https://github.com/memgraph/documentation/pull/1667  ("Memgraph v3.12.0")
# via a LOCALLY INSTALLED mgconsole.
#
# 10 test cases (feature -> Memgraph PR):
#   1. Lightweight edges                       (--storage-light-edge, #4228)   [Community]
#   2. replace() empty-search fix              (#4269)                          [Community]
#   3. text_search.fuzzy_phrase_search         (#4304)                          [Community]
#   4. text_search fuzzy_prefix option         (#4304)                          [Community]
#   5. SHOW STORAGE INFO state/health fields   (#4284)                          [Community]
#   6. WAL CRC32 checksum recovery (restart)   (#4225)                          [Community]
#   7. SHOW DATABASES State/Health columns     (#4284)                          [Enterprise]
#   8. SUSPEND / RESUME DATABASE (HOT/COLD)    (#4280)                          [Enterprise]
#   9. Coordinator global_read_only setting    (#4330)                          [Enterprise/HA]
#  10. Property-based access control (PBAC)    (#4156)                          [Enterprise]
#
# Usage:
#   MEMGRAPH_ENTERPRISE_LICENSE=... MEMGRAPH_ORGANIZATION_NAME=... \
#     ./test_release_features_3_12.bash
#
# Without a license, the 3 Enterprise/HA test cases are skipped and the 7
# Community cases still run.
# =============================================================================
set -uo pipefail

# ------------------------------- Configuration -------------------------------
IMAGE="${MEMGRAPH_IMAGE:-memgraph/memgraph-mage:3.12.0}"
MGCONSOLE="${MGCONSOLE:-mgconsole}"
HOST="${MEMGRAPH_HOST:-localhost}"

DATA_BOLT="${DATA_BOLT:-7698}"          # host port -> data instance bolt
COORD_BOLT="${COORD_BOLT:-7699}"        # host port -> coordinator bolt

DATA_CONTAINER="mg_smoke_3_12_data"
COORD_CONTAINER="mg_smoke_3_12_coord"
DATA_VOLUME="mg_smoke_3_12_vol"

ENTERPRISE_LICENSE="${MEMGRAPH_ENTERPRISE_LICENSE:-}"
ORGANIZATION_NAME="${MEMGRAPH_ORGANIZATION_NAME:-}"
ENTERPRISE="false"
if [ -n "$ENTERPRISE_LICENSE" ] && [ -n "$ORGANIZATION_NAME" ]; then
  ENTERPRISE="true"
fi
ENT_ENVS=()
if [ "$ENTERPRISE" = "true" ]; then
  ENT_ENVS=(-e "MEMGRAPH_ENTERPRISE_LICENSE=$ENTERPRISE_LICENSE" -e "MEMGRAPH_ORGANIZATION_NAME=$ORGANIZATION_NAME")
fi

# ---------------------------- Bookkeeping / output ---------------------------
TOTAL=0
PASSED=0
SKIPPED=0
declare -a FAILED_TESTS=()
declare -a SKIPPED_TESTS=()
fails=0   # reset per test by run_test; mutated by check_* helpers

RED=$'\033[0;31m'; GRN=$'\033[0;32m'; YLW=$'\033[0;33m'; BLU=$'\033[0;34m'; NC=$'\033[0m'

section() { echo ""; echo "${BLU}== $* ==${NC}"; }
note()    { echo "    $*"; }

check_contains() {   # <output> <needle> [label]
  local out="$1" needle="$2" label="${3:-$2}"
  if printf '%s' "$out" | grep -qiF -- "$needle"; then
    echo "    ${GRN}✓${NC} found: $label"
  else
    echo "    ${RED}✗ MISSING${NC}: $label  (expected substring: '$needle')"
    fails=$((fails + 1))
  fi
}

check_not_contains() {   # <output> <needle> [label]
  local out="$1" needle="$2" label="${3:-$2}"
  if printf '%s' "$out" | grep -qiF -- "$needle"; then
    echo "    ${RED}✗ UNEXPECTED${NC}: $label  (found forbidden substring: '$needle')"
    fails=$((fails + 1))
  else
    echo "    ${GRN}✓${NC} absent (as expected): $label"
  fi
}

run_test() {   # <name> <function>
  local name="$1" fn="$2"
  TOTAL=$((TOTAL + 1))
  echo ""
  echo "────────────────────────────────────────────────────────────────────"
  echo "TEST $TOTAL: $name"
  echo "────────────────────────────────────────────────────────────────────"
  fails=0
  "$fn"
  if [ "$fails" -eq 0 ]; then
    echo "  RESULT: ${GRN}✅ PASS${NC} — $name"
    PASSED=$((PASSED + 1))
  else
    echo "  RESULT: ${RED}❌ FAIL${NC} ($fails failed check(s)) — $name"
    FAILED_TESTS+=("$name")
  fi
}

skip_test() {  # <name> <reason>
  TOTAL=$((TOTAL + 1))
  SKIPPED=$((SKIPPED + 1))
  SKIPPED_TESTS+=("$1")
  echo ""
  echo "────────────────────────────────────────────────────────────────────"
  echo "TEST $TOTAL: $1"
  echo "  RESULT: ${YLW}⏭  SKIP${NC} — $2"
}

# ------------------------------ mgconsole helpers ----------------------------
# All run against the locally installed mgconsole binary.
mgq()       { printf '%s\n' "$1" | "$MGCONSOLE" --host "$HOST" --port "$DATA_BOLT" 2>&1; }
mgq_admin() { printf '%s\n' "$1" | "$MGCONSOLE" --host "$HOST" --port "$DATA_BOLT" --username admin --password admin 2>&1; }
mgq_user()  { printf '%s\n' "$2" | "$MGCONSOLE" --host "$HOST" --port "$DATA_BOLT" --username "$1" --password "$1" 2>&1; }
mgc()       { printf '%s\n' "$1" | "$MGCONSOLE" --host "$HOST" --port "$COORD_BOLT" 2>&1; }

wait_for() {   # <port> <probe-query> [max-retries]
  local port="$1" probe="$2" max="${3:-400}" i=0
  while ! printf '%s\n' "$probe" | "$MGCONSOLE" --host "$HOST" --port "$port" >/dev/null 2>&1; do
    sleep 0.3
    i=$((i + 1))
    if [ "$i" -ge "$max" ]; then
      echo "${RED}wait_for: $HOST:$port did not become ready after $max tries${NC}" >&2
      return 1
    fi
  done
  return 0
}

# ------------------------------- Cleanup / trap ------------------------------
cleanup() {
  local rc=$?
  echo ""
  echo "Cleaning up containers and volume..."
  docker rm -f "$DATA_CONTAINER"  >/dev/null 2>&1 || true
  docker rm -f "$COORD_CONTAINER" >/dev/null 2>&1 || true
  docker volume rm "$DATA_VOLUME" >/dev/null 2>&1 || true
  exit "$rc"
}
trap cleanup EXIT INT TERM

# --------------------------- Preflight sanity checks -------------------------
command -v "$MGCONSOLE" >/dev/null 2>&1 || { echo "${RED}ERROR: mgconsole not found on PATH ('$MGCONSOLE').${NC}"; exit 1; }
command -v docker       >/dev/null 2>&1 || { echo "${RED}ERROR: docker not found on PATH.${NC}"; exit 1; }

echo "Image .............. $IMAGE"
echo "mgconsole .......... $("$MGCONSOLE" --version 2>&1 | head -1) ($MGCONSOLE)"
echo "Data bolt port ..... $DATA_BOLT"
echo "Coordinator port ... $COORD_BOLT"
echo "Enterprise ......... $ENTERPRISE"

# ------------------------- Start the data instance --------------------------
section "Starting Memgraph data instance"
docker rm -f "$DATA_CONTAINER" >/dev/null 2>&1 || true
docker volume rm "$DATA_VOLUME" >/dev/null 2>&1 || true

# Flags exercise: lightweight edges (implies properties-on-edges), edge metadata
# for id() lookups, durable WAL + periodic snapshots (needed by SUSPEND and by
# the WAL CRC32 recovery test), and broken-state recovery handling.
docker run -d --rm \
  --name "$DATA_CONTAINER" \
  -p "$DATA_BOLT:7687" \
  -v "$DATA_VOLUME:/var/lib/memgraph" \
  ${ENT_ENVS[@]+"${ENT_ENVS[@]}"} \
  "$IMAGE" \
  --telemetry-enabled=false --log-level=TRACE --also-log-to-stderr \
  --storage-light-edge=true \
  --storage-properties-on-edges=true \
  --storage-enable-edges-metadata=true \
  --storage-wal-enabled=true \
  --storage-snapshot-interval-sec=300 \
  --storage-snapshot-on-exit=true \
  --data-recovery-on-startup=true \
  --storage-allow-recovery-failure=true \
  >/dev/null || { echo "${RED}ERROR: failed to start data container.${NC}"; exit 1; }

echo "Waiting for data instance to accept queries (MAGE module load can take ~30-60s)..."
wait_for "$DATA_BOLT" "RETURN 1;" 400 || { echo "${RED}ERROR: data instance never came up. Logs:${NC}"; docker logs "$DATA_CONTAINER" 2>&1 | tail -30; exit 1; }
echo "${GRN}Data instance is up.${NC}"

# =============================================================================
# TEST 1: Lightweight edges (--storage-light-edge). Community.
# Edges are pool-allocated (no per-edge skip-list container). Traversals and
# id()-based edge lookups must still work; edge properties must be stored.
# =============================================================================
test_lightweight_edges() {
  note "Creating a small graph with edge properties (light-edge mode)."
  mgq "CREATE (a:City {name:'A'})-[:ROAD {km:10}]->(b:City {name:'B'})-[:ROAD {km:20}]->(c:City {name:'C'});" >/dev/null

  local trav
  trav="$(mgq "MATCH (:City {name:'A'})-[r:ROAD*2]->(x:City) RETURN x.name AS name, reduce(s=0, e IN r | s + e.km) AS total;")"
  note "Traversal over light edges:"; printf '%s\n' "$trav" | sed 's/^/      /'
  check_contains "$trav" '"C"' "2-hop traversal reaches City C"
  check_contains "$trav" '30'  "edge property (km) sum = 30 (edge props stored in light-edge mode)"

  # id()-based edge lookup: only fast with edges-metadata, must still be correct.
  local byid
  byid="$(mgq "MATCH ()-[r:ROAD]->() WITH r ORDER BY r.km LIMIT 1 WITH id(r) AS eid MATCH ()-[e:ROAD]->() WHERE id(e)=eid RETURN e.km AS km;")"
  note "Edge lookup by id():"; printf '%s\n' "$byid" | sed 's/^/      /'
  check_contains "$byid" '10' "id()-based edge lookup resolves the edge (km=10)"
}

# =============================================================================
# TEST 2: replace() empty-search fix (#4269). Community.
# replace("abc", "", "-") must return "-a-b-c-" instead of looping / OOM.
# =============================================================================
test_replace_empty() {
  local out
  out="$(mgq "RETURN replace('abc', '', '-') AS r;")"
  note "replace('abc','','-'):"; printf '%s\n' "$out" | sed 's/^/      /'
  check_contains "$out" '-a-b-c-' "empty-search replace returns '-a-b-c-'"
}

# =============================================================================
# TEST 3: text_search.fuzzy_phrase_search (#4304). Community.
# Ordered, adjacent phrase with prefix on the last term.
# =============================================================================
test_fuzzy_phrase_search() {
  mgq "CREATE TEXT INDEX docIndex ON :Doc;" >/dev/null
  mgq "CREATE (:Doc {title:'big bad wolf', n:1});
       CREATE (:Doc {title:'big bad world', n:2});
       CREATE (:Doc {title:'the big bad wolf returns', n:3});
       CREATE (:Doc {title:'bad big wolf', n:4});
       CREATE (:Doc {title:'big wolf', n:5});" >/dev/null

  local out
  out="$(mgq "CALL text_search.fuzzy_phrase_search('docIndex', 'data.title:big bad wo')
              YIELD node RETURN node.title AS title, node.n AS n ORDER BY n ASC;")"
  note "fuzzy_phrase_search('data.title:big bad wo'):"; printf '%s\n' "$out" | sed 's/^/      /'
  check_contains     "$out" 'big bad wolf'             "matches 'big bad wolf' (n=1)"
  check_contains     "$out" 'big bad world'            "matches 'big bad world' (n=2)"
  check_contains     "$out" 'the big bad wolf returns' "matches 'the big bad wolf returns' (n=3)"
  check_not_contains "$out" 'bad big wolf'             "excludes 'bad big wolf' (wrong order)"
  check_not_contains "$out" 'big wolf"'                "excludes 'big wolf' (not adjacent)"

  # Typo tolerance: 'bd' -> 'bad' is one edit, still matches 'big bad wolf'.
  local fuzzy
  fuzzy="$(mgq "CALL text_search.fuzzy_phrase_search('docIndex', 'data.title:big bd wo', {fuzzy_distance:1})
                YIELD node RETURN node.title AS title;")"
  note "fuzzy_phrase_search('data.title:big bd wo', fuzzy_distance:1):"; printf '%s\n' "$fuzzy" | sed 's/^/      /'
  check_contains "$fuzzy" 'big bad wolf' "one-edit typo 'bd'->'bad' still matches 'big bad wolf'"
}

# =============================================================================
# TEST 4: text_search fuzzy_prefix option (#4304). Community.
# Each term is an independent fuzzy prefix; order/adjacency ignored -> both
# 'lucky lucy' and reversed 'lucy lucky' match.
# =============================================================================
test_fuzzy_prefix() {
  mgq "CREATE TEXT INDEX nameIndex ON :Person;" >/dev/null
  mgq "CREATE (:Person {name:'lucky luke'});
       CREATE (:Person {name:'lucky lucy'});
       CREATE (:Person {name:'lucy lucky'});
       CREATE (:Person {name:'unlucky lu'});" >/dev/null

  local out
  out="$(mgq "CALL text_search.search('nameIndex', 'data.name:lucky data.name:lu', {fuzzy_distance:1, fuzzy_prefix:true})
              YIELD node RETURN node.name AS name ORDER BY name ASC;")"
  note "search(..., fuzzy_prefix:true) for 'lucky' + 'lu':"; printf '%s\n' "$out" | sed 's/^/      /'
  check_contains "$out" 'lucky luke' "fuzzy prefix matches 'lucky luke'"
  check_contains "$out" 'lucy lucky' "fuzzy prefix matches reversed 'lucy lucky' (order ignored)"
}

# =============================================================================
# TEST 5: SHOW STORAGE INFO exposes 'state' (HOT/COLD) and 'health'
# (ready/broken) fields (#4284). Community.
# =============================================================================
test_storage_info_health() {
  local out
  out="$(mgq "SHOW STORAGE INFO ON CURRENT DATABASE;")"
  note "SHOW STORAGE INFO ON CURRENT DATABASE (state/health rows):"
  printf '%s\n' "$out" | grep -iE "state|health" | sed 's/^/      /'
  check_contains "$out" 'state'  "SHOW STORAGE INFO has a 'state' field"
  check_contains "$out" 'HOT'    "default database state is HOT"
  check_contains "$out" 'health' "SHOW STORAGE INFO has a 'health' field"
  check_contains "$out" 'ready'  "default database health is ready"
}

# =============================================================================
# TEST 6: SHOW DATABASES carries Name/State/Health columns (#4284). Community.
# =============================================================================
test_show_databases_columns() {
  local out
  out="$(mgq "SHOW DATABASES;")"
  note "SHOW DATABASES:"; printf '%s\n' "$out" | sed 's/^/      /'
  check_contains "$out" 'State'    "SHOW DATABASES has a State column"
  check_contains "$out" 'Health'   "SHOW DATABASES has a Health column"
  check_contains "$out" 'memgraph' "default 'memgraph' database is listed"
  check_contains "$out" 'HOT'      "default database reported HOT"
  check_contains "$out" 'ready'    "default database reported ready"
}

# =============================================================================
# TEST 7: WAL CRC32 checksum recovery (#4225). Community.
# Write data with WAL enabled, restart the container, and confirm the data is
# recovered — exercising the new per-transaction CRC32 verification path.
# =============================================================================
test_wal_recovery() {
  note "Writing a marker node and forcing a snapshot + WAL flush."
  mgq "CREATE (:WalMarker {tag:'crc32-survivor', v:42});" >/dev/null
  mgq "CREATE SNAPSHOT;" >/dev/null 2>&1 || true

  note "Restarting the container (triggers durability recovery)..."
  docker restart "$DATA_CONTAINER" >/dev/null
  wait_for "$DATA_BOLT" "RETURN 1;" || { echo "    ${RED}✗ instance did not come back after restart${NC}"; fails=$((fails+1)); return; }

  local out
  out="$(mgq "MATCH (n:WalMarker) RETURN n.tag AS tag, n.v AS v;")"
  note "After restart:"; printf '%s\n' "$out" | sed 's/^/      /'
  check_contains "$out" 'crc32-survivor' "marker node recovered from WAL/snapshot after restart"
  check_contains "$out" '42'             "marker property value intact"

  # A healthy recovery reports the database as ready (not broken).
  local health
  health="$(mgq "SHOW STORAGE INFO ON CURRENT DATABASE;")"
  check_contains "$health" 'ready' "database healthy (ready) after CRC32-verified recovery"
}

# =============================================================================
# TEST 8: SUSPEND / RESUME DATABASE — HOT/COLD tenants (#4280). Enterprise.
# =============================================================================
test_suspend_resume() {
  note "Creating tenant 'shopdb' with data."
  mgq "CREATE DATABASE shopdb;" >/dev/null 2>&1 || true
  mgq "USE DATABASE shopdb; CREATE (:Product {sku:'X1'}); CREATE (:Product {sku:'X2'});" >/dev/null

  # SUSPEND may transiently see the tenant as busy; retry a few times.
  note "Suspending 'shopdb' (HOT -> COLD)."
  local susp="" i=0
  while [ "$i" -lt 10 ]; do
    susp="$(mgq "SUSPEND DATABASE shopdb;")"
    printf '%s' "$susp" | grep -qi "active connections" || break
    sleep 0.5; i=$((i + 1))
  done
  printf '%s\n' "$susp" | sed 's/^/      /'
  check_contains "$susp" 'suspended' "SUSPEND DATABASE reports success"

  local cold
  cold="$(mgq "SHOW DATABASES;")"
  note "SHOW DATABASES after suspend:"; printf '%s\n' "$cold" | sed 's/^/      /'
  check_contains "$cold" 'COLD' "shopdb is COLD after suspend"

  note "Resuming 'shopdb' (COLD -> HOT)."
  local res="" j=0
  while [ "$j" -lt 10 ]; do
    res="$(mgq "RESUME DATABASE shopdb;")"
    printf '%s' "$res" | grep -qiE "failed to recover|retried" || break
    sleep 0.5; j=$((j + 1))
  done
  printf '%s\n' "$res" | sed 's/^/      /'
  check_contains "$res" 'resumed' "RESUME DATABASE reports success"

  local cnt
  cnt="$(mgq "USE DATABASE shopdb; MATCH (p:Product) RETURN count(p) AS c;")"
  note "Data after resume:"; printf '%s\n' "$cnt" | sed 's/^/      /'
  check_contains "$cnt" '2' "both products intact after suspend/resume cycle"
}

# =============================================================================
# TEST 9: Property-based access control (PBAC) (#4156). Enterprise.
# NOTE: This creates users, which disables anonymous access, so it MUST run
# after every test that assumes no users exist.
# =============================================================================
test_pbac() {
  note "Bootstrapping admin user + Employee data."
  # First connection (no users yet) creates the admin.
  mgq "CREATE USER admin IDENTIFIED BY 'admin';" >/dev/null
  mgq_admin "GRANT ALL PRIVILEGES TO admin; GRANT DATABASE * TO admin;
             GRANT READ, SET PROPERTY {*} ON NODES CONTAINING LABELS * TO admin;
             GRANT READ, SET PROPERTY {*} ON EDGES OF TYPE * TO admin;" >/dev/null
  mgq_admin "CREATE (:Employee {name:'Alice', ssn:'1234', salary:9999});" >/dev/null

  note "Creating restricted 'analyst': global READ {*} but DENY READ {salary} on :Employee."
  mgq_admin "CREATE USER analyst IDENTIFIED BY 'analyst';
             GRANT MATCH TO analyst;
             GRANT DATABASE memgraph TO analyst;
             GRANT READ ON NODES CONTAINING LABELS * TO analyst;
             GRANT READ {*} ON NODES CONTAINING LABELS * TO analyst;
             DENY READ {salary} ON NODES CONTAINING LABELS :Employee TO analyst;" >/dev/null

  local vals
  vals="$(mgq_user analyst "MATCH (e:Employee) RETURN e.name AS name, e.ssn AS ssn, e.salary AS salary;")"
  note "analyst sees (name/ssn granted, salary denied):"; printf '%s\n' "$vals" | sed 's/^/      /'
  check_contains "$vals" 'Alice' "granted property 'name' is visible"
  check_contains "$vals" '1234'  "globally granted property 'ssn' is visible"
  check_contains "$vals" 'Null'  "denied property 'salary' reads as Null"

  # keys() must omit the denied property entirely.
  local keys
  keys="$(mgq_user analyst "MATCH (e:Employee) RETURN keys(e) AS k;")"
  note "analyst keys(e):"; printf '%s\n' "$keys" | sed 's/^/      /'
  check_contains     "$keys" 'name'   "keys() includes granted 'name'"
  check_not_contains "$keys" 'salary' "keys() omits denied 'salary'"
}

# =============================================================================
# TEST 10: Coordinator global_read_only runtime setting (#4330). Enterprise/HA.
# Runs a dedicated coordinator container and toggles the Raft-replicated setting.
# =============================================================================
test_coordinator_global_read_only() {
  note "Starting a coordinator instance..."
  docker rm -f "$COORD_CONTAINER" >/dev/null 2>&1 || true
  docker run -d --rm \
    --name "$COORD_CONTAINER" \
    -p "$COORD_BOLT:7687" \
    ${ENT_ENVS[@]+"${ENT_ENVS[@]}"} \
    "$IMAGE" \
    --telemetry-enabled=false --log-level=TRACE --also-log-to-stderr \
    --bolt-port=7687 \
    --coordinator-id=1 \
    --coordinator-port=10111 \
    --management-port=10121 \
    --coordinator-hostname=localhost \
    >/dev/null || { echo "    ${RED}✗ failed to start coordinator container${NC}"; fails=$((fails+1)); return; }

  if ! wait_for "$COORD_BOLT" "SHOW COORDINATOR SETTINGS;" 400; then
    echo "    ${RED}✗ coordinator never became ready. Logs:${NC}"
    docker logs "$COORD_CONTAINER" 2>&1 | tail -20 | sed 's/^/      /'
    fails=$((fails + 1)); return
  fi

  local settings
  settings="$(mgc "SHOW COORDINATOR SETTINGS;")"
  note "SHOW COORDINATOR SETTINGS:"; printf '%s\n' "$settings" | sed 's/^/      /'
  check_contains "$settings" 'global_read_only' "coordinator exposes 'global_read_only' setting"

  mgc "SET COORDINATOR SETTING 'global_read_only' TO 'true';" >/dev/null
  local after
  after="$(mgc "SHOW COORDINATOR SETTINGS;")"
  note "After SET global_read_only -> true:"
  printf '%s\n' "$after" | grep -i 'global_read_only' | sed 's/^/      /'
  check_contains "$after" 'true' "global_read_only reads 'true' after being set"

  # Restore, and prove another runtime setting is also writable.
  mgc "SET COORDINATOR SETTING 'global_read_only' TO 'false';" >/dev/null
  local lag
  lag="$(mgc "SET COORDINATOR SETTING 'max_replica_read_lag' TO '10'; SHOW COORDINATOR SETTINGS;")"
  check_contains "$lag" 'max_replica_read_lag' "max_replica_read_lag setting is present and writable"
}

# ================================= Run tests =================================
# Community-tier features first (they assume no users / no extra tenants).
run_test "Lightweight edges (--storage-light-edge, #4228)"        test_lightweight_edges
run_test "replace() empty-search fix (#4269)"                      test_replace_empty
run_test "text_search.fuzzy_phrase_search (#4304)"                 test_fuzzy_phrase_search
run_test "text_search fuzzy_prefix option (#4304)"                 test_fuzzy_prefix
run_test "SHOW STORAGE INFO state/health fields (#4284)"           test_storage_info_health
run_test "WAL CRC32 checksum recovery on restart (#4225)"          test_wal_recovery

# Enterprise-tier features (SHOW DATABASES and multi-tenancy need a license).
if [ "$ENTERPRISE" = "true" ]; then
  run_test "SHOW DATABASES State/Health columns (#4284)"          test_show_databases_columns
  run_test "SUSPEND/RESUME DATABASE HOT/COLD (#4280)"              test_suspend_resume
  run_test "Coordinator global_read_only setting (#4330)"         test_coordinator_global_read_only
  # PBAC creates users (disables anonymous access) -> keep it LAST.
  run_test "Property-based access control / PBAC (#4156)"          test_pbac
else
  skip_test "SHOW DATABASES State/Health columns (#4284)"        "no enterprise license (set MEMGRAPH_ENTERPRISE_LICENSE + MEMGRAPH_ORGANIZATION_NAME)"
  skip_test "SUSPEND/RESUME DATABASE HOT/COLD (#4280)"            "no enterprise license"
  skip_test "Coordinator global_read_only setting (#4330)"       "no enterprise license"
  skip_test "Property-based access control / PBAC (#4156)"        "no enterprise license"
fi

# ================================== Summary ==================================
echo ""
echo "════════════════════════════════════════════════════════════════════"
echo " SUMMARY"
echo "════════════════════════════════════════════════════════════════════"
echo "  Total:   $TOTAL"
echo "  ${GRN}Passed:  $PASSED${NC}"
echo "  ${YLW}Skipped: $SKIPPED${NC}"
echo "  ${RED}Failed:  ${#FAILED_TESTS[@]}${NC}"
if [ "${#SKIPPED_TESTS[@]}" -gt 0 ]; then
  echo "  Skipped tests:"
  for t in "${SKIPPED_TESTS[@]}"; do echo "    - $t"; done
fi
if [ "${#FAILED_TESTS[@]}" -gt 0 ]; then
  echo "  Failed tests:"
  for t in "${FAILED_TESTS[@]}"; do echo "    - $t"; done
  echo ""
  echo "${RED}RELEASE SMOKE TEST FAILED${NC}"
  exit 1
fi
echo ""
echo "${GRN}RELEASE SMOKE TEST PASSED${NC}"
exit 0
