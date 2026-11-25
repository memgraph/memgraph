#!/bin/bash
# =============================================================================
# Smoke test for Memgraph v3.13.0 new features.
#
# Spins up a Memgraph (MAGE) Docker container (and, for HA features, a separate
# coordinator container) and exercises the new v3.13.0 features documented in:
#   https://github.com/memgraph/documentation/pull/1689      ("Add Memgraph v3.13.0")
#   https://github.com/memgraph/documentation/milestone/19    (3.13 docs milestone)
# via a LOCALLY INSTALLED mgconsole.
#
# 20 test cases (feature -> Memgraph PR):
#   1. Global vertex-property index + CREATE RANGE INDEX FOR   (#4353, #4486)         [Community]
#   2. COUNT {} / COLLECT {} / EXISTS {} in projections, CASE  (#4598, #4632, #4504, #4596) [Community]
#   3. Unicode-aware size()/substring()/left()/right()/reverse() (#4585, #4586)      [Community]
#   4. Cypher semantics: octal literals, toInteger(), single(), split(), list comp (#4588-#4591, #4665) [Community]
#   5. *KSHORTEST filter lambda + per-row |k                    (#4559)                [Community]
#   6. TERMINATE TRANSACTIONS "*" + strict id parsing          (#4534)                [Community]
#   7. Property-value descriptions + description()             (#4526)                [Community]
#   8. MAGE collections / map / text additions                 (#4415, #4417, #4435)  [Community]
#   9. MAGE convert JSON functions + apoc alias remap          (#4443)                [Community]
#  10. MAGE search.node / search.node_all                      (#4460)                [Community]
#  11. MAGE path.expand_config                                 (#4530)                [Community]
#  12. Vector-index property omission runtime setting          (#4556)                [Community]
#  13. New configuration flags + OpenMetrics default           (#4334, #4546, #4579, #4678) [Community]
#  14. WAL header-based recovery on restart                    (#4528)                [Community]
#  15. cross_database module gated by Enterprise license       (#4726)                [Community/Enterprise]
#  16. Metrics HTTP endpoint serves OpenMetrics by default     (#4678)                [Enterprise]
#  17. Coordinator: --coordinator-id=0, SHOW ROUTING TABLE, SHOW VERSION (#4483, #4502, #4535) [Enterprise/HA]
#  18. Coordinator SSO roles + COORDINATOR_READ/WRITE          (#4399)                [Enterprise/HA]
#  19. Text search respects fine-grained label permissions     (#4316)                [Enterprise]
#  20. COORDINATOR privilege removed from data instances       (#4399)                [Enterprise]
#
# Usage:
#   MEMGRAPH_ENTERPRISE_LICENSE=... MEMGRAPH_ORGANIZATION_NAME=... \
#     ./test_release_features_3_13.bash
#
# Without a license, the 5 Enterprise/HA test cases are skipped and the 15
# Community cases still run (test 15 adapts its expectation to the tier).
# =============================================================================
set -uo pipefail

# ------------------------------- Configuration -------------------------------
IMAGE="${MEMGRAPH_IMAGE:-memgraph/memgraph-mage:3.13.0}"
MGCONSOLE="${MGCONSOLE:-mgconsole}"
HOST="${MEMGRAPH_HOST:-localhost}"

DATA_BOLT="${DATA_BOLT:-7698}"          # host port -> data instance bolt
COORD_BOLT="${COORD_BOLT:-7699}"        # host port -> coordinator bolt
METRICS_PORT="${METRICS_PORT:-9791}"    # host port -> data instance metrics HTTP server

DATA_CONTAINER="mg_smoke_3_13_data"
COORD_CONTAINER="mg_smoke_3_13_coord"
DATA_VOLUME="mg_smoke_3_13_vol"

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
show()    { printf '%s\n' "$1" | sed 's/^/      /'; }

# NOTE: the helpers feed grep via a here-string, not "printf | grep". With
# `set -o pipefail`, grep -q exiting on the first match makes printf fail with
# SIGPIPE on large inputs (e.g. the metrics body), which would flip the result.
check_contains() {   # <output> <needle> [label]
  local out="$1" needle="$2" label="${3:-$2}"
  if grep -qiF -- "$needle" <<< "$out"; then
    echo "    ${GRN}✓${NC} found: $label"
  else
    echo "    ${RED}✗ MISSING${NC}: $label  (expected substring: '$needle')"
    fails=$((fails + 1))
  fi
}

check_not_contains() {   # <output> <needle> [label]
  local out="$1" needle="$2" label="${3:-$2}"
  if grep -qiF -- "$needle" <<< "$out"; then
    echo "    ${RED}✗ UNEXPECTED${NC}: $label  (found forbidden substring: '$needle')"
    fails=$((fails + 1))
  else
    echo "    ${GRN}✓${NC} absent (as expected): $label"
  fi
}

# mgconsole reports server-side failures as "Client received query exception: ..."
check_error() {   # <output> <label>
  local out="$1" label="$2"
  if grep -qiE "exception|error" <<< "$out"; then
    echo "    ${GRN}✓${NC} rejected: $label"
  else
    echo "    ${RED}✗ NOT REJECTED${NC}: $label  (expected a query error)"
    fails=$((fails + 1))
  fi
}

check_no_error() {   # <output> <label>
  local out="$1" label="$2"
  if grep -qiE "exception|error" <<< "$out"; then
    echo "    ${RED}✗ ERRORED${NC}: $label"
    fails=$((fails + 1))
  else
    echo "    ${GRN}✓${NC} succeeded: $label"
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
command -v curl         >/dev/null 2>&1 || { echo "${RED}ERROR: curl not found on PATH.${NC}"; exit 1; }

echo "Image .............. $IMAGE"
echo "mgconsole .......... $("$MGCONSOLE" --version 2>&1 | head -1) ($MGCONSOLE)"
echo "Data bolt port ..... $DATA_BOLT"
echo "Coordinator port ... $COORD_BOLT"
echo "Metrics port ....... $METRICS_PORT"
echo "Enterprise ......... $ENTERPRISE"

# ------------------------- Start the data instance --------------------------
section "Starting Memgraph data instance"
docker rm -f "$DATA_CONTAINER" >/dev/null 2>&1 || true
docker volume rm "$DATA_VOLUME" >/dev/null 2>&1 || true

# Flags exercise: durable WAL + snapshots (needed by the WAL-header recovery
# test), broken-state recovery handling, and the 3.13 vector-index property
# omission flag left at its default so the runtime setting toggle is observable.
# --metrics-format is deliberately NOT set so the new OpenMetrics default is
# what gets tested.
docker run -d --rm \
  --name "$DATA_CONTAINER" \
  -p "$DATA_BOLT:7687" \
  -p "$METRICS_PORT:9091" \
  -v "$DATA_VOLUME:/var/lib/memgraph" \
  ${ENT_ENVS[@]+"${ENT_ENVS[@]}"} \
  "$IMAGE" \
  --telemetry-enabled=false --log-level=TRACE --also-log-to-stderr \
  --storage-properties-on-edges=true \
  --storage-wal-enabled=true \
  --storage-snapshot-interval-sec=300 \
  --storage-snapshot-on-exit=true \
  --data-recovery-on-startup=true \
  --storage-allow-recovery-failure=true \
  --storage-omit-vector-index-properties-on-return=false \
  >/dev/null || { echo "${RED}ERROR: failed to start data container.${NC}"; exit 1; }

echo "Waiting for data instance to accept queries (MAGE module load can take ~30-60s)..."
wait_for "$DATA_BOLT" "RETURN 1;" 400 || { echo "${RED}ERROR: data instance never came up. Logs:${NC}"; docker logs "$DATA_CONTAINER" 2>&1 | tail -30; exit 1; }
echo "${GRN}Data instance is up.${NC}"

# =============================================================================
# TEST 1: Global vertex-property index (#4353) + CREATE RANGE INDEX FOR (#4486).
# Community. A label-less property lookup must be served by the new index.
# =============================================================================
test_global_index() {
  note "Creating nodes with different labels sharing a 'uid' property."
  mgq "CREATE (:Person {uid:'u-1', tag:'person'}), (:Company {uid:'u-2', tag:'company'}), (:Device {uid:'u-3', tag:'device'});" >/dev/null

  local create
  create="$(mgq "CREATE GLOBAL INDEX ON :(uid);")"
  check_no_error "$create" "CREATE GLOBAL INDEX ON :(uid)"

  local info
  info="$(mgq "SHOW INDEX INFO;")"
  note "SHOW INDEX INFO:"; show "$info"
  check_contains "$info" 'vertex-property' "global vertex-property index listed in SHOW INDEX INFO"

  local plan
  plan="$(mgq "EXPLAIN MATCH (n) WHERE n.uid = 'u-2' RETURN n;")"
  note "EXPLAIN MATCH (n) WHERE n.uid = 'u-2':"; show "$plan"
  check_contains "$plan" 'ScanAllByVertexProperty' "planner uses ScanAllByVertexProperty for label-less lookup"

  local hit
  hit="$(mgq "MATCH (n) WHERE n.uid = 'u-2' RETURN n.tag AS tag;")"
  note "Lookup result:"; show "$hit"
  check_contains "$hit" 'company' "global index lookup returns the right node"

  # Fallback for a labelled lookup with no matching label-property index.
  local fallback
  fallback="$(mgq "EXPLAIN MATCH (n:Device {uid:'u-3'}) RETURN n;")"
  check_contains "$fallback" 'ScanAllByVertexProperty' "global index used as fallback when no :Device(uid) index exists"

  local drop
  drop="$(mgq "DROP GLOBAL INDEX ON :(uid);")"
  check_no_error "$drop" "DROP GLOBAL INDEX ON :(uid)"

  note "CREATE RANGE INDEX FOR ... ON ... (Neo4j-style syntax)."
  local range
  range="$(mgq "CREATE RANGE INDEX FOR (n:Item) ON (n.price);")"
  check_no_error "$range" "CREATE RANGE INDEX FOR (n:Item) ON (n.price)"
  local info2
  info2="$(mgq "SHOW INDEX INFO;")"
  check_contains "$info2" 'Item' "range index shows up as a label+property index on :Item"
  mgq "DROP INDEX ON :Item(price);" >/dev/null 2>&1 || true
}

# =============================================================================
# TEST 2: COUNT { } (#4598), COLLECT { } (#4632), EXISTS { } in projections
# (#4504) and inside CASE (#4596). Community.
# =============================================================================
test_subquery_expressions() {
  mgq "CREATE (a:P {name:'ann'}), (b:P {name:'bob'}), (c:P {name:'cid'}), (d:P {name:'dan'})
       CREATE (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(c), (b)-[:KNOWS]->(c);" >/dev/null

  local rows
  rows="$(mgq "MATCH (p:P)
               RETURN p.name + ':' + toString(COUNT { MATCH (p)-[:KNOWS]->() })
                      + ':' + CASE WHEN EXISTS { MATCH (p)-[:KNOWS]->() } THEN 'social' ELSE 'loner' END AS row
               ORDER BY row;")"
  note "name:COUNT{}:CASE WHEN EXISTS{} per person:"; show "$rows"
  check_contains "$rows" 'ann:2:social' "COUNT {} = 2 and EXISTS {} inside CASE for 'ann'"
  check_contains "$rows" 'bob:1:social' "COUNT {} = 1 for 'bob'"
  check_contains "$rows" 'dan:0:loner'  "COUNT {} = 0 and CASE ELSE branch for isolated 'dan'"

  local coll
  coll="$(mgq "MATCH (p:P {name:'ann'}) RETURN COLLECT { MATCH (p)-[:KNOWS]->(f) RETURN f.name ORDER BY f.name } AS friends;")"
  note "COLLECT {} for 'ann':"; show "$coll"
  check_contains "$coll" '"bob", "cid"' "COLLECT {} builds the ordered friend list"

  local empty
  empty="$(mgq "MATCH (p:P {name:'dan'}) RETURN COLLECT { MATCH (p)-[:KNOWS]->(f) RETURN f.name } AS friends;")"
  note "COLLECT {} for 'dan':"; show "$empty"
  check_contains     "$empty" '[]'   "empty COLLECT {} yields [] (not null)"
  check_not_contains "$empty" 'Null' "empty COLLECT {} is not Null"

  local with
  with="$(mgq "MATCH (p:P) WITH p, EXISTS { MATCH (p)-[:KNOWS]->() } AS hasFriends
               WHERE hasFriends RETURN count(p) AS c;")"
  note "EXISTS {} in WITH + WHERE:"; show "$with"
  check_contains "$with" '2' "EXISTS {} usable in WITH projection (2 people have outgoing KNOWS)"

  # A RETURN inside EXISTS { } is honoured (#4597): DISTINCT/LIMIT affect the answer.
  local body
  body="$(mgq "RETURN EXISTS { MATCH (p:P) RETURN p LIMIT 0 } AS never;")"
  note "EXISTS { ... RETURN p LIMIT 0 }:"; show "$body"
  check_contains "$body" 'false' "RETURN ... LIMIT 0 inside EXISTS {} makes it false"
}

# =============================================================================
# TEST 3: String functions count characters (code points), not bytes (#4585,
# #4586). Community.
# =============================================================================
test_unicode_strings() {
  local out
  out="$(mgq "RETURN toString(size('中文')) + '|' + reverse('héllo') + '|' + substring('中文字', 1, 1)
                     + '|' + left('日本語', 2) + '|' + right('日本語', 1) + '|' + toString(size('🙂👍')) AS r;")"
  note "size/reverse/substring/left/right on multi-byte text:"; show "$out"
  check_contains "$out" '2|olléh|文|日本|語|2' "all five functions operate on characters, not bytes"
}

# =============================================================================
# TEST 4: Cypher semantics fixes. Community.
#   #4588 leading-zero integer literal is octal (010 = 8; 09 is an error)
#   #4589 toInteger() converts whole-number strings exactly / errors on overflow
#   #4590 single() returns null when a predicate is null
#   #4591 split('', ',') returns ['']
#   #4665 list comprehension drops elements whose WHERE is null
# =============================================================================
test_cypher_semantics() {
  local out
  out="$(mgq "RETURN toString(010) + '|' + toString(single(x IN [null, 1] WHERE x > 0) IS NULL)
                     + '|' + toString(size(split('', ','))) + '|' + toString(size([x IN [1, null, 3] WHERE x > 0]))
                     + '|' + toString(toInteger('9223372036854775807') = 9223372036854775807) AS r;")"
  note "010 | single(null-pred) IS NULL | size(split('',',')) | size([.. WHERE null]) | toInteger(max int64) exact:"; show "$out"
  check_contains "$out" '8|true|1|2|true' "octal literal, single() null, split(''), list-comp null, exact toInteger"

  local bad_literal
  bad_literal="$(mgq "RETURN 09 AS r;")"
  note "RETURN 09:"; show "$bad_literal"
  check_error "$bad_literal" "'09' is rejected (invalid octal literal, was 9.0)"

  local overflow
  overflow="$(mgq "RETURN toInteger('99999999999999999999') AS r;")"
  note "toInteger('99999999999999999999'):"; show "$overflow"
  check_error "$overflow" "toInteger() overflow raises instead of wrapping"

  local ornull
  ornull="$(mgq "RETURN toIntegerOrNull('99999999999999999999') AS r;")"
  check_contains "$ornull" 'Null' "toIntegerOrNull() returns Null on overflow"
}

# =============================================================================
# TEST 5: *KSHORTEST filter lambda and per-row |k expression (#4559). Community.
# Graph: A->B->E, A->C->E, A->D->E (three shortest paths of equal length).
# =============================================================================
test_kshortest() {
  mgq "CREATE (a:N {name:'A', k:2}), (b:N {name:'B'}), (c:N {name:'C'}), (d:N {name:'D'}), (e:N {name:'E'})
       CREATE (a)-[:R]->(b), (b)-[:R]->(e), (a)-[:R]->(c), (c)-[:R]->(e), (a)-[:R]->(d), (d)-[:R]->(e);" >/dev/null

  local all
  all="$(mgq "MATCH (a:N {name:'A'}), (e:N {name:'E'}) WITH a, e MATCH p=(a)-[*KSHORTEST]->(e) RETURN count(p) AS c;")"
  note "KSHORTEST without filter:"; show "$all"
  check_contains "$all" '3' "3 shortest paths without a filter"

  local filtered
  filtered="$(mgq "MATCH (a:N {name:'A'}), (e:N {name:'E'}) WITH a, e
                   MATCH p=(a)-[*KSHORTEST (r, n | n.name <> 'B')]->(e)
                   RETURN [x IN nodes(p) | x.name] AS names ORDER BY names;")"
  note "KSHORTEST (r, n | n.name <> 'B'):"; show "$filtered"
  check_contains     "$filtered" '"C"' "path through C kept"
  check_contains     "$filtered" '"D"' "path through D kept"
  check_not_contains "$filtered" '"B"' "path through B pruned by the filter lambda"

  local perrow
  perrow="$(mgq "MATCH (a:N {name:'A'}), (e:N {name:'E'}) WITH a, e MATCH p=(a)-[*KSHORTEST |a.k]->(e) RETURN count(p) AS c;")"
  note "KSHORTEST |a.k (k=2 from the row):"; show "$perrow"
  check_contains "$perrow" '2' "|k taken from a row expression limits to 2 paths"

  local neg
  neg="$(mgq "MATCH (a:N {name:'A'}), (e:N {name:'E'}) WITH a, e MATCH p=(a)-[*KSHORTEST |-1]->(e) RETURN count(p);")"
  check_error "$neg" "negative |k raises a query error"
}

# =============================================================================
# TEST 6: TERMINATE TRANSACTIONS "*" (#4534) + strict transaction id parsing.
# Community. A background mgconsole keeps an explicit transaction open so the
# wildcard has something to kill.
# =============================================================================
test_terminate_all() {
  note "Opening a long-lived explicit transaction in the background..."
  ( printf 'BEGIN;\nCREATE (:Held {v:1});\n'; sleep 20 ) | "$MGCONSOLE" --host "$HOST" --port "$DATA_BOLT" >/dev/null 2>&1 &
  local bg=$!
  local i=0 shown=""
  while [ "$i" -lt 30 ]; do
    shown="$(mgq "SHOW TRANSACTIONS;")"
    printf '%s' "$shown" | grep -q "Held" && break
    sleep 0.3; i=$((i + 1))
  done
  note "SHOW TRANSACTIONS:"; show "$shown"
  check_contains "$shown" 'Held' "background transaction is visible"

  local killed
  killed="$(mgq "TERMINATE TRANSACTIONS \"*\";")"
  note "TERMINATE TRANSACTIONS \"*\":"; show "$killed"
  check_contains "$killed" 'transaction_id' "wildcard terminate returns the transaction_id/killed table"
  check_contains "$killed" 'true'           "background transaction reported killed: true"

  wait "$bg" 2>/dev/null || true
  local after
  after="$(mgq "MATCH (h:Held) RETURN count(h) AS c;")"
  check_contains "$after" '0' "terminated transaction's write was rolled back"

  local mixed
  mixed="$(mgq "TERMINATE TRANSACTIONS \"*\", \"1\";")"
  check_error "$mixed" "wildcard cannot be combined with explicit ids"

  local junk
  junk="$(mgq "TERMINATE TRANSACTIONS \"12abc\";")"
  note "TERMINATE TRANSACTIONS \"12abc\":"; show "$junk"
  check_error "$junk" "id with trailing characters is rejected instead of matching numeric prefix"
}

# =============================================================================
# TEST 7: Property-value descriptions + description() (#4526). Community.
# =============================================================================
test_property_value_descriptions() {
  mgq "SET DESCRIPTION ON PROPERTY gender VALUE '1' 'Male';
       SET DESCRIPTION ON PROPERTY gender VALUE '2' 'Female';
       SET DESCRIPTION ON PROPERTY gender 'Coded gender';" >/dev/null
  mgq "CREATE (:Emp {name:'Ada', gender:'1'}), (:Emp {name:'Bea', gender:'2'}), (:Emp {name:'Cyd', gender:'9'});" >/dev/null

  local shown
  shown="$(mgq "SHOW DESCRIPTIONS;")"
  note "SHOW DESCRIPTIONS:"; show "$shown"
  check_contains "$shown" 'Male'         "property-value description stored"
  check_contains "$shown" 'Coded gender' "global property description stored alongside value descriptions"

  local resolved
  resolved="$(mgq "MATCH (e:Emp) RETURN e.name + '=' + coalesce(description('gender', e.gender), 'n/a') AS r ORDER BY r;")"
  note "description('gender', e.gender):"; show "$resolved"
  check_contains "$resolved" 'Ada=Male'   "description() resolves '1' -> Male"
  check_contains "$resolved" 'Bea=Female' "description() resolves '2' -> Female"
  check_contains "$resolved" 'Cyd=n/a'    "description() returns Null for an undescribed value"

  mgq "DELETE DESCRIPTION ON PROPERTY gender VALUE '1';" >/dev/null
  local gone
  gone="$(mgq "RETURN description('gender', '1') AS d;")"
  check_contains "$gone" 'Null' "DELETE DESCRIPTION ON PROPERTY ... VALUE removes the entry"
  mgq "DELETE DESCRIPTION ON PROPERTY gender VALUE '2'; DELETE DESCRIPTION ON PROPERTY gender;" >/dev/null 2>&1 || true
}

# =============================================================================
# TEST 8: MAGE collections.disjunction/subtract/duplicates (#4415), map.get /
# map.merge_list (#4417), text.compare_cleaned (#4435), NULL handling (#4416).
# Community.
# =============================================================================
test_mage_helpers() {
  local coll
  # Set-style results carry no ordering guarantee -> sort before comparing.
  coll="$(mgq "RETURN collections.sort(collections.disjunction([1, 2, 3, 4, 5], [3, 4, 5])) AS disjunction,
                      collections.sort(collections.subtract([1, 2, 3, 4, 5, 6, 6], [3, 4, 5])) AS subtracted,
                      collections.sort(collections.duplicates([1, 1, 2, 3, 3, 3])) AS duplicates;")"
  note "collections.disjunction / subtract / duplicates (sorted):"; show "$coll"
  check_contains "$coll" '[1, 2]'    "disjunction([1..5],[3,4,5]) = [1, 2]"
  check_contains "$coll" '[1, 2, 6]' "subtract([1..6,6],[3,4,5]) = [1, 2, 6]"
  check_contains "$coll" '[1, 3]'    "duplicates([1,1,2,3,3,3]) = [1, 3]"

  local nulls
  nulls="$(mgq "RETURN toString(collections.sum(null) IS NULL) + '|' + toString(size(collections.sort(null)))
                       + '|' + toString(collections.contains(null, 1)) AS r;")"
  note "collections.* with NULL arguments:"; show "$nulls"
  check_contains "$nulls" 'true|0|false' "sum(null)->null, sort(null)->[], contains(null,x)->false"

  local maps
  maps="$(mgq "RETURN map.get({name:'Ivan'}, 'country', 'unknown', false) AS fallback,
                      map.merge_list([{a:1}, {a:2, b:3}]) AS merged,
                      map.merge(null, {x:1}) AS null_merge;")"
  note "map.get / map.merge_list / map.merge(null, ...):"; show "$maps"
  check_contains "$maps" 'unknown'      "map.get returns the fallback for an absent key"
  check_contains "$maps" '{a: 2, b: 3}' "map.merge_list merges left to right (last wins)"
  check_contains "$maps" '{x: 1}'       "map.merge treats a null map as empty"

  local text
  text="$(mgq "RETURN text.compare_cleaned('Hello, World!', 'hello world') AS same,
                      text.compare_cleaned('abc', 'abd') AS differ,
                      text.compare_cleaned(null, 'x') AS with_null;")"
  note "text.compare_cleaned:"; show "$text"
  check_contains "$text" 'true'  "compare_cleaned ignores punctuation/case"
  check_contains "$text" 'false' "compare_cleaned distinguishes different text / NULL -> false"
}

# =============================================================================
# TEST 9: MAGE convert JSON functions (#4443): from_json_map / from_json_list
# (with $-path selector), to_map, to_json, and the apoc.convert.* alias remap.
# Community.
# =============================================================================
test_convert_json() {
  local from
  from="$(mgq "RETURN convert.from_json_map('{\"a\": 1, \"b\": {\"c\": 2, \"d\": [10, 20]}}', '\$.b') AS sub,
                      convert.from_json_list('{\"a\": [1, 2, 3]}', '\$.a') AS lst,
                      convert.from_json_map('{\"mode\": \"fast\"}')['mode'] AS mode;")"
  note "convert.from_json_map / from_json_list with path:"; show "$from"
  check_contains "$from" 'c: 2'     "from_json_map with '\$.b' selects the nested map"
  check_contains "$from" '[1, 2, 3]' "from_json_list with '\$.a' selects the nested list"
  check_contains "$from" 'fast'      "from_json_map result is directly indexable"

  local to
  to="$(mgq "CREATE (n:Person:Human {name:'Ana', age:30})
             RETURN convert.to_json({a: 1, b: 'x', c: [1, 2], d: null}) AS j, convert.to_map(n) AS m, convert.to_json(n) AS nj;")"
  note "convert.to_json / to_map:"; show "$to"
  # mgconsole prints string cells with inner quotes escaped (\"), hence the needles below.
  check_contains "$to" '{\"a\":1,\"b\":\"x\",\"c\":[1,2],\"d\":null}' "to_json serialises a map compactly"
  check_contains "$to" 'name: "Ana"'                                  "to_map returns the node's property map"
  check_contains "$to" '\"labels\":[\"Person\",\"Human\"]'            "to_json emits structured node output with labels"
  check_contains "$to" '\"type\":\"node\"'                            "to_json tags graph objects with a type"

  # Breaking change: apoc.convert.toJson now resolves to the C++ convert module.
  local apoc
  apoc="$(mgq "RETURN apoc.convert.toJson({k: 'v', n: 1}) AS j;")"
  note "apoc.convert.toJson alias:"; show "$apoc"
  check_contains "$apoc" '{\"k\":\"v\",\"n\":1}' "apoc.convert.toJson alias produces the convert.to_json format"
}

# =============================================================================
# TEST 10: MAGE search.node / search.node_all (#4460). Community.
# =============================================================================
test_search_node() {
  mgq "CREATE (:Person {name:'Ann'}), (:Person {name:'Bob'}), (:Person {name:'Cid'}),
              (:Movie {title:'Matrix', tagline:'Matrix'}), (:Movie {title:'Other', tagline:'Matrix'});" >/dev/null

  local ge
  ge="$(mgq "CALL search.node({Person: 'name'}, '>=', 'Bob') YIELD node RETURN node.name AS name ORDER BY name;")"
  note "search.node({Person:'name'}, '>=', 'Bob'):"; show "$ge"
  check_contains     "$ge" 'Bob' "matches 'Bob'"
  check_contains     "$ge" 'Cid' "matches 'Cid'"
  check_not_contains "$ge" 'Ann' "excludes 'Ann' (< 'Bob')"

  local exact
  exact="$(mgq "CALL search.node('{\"Person\": \"name\"}', 'exact', 'Bob') YIELD node RETURN count(node) AS c;")"
  note "search.node (JSON-string spec, 'exact'):"; show "$exact"
  check_contains "$exact" '1' "JSON-string label/property spec works with 'exact'"

  local dedup all
  dedup="$(mgq "CALL search.node({Movie: ['title', 'tagline']}, 'exact', 'Matrix') YIELD node RETURN count(node) AS c;")"
  all="$(mgq "CALL search.node_all({Movie: ['title', 'tagline']}, 'exact', 'Matrix') YIELD node RETURN count(node) AS c;")"
  note "search.node (dedup) vs search.node_all (keeps duplicates):"; show "$dedup"; show "$all"
  check_contains "$dedup" '2' "search.node de-duplicates the node matching on both properties"
  check_contains "$all"   '3' "search.node_all keeps one row per matching property"
}

# =============================================================================
# TEST 11: MAGE path.expand_config (#4530): relationship/label filters, limit,
# label/relationship sequences, and rejection of unknown config keys. Community.
# =============================================================================
test_path_expand_config() {
  mgq "CREATE (w:Wolf {name:'w'}), (d:Dog {name:'d'}), (c:Cat {name:'c'}), (m:Mouse {name:'m'}), (h:Human {name:'h'})
       CREATE (w)-[:CATCHES]->(d), (d)-[:CATCHES]->(c), (c)-[:CATCHES]->(m), (d)-[:HATES]->(h);" >/dev/null

  local seq
  seq="$(mgq "MATCH (w:Wolf)
              CALL path.expand_config(w, {minHops: 0, maxHops: 3, filterStartNode: true,
                                          sequence: 'Wolf, CATCHES>, Dog, CATCHES>, Cat, CATCHES>, Mouse'})
              YIELD result RETURN [n IN nodes(result) | labels(n)[0]] AS names ORDER BY size(names) DESC LIMIT 1;")"
  note "expand_config with a label/relationship sequence:"; show "$seq"
  check_contains "$seq" '"Wolf", "Dog", "Cat", "Mouse"' "sequence expansion reaches Wolf->Dog->Cat->Mouse"

  local lim
  lim="$(mgq "MATCH (w:Wolf)
              CALL path.expand_config(w, {relationshipFilter: 'CATCHES>', maxHops: 3, limit: 2})
              YIELD result RETURN count(result) AS c;")"
  note "expand_config with limit: 2:"; show "$lim"
  check_contains "$lim" '2' "limit caps the number of returned paths"

  local incoming
  incoming="$(mgq "MATCH (m:Mouse)
                   CALL path.expand_config(m, {relationshipFilter: '<CATCHES', minHops: 3, maxHops: 3})
                   YIELD result RETURN [n IN nodes(result) | labels(n)[0]] AS names;")"
  note "expand_config with '<CATCHES' (incoming only):"; show "$incoming"
  check_contains "$incoming" '"Mouse", "Cat", "Dog", "Wolf"' "'<TYPE' means incoming TYPE (new reading)"

  local bogus
  bogus="$(mgq "MATCH (w:Wolf) CALL path.expand_config(w, {bogusKey: 1}) YIELD result RETURN count(result);")"
  note "expand_config with an unknown key:"; show "$bogus"
  check_error "$bogus" "unknown config keys are rejected instead of ignored"
}

# =============================================================================
# TEST 12: --storage-omit-vector-index-properties-on-return / runtime setting
# storage.omit_vector_index_properties_on_return (#4556). Community.
# =============================================================================
test_omit_vector_properties() {
  mgq "CREATE VECTOR INDEX embIdx ON :Emb(vec) WITH CONFIG {\"dimension\": 2, \"capacity\": 16};" >/dev/null
  mgq "CREATE (:Emb {name:'e1', vec:[0.25, 0.75]});" >/dev/null

  local before
  before="$(mgq "MATCH (n:Emb) RETURN n;")"
  note "Whole node with the setting off:"; show "$before"
  check_contains "$before" 'vec' "vector property returned while the setting is false"

  local set
  set="$(mgq "SET DATABASE SETTING 'storage.omit_vector_index_properties_on_return' TO 'true';")"
  check_no_error "$set" "runtime setting storage.omit_vector_index_properties_on_return accepted"
  local shown
  shown="$(mgq "SHOW DATABASE SETTING 'storage.omit_vector_index_properties_on_return';")"
  check_contains "$shown" 'true' "SHOW DATABASE SETTING reflects the new value"

  local omitted
  omitted="$(mgq "MATCH (n:Emb) RETURN n;")"
  note "Whole node with the setting on:"; show "$omitted"
  check_not_contains "$omitted" 'vec' "vector-indexed property omitted from the returned node"
  check_contains     "$omitted" 'e1'  "other properties still returned"

  local explicit
  explicit="$(mgq "MATCH (n:Emb) RETURN n.vec AS v;")"
  note "Explicit n.vec access:"; show "$explicit"
  check_contains "$explicit" '0.25' "explicit property access still returns the embedding"

  mgq "SET DATABASE SETTING 'storage.omit_vector_index_properties_on_return' TO 'false';" >/dev/null
  local restored
  restored="$(mgq "MATCH (n:Emb) RETURN n;")"
  check_contains "$restored" 'vec' "property returned again after resetting the setting"
}

# =============================================================================
# TEST 13: New configuration flags are present in SHOW CONFIG and the metrics
# format default is OpenMetrics (#4334, #4546, #4579, #4678). Community.
# =============================================================================
test_new_config_flags() {
  local cfg
  cfg="$(mgq "SHOW CONFIG;")"
  # SHOW CONFIG reports flag names with underscores (query_ast_cache_max_size).
  note "Relevant SHOW CONFIG rows (name | default | current):"
  printf '%s\n' "$cfg" | grep -iE "query_ast_cache_max_size|writeback_window|rocksdb_keep_log|metrics_format" \
    | awk -F'|' '{gsub(/ +/," "); print "     " $2 "|" $3 "|" $4}'
  check_contains "$cfg" 'query_ast_cache_max_size'             "--query-ast-cache-max-size exists (query cache bound)"
  check_contains "$cfg" 'storage_snapshot_writeback_window_mib' "--storage-snapshot-writeback-window-mib exists"
  check_contains "$cfg" 'storage_rocksdb_keep_log_file_num'     "--storage-rocksdb-keep-log-file-num exists"

  local mf
  mf="$(printf '%s\n' "$cfg" | grep -i 'metrics_format')"
  check_contains "$mf" 'OpenMetrics' "--metrics-format default is OpenMetrics (was JSON)"

  local ast
  ast="$(printf '%s\n' "$cfg" | grep -i 'query_ast_cache_max_size')"
  check_contains "$ast" '1000' "query AST cache default is 1000 entries"
}

# =============================================================================
# TEST 14: WAL recovery with the new per-file header (timestamp range + tx
# count) (#4528). Community. Write, restart, verify data + health.
# =============================================================================
test_wal_recovery() {
  note "Writing a marker node across several transactions (multiple WAL entries)."
  mgq "CREATE (:WalMarker {tag:'header-survivor', v:1});" >/dev/null
  mgq "MATCH (m:WalMarker) SET m.v = 2;" >/dev/null
  mgq "MATCH (m:WalMarker) SET m.v = 313;" >/dev/null
  # Descriptions are WAL-persisted too (numeric VALUE literal this time).
  mgq "SET DESCRIPTION ON PROPERTY status VALUE 1 'Active';" >/dev/null

  note "Restarting the container (triggers durability recovery)..."
  docker restart "$DATA_CONTAINER" >/dev/null
  wait_for "$DATA_BOLT" "RETURN 1;" || { echo "    ${RED}✗ instance did not come back after restart${NC}"; fails=$((fails+1)); return; }

  local out
  out="$(mgq "MATCH (n:WalMarker) RETURN n.tag AS tag, n.v AS v;")"
  note "After restart:"; show "$out"
  check_contains "$out" 'header-survivor' "marker node recovered from WAL after restart"
  check_contains "$out" '313'             "last WAL update applied (v=313)"

  local health
  health="$(mgq "SHOW STORAGE INFO ON CURRENT DATABASE;")"
  check_contains "$health" 'ready' "database healthy (ready) after recovery"

  local descs
  descs="$(mgq "RETURN description('status', 1) AS d;")"
  note "description('status', 1) after restart:"; show "$descs"
  check_contains "$descs" 'Active' "property-value descriptions survive the restart (durable)"
}

# =============================================================================
# TEST 15: cross_database module requires Memgraph Enterprise (#4726).
# Without a license the module is not loaded at all; with one, it is.
# =============================================================================
test_cross_database_gating() {
  local procs
  procs="$(mgq "CALL mg.procedures() YIELD name WHERE name STARTS WITH 'cross_database.' RETURN count(name) AS c;")"
  note "Number of registered cross_database.* procedures:"; show "$procs"
  if [ "$ENTERPRISE" = "true" ]; then
    check_not_contains "$procs" '| 0 ' "cross_database procedures registered with a valid license"
  else
    check_contains "$procs" '0' "cross_database procedures NOT registered without a license"
    local logs
    logs="$(docker logs "$DATA_CONTAINER" 2>&1 | grep -i "cross_database" | grep -i "license" | head -1)"
    note "Container log:"; show "${logs:-<no matching log line>}"
    check_contains "$logs" 'enterprise license' "startup log explains cross_database needs an enterprise license"
  fi
}

# =============================================================================
# TEST 16: Metrics HTTP endpoint serves OpenMetrics by default (#4678).
# Enterprise (the metrics server is an Enterprise feature).
# =============================================================================
test_openmetrics_endpoint() {
  local body ctype
  body="$(curl -s --max-time 10 "http://$HOST:$METRICS_PORT/metrics")"
  ctype="$(curl -s -o /dev/null --max-time 10 -w '%{content_type}' "http://$HOST:$METRICS_PORT/metrics")"
  note "Content-Type: $ctype"
  note "First metric lines:"; printf '%s\n' "$body" | head -6 | sed 's/^/      /'
  check_contains "$ctype" 'openmetrics-text' "Content-Type is application/openmetrics-text"
  check_contains "$body"  '# TYPE'           "body uses the OpenMetrics text exposition format"
  check_contains "$body"  'memgraph_'        "memgraph_* metrics present"
  check_not_contains "$body" '"General"'     "legacy JSON document not served by default"

  local root
  root="$(curl -s --max-time 10 "http://$HOST:$METRICS_PORT/")"
  check_contains "$root" '# TYPE' "root path '/' also serves OpenMetrics"
}

# =============================================================================
# TEST 17: Coordinator introspection: --coordinator-id=0 accepted (#4483),
# SHOW ROUTING TABLE (#4502), SHOW VERSION on a coordinator (#4535).
# Enterprise/HA. Starts a dedicated single-coordinator container.
# =============================================================================
test_coordinator_introspection() {
  note "Starting a coordinator with --coordinator-id=0..."
  docker rm -f "$COORD_CONTAINER" >/dev/null 2>&1 || true
  docker run -d --rm \
    --name "$COORD_CONTAINER" \
    -p "$COORD_BOLT:7687" \
    ${ENT_ENVS[@]+"${ENT_ENVS[@]}"} \
    "$IMAGE" \
    --telemetry-enabled=false --log-level=TRACE --also-log-to-stderr \
    --bolt-port=7687 \
    --coordinator-id=0 \
    --coordinator-port=10111 \
    --management-port=10121 \
    --coordinator-hostname=localhost \
    >/dev/null || { echo "    ${RED}✗ failed to start coordinator container${NC}"; fails=$((fails+1)); return; }

  if ! wait_for "$COORD_BOLT" "SHOW COORDINATOR SETTINGS;" 400; then
    echo "    ${RED}✗ coordinator never became ready. Logs:${NC}"
    docker logs "$COORD_CONTAINER" 2>&1 | tail -20 | sed 's/^/      /'
    fails=$((fails + 1)); return
  fi
  echo "    ${GRN}✓${NC} coordinator started with --coordinator-id=0"

  local routing
  routing="$(mgc "SHOW ROUTING TABLE;")"
  note "SHOW ROUTING TABLE (no data instances registered):"; show "$routing"
  check_contains "$routing" 'role'    "routing table has a 'role' column"
  check_contains "$routing" 'servers' "routing table has a 'servers' column"
  check_contains "$routing" 'ROUTE'   "ROUTE row lists the coordinator itself"
  check_not_contains "$routing" 'WRITE' "no WRITE row while no MAIN is registered"

  local on_data
  on_data="$(mgq "SHOW ROUTING TABLE;")"
  note "SHOW ROUTING TABLE on the data instance:"; show "$on_data"
  check_error "$on_data" "SHOW ROUTING TABLE rejected on a data instance"

  local ver
  ver="$(mgc "SHOW VERSION;")"
  note "SHOW VERSION on the coordinator:"; show "$ver"
  check_no_error "$ver" "SHOW VERSION runs on a coordinator"
  check_contains "$ver" 'version' "SHOW VERSION returns the version column"

  local build
  build="$(mgc "SHOW BUILD INFO;")"
  check_no_error "$build" "SHOW BUILD INFO runs on a coordinator"
}

# =============================================================================
# TEST 18: Coordinator SSO roles + COORDINATOR_READ / COORDINATOR_WRITE (#4399).
# Enterprise/HA. Roles live in the coordinator's Raft state. Granting only
# COORDINATOR_READ keeps basic auth open (SSO enforcement needs
# --auth-module-mappings AND a COORDINATOR_WRITE role), so this stays safe.
# =============================================================================
test_coordinator_roles() {
  local created
  created="$(mgc "CREATE ROLE analyst; CREATE ROLE IF NOT EXISTS analyst; GRANT COORDINATOR_READ TO analyst;")"
  note "CREATE ROLE + GRANT COORDINATOR_READ on the coordinator:"; show "$created"
  check_no_error "$created" "role creation and COORDINATOR_READ grant accepted"

  local roles
  roles="$(mgc "SHOW ROLES;")"
  note "SHOW ROLES:"; show "$roles"
  check_contains "$roles" 'analyst' "role stored in coordinator state"

  local privs
  privs="$(mgc "SHOW PRIVILEGES FOR ROLE analyst;")"
  note "SHOW PRIVILEGES FOR ROLE analyst:"; show "$privs"
  check_contains     "$privs" 'COORDINATOR_READ'  "COORDINATOR_READ listed"
  check_not_contains "$privs" 'COORDINATOR_WRITE' "COORDINATOR_WRITE not granted"

  local revoked
  revoked="$(mgc "REVOKE COORDINATOR_READ FROM analyst; SHOW PRIVILEGES FOR ROLE analyst;")"
  note "After REVOKE COORDINATOR_READ:"; show "$revoked"
  check_not_contains "$revoked" 'COORDINATOR_READ' "REVOKE removes the privilege"

  # Coordinator-only privileges are rejected on data instances.
  local on_data
  on_data="$(mgq "CREATE ROLE tmp_role; GRANT COORDINATOR_READ TO tmp_role;")"
  note "GRANT COORDINATOR_READ on the data instance:"; show "$on_data"
  check_error "$on_data" "COORDINATOR_READ rejected on a data instance"
  mgq "DROP ROLE tmp_role;" >/dev/null 2>&1 || true

  local dropped
  dropped="$(mgc "DROP ROLE analyst; SHOW ROLES;")"
  check_not_contains "$dropped" 'analyst' "DROP ROLE removes the coordinator role"
}

# =============================================================================
# TEST 19: Text search respects fine-grained label permissions (#4316).
# Enterprise. NOTE: creates users, which disables anonymous access, so it MUST
# run after every test that assumes no users exist.
# =============================================================================
test_text_search_rbac() {
  note "Bootstrapping admin user + text-indexed :Note/:Public data."
  mgq "CREATE USER admin IDENTIFIED BY 'admin';" >/dev/null
  mgq_admin "GRANT ALL PRIVILEGES TO admin; GRANT DATABASE * TO admin;
             GRANT READ, SET PROPERTY {*} ON NODES CONTAINING LABELS * TO admin;
             GRANT READ, SET PROPERTY {*} ON EDGES OF TYPE * TO admin;" >/dev/null
  mgq_admin "CREATE TEXT INDEX noteIdx ON :Note; CREATE TEXT INDEX pubIdx ON :Public;" >/dev/null
  mgq_admin "CREATE (:Note {body:'secret memgraph roadmap'}); CREATE (:Public {body:'public memgraph blog'});" >/dev/null

  local admin_hits
  admin_hits="$(mgq_admin "CALL text_search.search('noteIdx', 'data.body:memgraph') YIELD node RETURN count(node) AS c;")"
  note "admin hits on noteIdx:"; show "$admin_hits"
  check_contains "$admin_hits" '1' "admin sees the :Note hit"

  note "Creating 'reader': MATCH + READ on :Public only (no access to :Note)."
  mgq_admin "CREATE USER reader IDENTIFIED BY 'reader';
             GRANT MATCH TO reader; GRANT DATABASE memgraph TO reader;
             GRANT READ ON NODES CONTAINING LABELS :Public TO reader;
             GRANT READ {*} ON NODES CONTAINING LABELS :Public TO reader;" >/dev/null

  local reader_pub
  reader_pub="$(mgq_user reader "CALL text_search.search('pubIdx', 'data.body:memgraph') YIELD node RETURN node.body AS body;")"
  note "reader hits on pubIdx:"; show "$reader_pub"
  check_contains "$reader_pub" 'public memgraph blog' "reader sees the :Public hit it may read"

  local reader_note
  reader_note="$(mgq_user reader "CALL text_search.search('noteIdx', 'data.body:memgraph') YIELD node RETURN count(node) AS c;")"
  note "reader hits on noteIdx (label not readable):"; show "$reader_note"
  check_contains     "$reader_note" '0'       "reader gets 0 hits on the :Note index"
  check_not_contains "$reader_note" 'roadmap' "unreadable :Note content never leaks through text search"
}

# =============================================================================
# TEST 20: The COORDINATOR privilege was removed from data instances (#4399).
# Enterprise (GRANT / SHOW PRIVILEGES need a license). Reuses the admin user
# created by the text-search RBAC test -> keep LAST.
# =============================================================================
test_coordinator_privilege_removed() {
  local grant
  grant="$(mgq_admin "GRANT COORDINATOR TO admin;")"
  note "GRANT COORDINATOR TO admin:"; show "$grant"
  check_error "$grant" "GRANT COORDINATOR is a syntax error"

  local deny
  deny="$(mgq_admin "DENY COORDINATOR TO admin;")"
  check_error "$deny" "DENY COORDINATOR is a syntax error"

  # Multi-tenant (Enterprise) builds require a database specifier here, and
  # USER disambiguates from the implicit same-named role.
  local privs
  privs="$(mgq_admin "SHOW PRIVILEGES FOR USER admin ON MAIN;")"
  note "SHOW PRIVILEGES FOR USER admin ON MAIN (first rows):"; printf '%s\n' "$privs" | head -8 | sed 's/^/      /'
  check_no_error     "$privs" "SHOW PRIVILEGES FOR USER admin ON MAIN"
  check_contains     "$privs" 'TRANSACTION_MANAGEMENT' "other privileges still reported"
  check_not_contains "$privs" 'COORDINATOR' "COORDINATOR no longer part of GRANT ALL PRIVILEGES / SHOW PRIVILEGES"
}

# ================================= Run tests =================================
# Community-tier features first (they assume no users exist).
run_test "Global vertex-property index + CREATE RANGE INDEX FOR (#4353, #4486)"       test_global_index
run_test "COUNT {} / COLLECT {} / EXISTS {} in projections & CASE (#4598, #4632, #4504, #4596)" test_subquery_expressions
run_test "Unicode-aware string functions (#4585, #4586)"                             test_unicode_strings
run_test "Cypher semantics: octal, toInteger, single, split, list comp (#4588-#4591, #4665)" test_cypher_semantics
run_test "*KSHORTEST filter lambda + per-row |k (#4559)"                            test_kshortest
run_test "TERMINATE TRANSACTIONS \"*\" + strict id parsing (#4534)"                  test_terminate_all
run_test "Property-value descriptions + description() (#4526)"                       test_property_value_descriptions
run_test "MAGE collections / map / text additions (#4415, #4417, #4435)"             test_mage_helpers
run_test "MAGE convert JSON functions + apoc alias (#4443)"                          test_convert_json
run_test "MAGE search.node / search.node_all (#4460)"                                test_search_node
run_test "MAGE path.expand_config (#4530)"                                           test_path_expand_config
run_test "Vector-index property omission runtime setting (#4556)"                    test_omit_vector_properties
run_test "New configuration flags + OpenMetrics default (#4334, #4546, #4579, #4678)" test_new_config_flags
run_test "WAL header-based recovery on restart (#4528)"                              test_wal_recovery
run_test "cross_database gated by Enterprise license (#4726)"                        test_cross_database_gating

# Enterprise-tier features.
if [ "$ENTERPRISE" = "true" ]; then
  run_test "Metrics endpoint serves OpenMetrics by default (#4678)"                  test_openmetrics_endpoint
  run_test "Coordinator: --coordinator-id=0, SHOW ROUTING TABLE, SHOW VERSION (#4483, #4502, #4535)" test_coordinator_introspection
  run_test "Coordinator SSO roles + COORDINATOR_READ/WRITE (#4399)"                  test_coordinator_roles
  # Creates users (disables anonymous access) -> after all anonymous tests.
  run_test "Text search respects fine-grained label permissions (#4316)"            test_text_search_rbac
  run_test "COORDINATOR privilege removed from data instances (#4399)"               test_coordinator_privilege_removed
else
  skip_test "Metrics endpoint serves OpenMetrics by default (#4678)"                "no enterprise license (set MEMGRAPH_ENTERPRISE_LICENSE + MEMGRAPH_ORGANIZATION_NAME)"
  skip_test "Coordinator: --coordinator-id=0, SHOW ROUTING TABLE, SHOW VERSION (#4483, #4502, #4535)" "no enterprise license"
  skip_test "Coordinator SSO roles + COORDINATOR_READ/WRITE (#4399)"                "no enterprise license"
  skip_test "Text search respects fine-grained label permissions (#4316)"          "no enterprise license"
  skip_test "COORDINATOR privilege removed from data instances (#4399)"             "no enterprise license"
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
