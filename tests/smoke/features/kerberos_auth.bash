#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/../utils.bash"

# End-to-end Kerberos/GSSAPI login against the image under test: a real KDC
# issues a real service ticket, memgraph's reference kerberos auth module
# accepts it with a real keytab, and the resulting session is checked for the
# mapped user, role and privileges.
#
# Unlike every other feature here this one cannot run against the shared
# memgraph_smoke container: SSO is switched on at startup
# (--auth-module-mappings) and the acceptor needs a keytab, so the test brings
# up its own containers and tears them down again.
#
#   memgraph_smoke_kdc  ubuntu + MIT KDC, and the Bolt client (kerberos/client.py)
#   memgraph_smoke_krb  the image under test, SSO enabled, keytab mounted
#
# It is also split in two: test_kerberos_auth_setup builds the realm and the
# instance, test_kerberos_auth does the login. The caller runs them one after
# the other under `set -e`, so any failing step in either stops the suite right
# where it broke.
#
# The whole configuration lives here; both container-side scripts read it from
# the environment so the realm and principals are defined exactly once.
KERBEROS_DIR="$SMOKE_DIR/kerberos"
KERBEROS_NETWORK="memgraph_smoke_kerberos"
KERBEROS_KDC_CONTAINER="memgraph_smoke_kdc"
KERBEROS_MG_CONTAINER="memgraph_smoke_krb"
KERBEROS_BOOTSTRAP_CONTAINER="memgraph_smoke_krb_bootstrap"
# Durable auth storage, so the role created before SSO is switched on is still
# there when memgraph comes back up with the module enabled. A named volume
# rather than a bind mount: /var/lib/memgraph has to be writable by the
# in-container memgraph user (uid 101).
KERBEROS_DATA_VOLUME="memgraph_smoke_kerberos_data"
# Network aliases. The memgraph one is part of the service principal, so it is
# also the host the client asks the KDC for a ticket for.
KERBEROS_KDC_HOST="mgkdc"
KERBEROS_MG_HOST="mgserver"
KERBEROS_REALM="MEMGRAPH.TEST"
KERBEROS_SERVICE_PRINCIPAL="memgraph/${KERBEROS_MG_HOST}@${KERBEROS_REALM}"
KERBEROS_CLIENT_USER="kerberos_tester"
KERBEROS_CLIENT_PRINCIPAL="${KERBEROS_CLIENT_USER}@${KERBEROS_REALM}"
KERBEROS_CLIENT_PASSWORD="kerberos_tester_1234"
KERBEROS_ROLE="kerberos_role"
# The shared container suite already uses MEMGRAPH_BOLT_PORT (8003).
KERBEROS_BOLT_PORT="8004"
# Host side of /krb5: the keytab and krb5.conf the KDC generates for the
# memgraph container. A fixed path (not mktemp) so cleanup works even when the
# run that created it was killed.
KERBEROS_SHARED_DIR="${TMPDIR:-/tmp}/memgraph_smoke_kerberos"

kerberos_cleanup() {
  docker rm -f "$KERBEROS_MG_CONTAINER" "$KERBEROS_BOOTSTRAP_CONTAINER" \
    "$KERBEROS_KDC_CONTAINER" >/dev/null 2>&1 || true
  docker network rm "$KERBEROS_NETWORK" >/dev/null 2>&1 || true
  docker volume rm "$KERBEROS_DATA_VOLUME" >/dev/null 2>&1 || true
  # setup-kdc.sh chowns the shared dir back to the invoking user, so an
  # unprivileged runner can remove the root-created keytab.
  rm -rf "$KERBEROS_SHARED_DIR"
}

kerberos_dump_logs() {
  echo "--- $KERBEROS_MG_CONTAINER (memgraph, TRACE) ---"
  docker logs "$KERBEROS_MG_CONTAINER" 2>&1 | tail -n 80 || true
  # The container's stdout is just `sleep infinity`; the KDC's own log is
  # what says whether tickets were issued (see [logging] in setup-kdc.sh).
  echo "--- $KERBEROS_KDC_CONTAINER (krb5kdc.log) ---"
  docker exec "$KERBEROS_KDC_CONTAINER" tail -n 30 /var/log/krb5kdc.log 2>&1 || true
}

kerberos_wait_for_bolt() {
  # With the module enabled Bolt is access-controlled, so an unauthenticated
  # probe query can't tell "not listening yet" from "auth refused". Wait for
  # the line memgraph logs once the Bolt server is accepting instead.
  local container="$1"
  local retries=0
  until docker logs "$container" 2>&1 | grep -q "Bolt server is fully armed and operational"; do
    retries=$((retries + 1))
    if [ "$retries" -ge 300 ]; then
      echo "memgraph in $container never opened its Bolt server"
      return 1
    fi
    sleep 0.2
  done
}

test_kerberos_auth_setup() {
  echo "FEATURE: Kerberos (GSSAPI) authentication -- realm and instance setup"
  # Leftovers from a run that was killed mid-way: clearing them here (rather
  # than in a trap) is what makes a failed run self-healing, and it leaves the
  # containers around for inspection when something did break.
  kerberos_cleanup

  rm -rf "$KERBEROS_SHARED_DIR"
  mkdir -p "$KERBEROS_SHARED_DIR"
  docker network create "$KERBEROS_NETWORK" >/dev/null

  echo "SUBFEATURE: bringing up a throwaway KDC for realm $KERBEROS_REALM"
  docker run -d --name "$KERBEROS_KDC_CONTAINER" \
    --network "$KERBEROS_NETWORK" --network-alias "$KERBEROS_KDC_HOST" \
    -v "$KERBEROS_SHARED_DIR:/krb5" \
    -v "$KERBEROS_DIR:/kerberos:ro" \
    -e KRB5_REALM="$KERBEROS_REALM" \
    -e KRB5_KDC_HOST="$KERBEROS_KDC_HOST" \
    -e KRB5_SERVICE_PRINCIPAL="$KERBEROS_SERVICE_PRINCIPAL" \
    -e KRB5_CLIENT_PRINCIPAL="$KERBEROS_CLIENT_PRINCIPAL" \
    -e KRB5_CLIENT_PASSWORD="$KERBEROS_CLIENT_PASSWORD" \
    -e KRB5_SHARED_DIR=/krb5 \
    -e HOST_UID="$(id -u)" \
    -e HOST_GID="$(id -g)" \
    ubuntu:24.04 sleep infinity >/dev/null
  docker exec "$KERBEROS_KDC_CONTAINER" bash /kerberos/setup-kdc.sh

  # The role the principal maps onto has to exist before SSO is switched on,
  # and it cannot be created afterwards: --auth-module-mappings makes memgraph
  # access-controlled from the first boot (Auth::AccessControlled), so there is
  # no unauthenticated window left to create it in. Same order the docs
  # prescribe: create the roles, then enable the module.
  echo "SUBFEATURE: creating role $KERBEROS_ROLE before enabling SSO"
  docker volume create "$KERBEROS_DATA_VOLUME" >/dev/null
  docker run -d --name "$KERBEROS_BOOTSTRAP_CONTAINER" \
    -p "$KERBEROS_BOLT_PORT:7687" \
    -v "$KERBEROS_DATA_VOLUME:/var/lib/memgraph" \
    $MEMGRAPH_ENTERPRISE_DOCKER_ENVS \
    "$MEMGRAPH_DOCKERHUB_IMAGE" $MEMGRAPH_GENERAL_FLAGS >/dev/null
  wait_for_memgraph "$MEMGRAPH_DEFAULT_HOST" "$KERBEROS_BOLT_PORT"
  # GRANT ALL PRIVILEGES covers the system privileges but NOT the fine-grained
  # label permissions, which start out granted to nobody -- without the second
  # grant the session authenticates and then fails any write with "missing
  # CREATE permission on labels". CREATE + READ is what the login check below
  # exercises.
  echo "CREATE ROLE $KERBEROS_ROLE;
        GRANT ALL PRIVILEGES TO $KERBEROS_ROLE;
        GRANT CREATE, READ ON NODES CONTAINING LABELS * TO $KERBEROS_ROLE;
        SHOW ROLES;" \
    | $MEMGRAPH_CONSOLE_BINARY --host "$MEMGRAPH_DEFAULT_HOST" --port "$KERBEROS_BOLT_PORT"
  # SIGTERM, not SIGKILL: let the auth storage close cleanly before the next
  # container opens it.
  docker stop -t 30 "$KERBEROS_BOOTSTRAP_CONTAINER" >/dev/null
  docker rm -f "$KERBEROS_BOOTSTRAP_CONTAINER" >/dev/null

  echo "SUBFEATURE: starting memgraph with --auth-module-mappings=kerberos"
  # The keytab and krb5.conf are the two files the acceptor side needs; the
  # MEMGRAPH_SSO_KERBEROS_* variables are read by the reference module itself
  # (src/auth/reference_modules/kerberos.py). role_mapping_mode=principal maps
  # the Kerberos principal straight onto a memgraph role, which keeps LDAP out
  # of the picture.
  # No published port: the client reaches Bolt over the test network, and
  # readiness is read from the log, so nothing here needs a host port -- one
  # less thing to collide with the bootstrap container that just released it.
  docker run -d --name "$KERBEROS_MG_CONTAINER" \
    --network "$KERBEROS_NETWORK" --network-alias "$KERBEROS_MG_HOST" \
    -v "$KERBEROS_DATA_VOLUME:/var/lib/memgraph" \
    -v "$KERBEROS_SHARED_DIR/memgraph.keytab:/etc/memgraph/memgraph.keytab:ro" \
    -v "$KERBEROS_SHARED_DIR/krb5.conf:/etc/krb5.conf:ro" \
    $MEMGRAPH_ENTERPRISE_DOCKER_ENVS \
    -e MEMGRAPH_SSO_KERBEROS_KEYTAB=/etc/memgraph/memgraph.keytab \
    -e MEMGRAPH_SSO_KERBEROS_SERVICE_PRINCIPAL="$KERBEROS_SERVICE_PRINCIPAL" \
    -e MEMGRAPH_SSO_KERBEROS_REALM="$KERBEROS_REALM" \
    -e MEMGRAPH_SSO_KERBEROS_ROLE_MAPPING_MODE=principal \
    -e MEMGRAPH_SSO_KERBEROS_ROLE_MAPPING="$KERBEROS_CLIENT_USER:$KERBEROS_ROLE" \
    "$MEMGRAPH_DOCKERHUB_IMAGE" \
    $MEMGRAPH_GENERAL_FLAGS --auth-module-mappings=kerberos >/dev/null

  kerberos_wait_for_bolt "$KERBEROS_MG_CONTAINER"
}

test_kerberos_auth() {
  echo "FEATURE: Kerberos (GSSAPI) authentication -- login with a service ticket"
  # The client runs in the KDC container so it reaches both the KDC and
  # memgraph over the test network, by the same names the principals use.
  # `if !` keeps the failure in hand just long enough to dump the logs that
  # explain it; everything else in this file fails through errexit.
  if ! docker exec \
    -e MEMGRAPH_URI="bolt://${KERBEROS_MG_HOST}:7687" \
    -e KRB5_CLIENT_PRINCIPAL="$KERBEROS_CLIENT_PRINCIPAL" \
    -e KRB5_CLIENT_PASSWORD="$KERBEROS_CLIENT_PASSWORD" \
    -e KRB5_SERVICE_PRINCIPAL="$KERBEROS_SERVICE_PRINCIPAL" \
    -e KRB5_EXPECTED_USER="$KERBEROS_CLIENT_USER" \
    -e KRB5_EXPECTED_ROLE="$KERBEROS_ROLE" \
    "$KERBEROS_KDC_CONTAINER" python3 -u /kerberos/client.py; then
    echo "FEATURE FAILED: Kerberos (GSSAPI) authentication -- container logs follow"
    kerberos_dump_logs
    return 1
  fi

  kerberos_cleanup
}

if [ "${BASH_SOURCE[0]}" -ef "$0" ]; then
  set -e # To make sure the script will return non-0 in case of a failure.
  # No EXIT trap: on success test_kerberos_auth cleans up, and on failure the
  # containers are worth keeping around to poke at. The next run starts by
  # clearing them.
  test_kerberos_auth_setup
  test_kerberos_auth
fi
