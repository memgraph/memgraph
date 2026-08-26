#!/bin/bash
# Turns a bare ubuntu container into a throwaway MIT Kerberos realm for
# tests/smoke/features/kerberos_auth.bash. The same container doubles as the
# Bolt client host (client.py), so it also gets the client-side krb5 config,
# python3-gssapi and the neo4j driver.
#
# Everything here is disposable — the realm, the KDC master key and the
# principal passwords live and die with the container — so they are passed in
# and written down in plain sight rather than pretending to be secrets.
#
# Reads from the environment (set by the feature script, one source of truth):
#   KRB5_REALM, KRB5_KDC_HOST, KRB5_SERVICE_PRINCIPAL,
#   KRB5_CLIENT_PRINCIPAL, KRB5_CLIENT_PASSWORD, KRB5_SHARED_DIR,
#   HOST_UID, HOST_GID

set -euo pipefail

: "${KRB5_REALM:?}"
: "${KRB5_KDC_HOST:?}"
: "${KRB5_SERVICE_PRINCIPAL:?}"
: "${KRB5_CLIENT_PRINCIPAL:?}"
: "${KRB5_CLIENT_PASSWORD:?}"
: "${KRB5_SHARED_DIR:?}"

# A flaky apt or PyPI mirror is the one failure in here that says nothing about
# memgraph, so give every network step three tries before failing the test.
retry() {
  local attempt
  for attempt in 1 2 3; do
    (( attempt == 1 )) || sleep $(( attempt * 5 ))
    "$@" && return 0
    echo "attempt $attempt/3 failed: $*" >&2
  done
  return 1
}

export DEBIAN_FRONTEND=noninteractive
retry apt-get update -qq
retry apt-get install -y -qq --no-install-recommends \
  krb5-kdc krb5-admin-server krb5-user python3-gssapi python3-pip
retry pip3 install --quiet --no-cache-dir --break-system-packages neo4j==5.23

# One krb5.conf for both sides: written into the shared dir so the memgraph
# container can mount the very same file.
cat > "$KRB5_SHARED_DIR/krb5.conf" <<EOF
[libdefaults]
    default_realm = $KRB5_REALM
    dns_lookup_realm = false
    dns_lookup_kdc = false
    # Docker's embedded DNS publishes no PTR records for network aliases, so
    # host canonicalization would fail to resolve $KRB5_KDC_HOST to a realm.
    rdns = false
    dns_canonicalize_hostname = false
    # Go straight to TCP; no reason to pay a UDP truncation retry in here.
    udp_preference_limit = 1

[realms]
    $KRB5_REALM = {
        kdc = $KRB5_KDC_HOST
        admin_server = $KRB5_KDC_HOST
    }

[logging]
    # Only the kdc key, which nothing but the krb5kdc daemon reads: the
    # memgraph container mounts this same file and must not be told to write
    # logs into a path it has no business writing to. The feature script dumps
    # this file when the test fails.
    kdc = FILE:/var/log/krb5kdc.log
EOF
cp "$KRB5_SHARED_DIR/krb5.conf" /etc/krb5.conf

# -s stashes the master key on disk so krb5kdc can start without a prompt.
kdb5_util create -s -r "$KRB5_REALM" -P throwaway-master-key
# The client logs in with a password; the service uses a random key we export
# to the keytab that memgraph's auth module accepts tickets with.
kadmin.local -q "addprinc -pw $KRB5_CLIENT_PASSWORD $KRB5_CLIENT_PRINCIPAL"
kadmin.local -q "addprinc -randkey $KRB5_SERVICE_PRINCIPAL"
kadmin.local -q "ktadd -k $KRB5_SHARED_DIR/memgraph.keytab $KRB5_SERVICE_PRINCIPAL"

# ktadd writes 0600 root:root. The keytab is read by the memgraph user (uid
# 101) inside a different container, and the whole directory is deleted by the
# unprivileged CI user afterwards, so widen the mode and hand ownership back.
chmod 644 "$KRB5_SHARED_DIR/memgraph.keytab"
chown -R "${HOST_UID:-0}:${HOST_GID:-0}" "$KRB5_SHARED_DIR"

krb5kdc  # daemonizes
echo "KDC ready: realm $KRB5_REALM, service $KRB5_SERVICE_PRINCIPAL"
