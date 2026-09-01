# Base image for the throwaway KDC in features/kerberos_auth.bash. The krb5
# stack and the client deps are baked in here so that looped smoke runs
# (--reuse-env) don't reinstall them per iteration; the realm itself is still
# created per run by setup-kdc.sh.
FROM ubuntu:24.04
ENV DEBIAN_FRONTEND=noninteractive
# Retry the network steps: a flaky mirror says nothing about memgraph.
RUN for attempt in 1 2 3 4 5; do \
      [ "$attempt" -eq 1 ] || sleep $((attempt * 5)); \
      apt-get update -qq && \
      apt-get install -y -qq --no-install-recommends \
        krb5-kdc krb5-admin-server krb5-user python3-gssapi python3-pip && \
      pip3 install --quiet --no-cache-dir --break-system-packages neo4j==5.23 && \
      exit 0; \
      echo "attempt $attempt/5 failed" >&2; \
    done; exit 1
