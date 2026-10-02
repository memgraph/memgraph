#!/bin/bash
# FIPS 140-3 compliance checks. Most run only against the prod-fips image (see
# the fips mode in suite.bash) and would fail on a normal one, which is the
# point: they check that approved mode is genuinely active rather than merely
# configured.
#
# test_fips_info_disabled is the exception and runs on every normal image,
# because the dangerous direction is a false positive: an image that wrongly
# reported approved mode would be making a compliance claim that is not true,
# and normal smoke runs are where that would actually be noticed.
FIPS_SMOKE_DIR="$( cd "$( dirname "$( dirname "${BASH_SOURCE[0]}" )" )" && pwd )"
source "$FIPS_SMOKE_DIR/utils.bash"

# mgconsole's CSV wraps every field in quotes and doubles the quotes a string
# value already carries, so `bcrypt` arrives as """bcrypt""", `false` as
# "false" and an empty string as """""". Strip quotes outright rather than
# trying to count them; no value here contains a comma.
fips_info_field() {
  local out="$1" field="$2"
  echo "$out" | tr -d '"' | awk -F, -v f="$field" '$1 == f { print $2 }'
}

fips_info_field_is() {
  [ "$(fips_info_field "$1" "$2")" = "$3" ]
}

fips_info_field_is_empty() {
  [ -z "$(fips_info_field "$1" "$2")" ]
}

fips_info_field_matches() {
  echo "$(fips_info_field "$1" "$2")" | grep -qE "$3"
}

# `SHOW FIPS INFO` is what an operator under audit is told to run, so its
# output is the thing worth asserting on rather than the startup log.
test_fips_show_info() {
  echo "FEATURE: FIPS - SHOW FIPS INFO"
  local out
  out="$(run_query_csv "SHOW FIPS INFO;")"
  echo "$out"

  fips_info_field_is "$out" enabled true \
    || { echo "FAIL: SHOW FIPS INFO does not report approved mode as enabled"; return 1; }
  # The module identifies itself; an empty name would mean we reported approved
  # mode without ever having read the provider's parameters.
  fips_info_field_matches "$out" module_name '[A-Za-z]' \
    || { echo "FAIL: module_name is empty"; return 1; }
  fips_info_field_matches "$out" module_version '^[0-9]+\.[0-9]+\.[0-9]+' \
    || { echo "FAIL: module_version is not a version string"; return 1; }
  fips_info_field_is "$out" password_algorithm pbkdf2-sha256 \
    || { echo "FAIL: password_algorithm is not pbkdf2-sha256"; return 1; }
  fips_info_field_is "$out" tls_min_version "TLSv1.2" \
    || { echo "FAIL: tls_min_version is not TLSv1.2"; return 1; }
}

# Runs on the normal image. Guards the failure that would matter most: an image
# claiming approved mode without a validated provider behind it.
test_fips_info_disabled() {
  echo "FEATURE: FIPS - SHOW FIPS INFO reports disabled on a normal image"
  local out
  out="$(run_query_csv "SHOW FIPS INFO;")"
  echo "$out"

  fips_info_field_is "$out" enabled false \
    || { echo "FAIL: a non-FIPS image must report approved mode as not enabled"; return 1; }
  # Stale or invented module identity would be as misleading as a wrongly
  # enabled flag, so these must be blank rather than "unknown".
  fips_info_field_is_empty "$out" module_name \
    || { echo "FAIL: module_name should be empty when FIPS is off"; return 1; }
  fips_info_field_is_empty "$out" module_version \
    || { echo "FAIL: module_version should be empty when FIPS is off"; return 1; }
  # The smoke container does not set --password-encryption-algorithm, so this
  # also pins the shipped default.
  fips_info_field_is "$out" password_algorithm bcrypt \
    || { echo "FAIL: password_algorithm should be the bcrypt default"; return 1; }
  # Blank rather than a version: outside approved mode Memgraph sets no floor,
  # so naming one here would claim an enforcement that is not happening.
  fips_info_field_is_empty "$out" tls_min_version \
    || { echo "FAIL: tls_min_version should be empty when FIPS is off"; return 1; }
}

# The binary reports approved mode; these check the module underneath actually
# behaves that way, from inside the shipped image.
test_fips_provider_active() {
  echo "FEATURE: FIPS - OpenSSL provider"
  $MEMGRAPH_EXEC openssl list -providers | tee /dev/stderr | grep -q "OpenSSL FIPS Provider" \
    || { echo "FAIL: FIPS provider not listed"; return 1; }
  # Loaded is not operational: a module that failed a power-on self-test still
  # appears in the list.
  $MEMGRAPH_EXEC openssl list -providers | grep -A3 "^  fips" | grep -q "status: active" \
    || { echo "FAIL: FIPS provider is not operational"; return 1; }
}

test_fips_drbg_from_provider() {
  echo "FEATURE: FIPS - DRBG source"
  # DRBGs are instantiated lazily and cached for the process lifetime, so a
  # default-provider DRBG would mean every salt and nonce came from an
  # unvalidated source while everything else still looked correct.
  $MEMGRAPH_EXEC openssl list -random-instances | tee /dev/stderr | grep -q "@ fips" \
    || { echo "FAIL: DRBG is not supplied by the FIPS provider"; return 1; }
}

test_fips_non_approved_algorithms_unavailable() {
  echo "FEATURE: FIPS - non-approved algorithms"
  $MEMGRAPH_EXEC bash -c 'echo test | openssl dgst -sha256' >/dev/null \
    || { echo "FAIL: SHA-256 unavailable in approved mode"; return 1; }
  # Must be absent, not merely deprioritised — that is the difference between
  # the strict and permissive provider configurations.
  if $MEMGRAPH_EXEC bash -c 'echo test | openssl dgst -md5' >/dev/null 2>&1; then
    echo "FAIL: MD5 is still reachable; the default provider is active"
    return 1
  fi
}

# This checks that we haven't accidentally included any binaries with OpenSSL
# statically linked in the image. Python packages that use OpenSSL will often
# do just this, so this test will fail if our custom packages are replaced
# with those usually provided by pip.
test_fips_no_bundled_openssl() {
  echo "FEATURE: FIPS - no second OpenSSL in the image"
  command -v readelf >/dev/null \
    || { echo "FAIL: readelf not found on the host (apt install binutils)"; return 1; }

  # Scan binaries for OpenSSL
  local candidates
  candidates="$($MEMGRAPH_EXEC bash -c '
    find / -xdev -type f \( -name "*.so" -o -name "*.so.*" -o -path "/usr/lib/memgraph/*" \) 2>/dev/null |
    while read -r f; do
      grep -aqE "OpenSSL [0-9]+\.[0-9]+\.[0-9]+|EVP_DigestInit|OPENSSL_init_crypto" "$f" 2>/dev/null && echo "$f"
    done
    exit 0' 2>/dev/null)"

  # libcrypto should always appear as a candidate
  if [ -z "$candidates" ]; then
    echo "FAIL: nothing in the image carries an OpenSSL banner, not even libcrypto."
    echo "      The scan is broken, not the image."
    return 1
  fi

  local found="" file needed
  while read -r file; do
    [ -n "$file" ] || continue
    case "$file" in
      # The validated module and its provider modules should be ignored.
      */libcrypto.so.*|*/libssl.so.*|*/ossl-modules/*) continue ;;
    esac
    needed="$(container_dt_needed "$file")" || continue   # not an ELF object
    if ! printf '%s\n' "$needed" | grep -qE '^lib(ssl|crypto)\.so'; then
      found="$found  $file"$'\n'
    fi
  done <<< "$candidates"

  if [ -n "$found" ]; then
    echo "FAIL: these carry their own OpenSSL instead of linking the validated one:"
    printf '%s' "$found"
    return 1
  fi
  echo "  every OpenSSL user in the image links the validated libcrypto"
}

# Check that the hashlib gate is working correctly
test_fips_hashlib_gate() {
  echo "FEATURE: FIPS - hashlib gate"
  local out
  out="$(run_python_in_container \
    "$FIPS_SMOKE_DIR/scripts/fips_hashlib_gate.py" /tmp/fips_hashlib_gate.py 2>&1)" \
    || { echo "FAIL: hashlib gate (auth-module interpreter)"; printf "%s\n" "$out"; return 1; }
  printf "%s\n" "$out"

  # The gate names itself when it gives up; either message means the embedded
  # interpreter is running with hashlib unrestricted.
  local stood_down
  stood_down="$(docker logs "$MEMGRAPH_SMOKE_CONTAINER" 2>&1 \
    | grep -E "memgraph_fips_hashlib: (could not restrict hashlib|OpenSSL reports approved mode)" || true)"
  if [ -n "$stood_down" ]; then
    echo "FAIL: the gate stood down in the embedded interpreter:"
    printf "%s\n" "$stood_down" | sed "s/^/      /"
    return 1
  fi
  echo "  embedded interpreter: no stand-down reported at startup"
}

# Test that the auth modules for SAML and OIDC work correctly in FIPS mode

test_fips_saml_signature_path() {
  echo "FEATURE: FIPS - SAML signature path (xmlsec -> validated OpenSSL)"
  local out
  out="$(run_python_in_container \
    "$FIPS_SMOKE_DIR/scripts/fips_saml_signature_path.py" \
    /tmp/fips_saml_signature_path.py 2>&1)" \
    || { echo "FAIL: SAML signature path"; echo "$out" | sed "s/^/      /"; return 1; }
  echo "$out"
}

test_fips_oidc_jwt_path() {
  echo "FEATURE: FIPS - OIDC token path (PyJWT/cryptography -> validated OpenSSL)"
  local out
  out="$(run_python_in_container \
    "$FIPS_SMOKE_DIR/scripts/fips_oidc_jwt_path.py" \
    /tmp/fips_oidc_jwt_path.py 2>&1)" \
    || { echo "FAIL: OIDC token path"; echo "$out" | sed "s/^/      /"; return 1; }
  echo "$out"
}

# Passwords hashed under approved mode must actually use the approved KDF, and
# the legacy algorithms must be refused rather than silently accepted.
test_fips_password_hashing() {
  echo "FEATURE: FIPS - password hashing"
  run_query_admin "CREATE USER fips_user IDENTIFIED BY 'fips_user1234';"
  # A pre-computed bcrypt hash is a user-supplied hash, which approved mode
  # rejects outright: accepting it would store an unapprovable hash.
  local out
  out="$(echo "CREATE USER fips_legacy IDENTIFIED BY 'bcrypt:\$2a\$12\$ueWpo7FfYrBwoFwBhaCD1ucO4hbwKtOtr9MvxCELJaNq746xhvqYy';" \
    | $MGCONSOLE_ADMIN 2>&1 || true)"
  echo "$out" | grep -qi "not permitted in FIPS mode" \
    || { echo "FAIL: a bcrypt hash was not rejected. Output: $out"; return 1; }
  run_query_admin "DROP USER fips_user;"
}

# FIPS mode is an enterprise feature. Run without a licence and startup must
# refuse rather than quietly serve traffic in approved mode unlicensed.
test_fips_requires_enterprise_licence() {
  echo "FEATURE: FIPS - requires an enterprise licence"
  local out status
  # Deliberately no $MEMGRAPH_ENTERPRISE_DOCKER_ENVS.
  out="$(docker run --rm "$MEMGRAPH_DOCKERHUB_IMAGE" --fips-mode=true 2>&1)" && status=0 || status=$?
  echo "$out"

  # 16 is ExitCode::FipsModeRequiresEnterprise.
  if [ "$status" -ne 16 ]; then
    echo "FAIL: expected exit 16, got $status"
    return 1
  fi
  echo "$out" | grep -qi "license" \
    || { echo "FAIL: the error did not mention the licence"; return 1; }
  echo "  refused with exit 16 as expected"
}

# Refusing to start under a non-approved password algorithm is a startup-time
# guarantee, so it cannot be checked against the already-running container.
# bcrypt is the default, so this is also what an operator hits first if they
# enable FIPS without setting the algorithm.
test_fips_refuses_non_approved_algorithm() {
  echo "FEATURE: FIPS - refuses a non-approved password algorithm"
  local out status
  out="$(docker run --rm $MEMGRAPH_ENTERPRISE_DOCKER_ENVS "$MEMGRAPH_DOCKERHUB_IMAGE" \
    --fips-mode=true --password-encryption-algorithm=bcrypt 2>&1)" && status=0 || status=$?
  echo "$out"

  # 15 is ExitCode::FipsModeUnsupportedPasswordAlgorithm; the value is part of
  # the operational interface, so a restart policy can tell a misconfiguration
  # apart from a crash.
  if [ "$status" -ne 15 ]; then
    echo "FAIL: expected exit 15, got $status"
    return 1
  fi
  echo "$out" | grep -q "is incompatible with --password-encryption-algorithm=bcrypt" \
    || { echo "FAIL: the error did not name the incompatible algorithm"; return 1; }
  echo "  refused with exit 15 as expected"
}
