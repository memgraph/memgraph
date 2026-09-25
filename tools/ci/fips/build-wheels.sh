#!/bin/bash
# Rebuild the Python wheels whose PyPI build statically links its own copy of
# OpenSSL, so they use the system one instead. In an image running in FIPS
# approved mode a bundled OpenSSL is a second, unvalidated cryptographic module
# on the auth path, and it does not just fail to comply - it breaks, because it
# looks for fips.so in a MODULESDIR baked in at wheel-build time.
#
#   cryptography  bundles OpenSSL 4.0.1 (symbols hidden; only a .rodata banner
#                 shows it). Used for OIDC RS256 verification.
#   xmlsec        bundles OpenSSL 3.5.1 and exports its symbols. Does all SAML
#                 signature verification.
#   lxml          no OpenSSL of its own, but xmlsec refuses to import unless
#                 lxml's libxml2 major.minor matches its own, and the PyPI lxml
#                 bundles 2.14 against Ubuntu 24.04's 2.9.
#
# gssapi is NOT in the default set and is not in the FIPS image: Ubuntu's krb5
# is built with its own builtin crypto (libk5crypto3 imports no OpenSSL symbols
# at all), so Kerberos would run outside the validated module however the wheel
# is linked. MG_FIPS drops kerberos.py and its gssapi pin instead. Passing
# gssapi==<ver> explicitly still works and is audited - that is how to check a
# krb5 rebuilt with --with-crypto-impl=openssl, which is the way back in.
#
# No auditwheel: its job is to vendor external libraries into the wheel, which
# is the thing being removed here.

set -euo pipefail

function print_help() {
    echo "Usage: $0 [--output-dir <dir>] [--python <python>] [--no-audit] [<pkg>==<ver> ...]"
    exit 1
}

OUTPUT_DIR="$PWD/build/fips-wheels"
PYTHON="python3"
AUDIT=true
PACKAGES=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --output-dir) OUTPUT_DIR=$2; shift 2 ;;
        --python)     PYTHON=$2; shift 2 ;;
        --no-audit)   AUDIT=false; shift ;;
        -h|--help)    print_help ;;
        -*)           print_help ;;
        *)            PACKAGES+=("$1"); shift ;;
    esac
done
# Keep in sync with src/auth/reference_modules/requirements.txt.
[[ ${#PACKAGES[@]} -gt 0 ]] || PACKAGES=(cryptography==50.0.0 xmlsec==1.3.16 lxml==6.1.0)

# Fail before building rather than producing a wheel with the wrong linkage.
# Only require what the requested packages actually need.
has() { printf '%s\n' "${PACKAGES[@]}" | grep -q "^$1"; }

APT_HINT="apt install libssl-dev libxml2-dev libxslt1-dev libxmlsec1-dev libxmlsec1t64-openssl libkrb5-dev pkg-config"
command -v pkg-config >/dev/null || { echo "pkg-config not found" >&2; echo "$APT_HINT" >&2; exit 1; }
NEED=""
has cryptography && NEED+=" openssl"
has xmlsec       && NEED+=" openssl xmlsec1 libxml-2.0"
has lxml         && NEED+=" libxml-2.0"
has gssapi       && NEED+=" krb5-gssapi"
MISSING=()
for mod in $(echo "$NEED" | tr ' ' '\n' | sort -u); do
    pkg-config --exists "$mod" 2>/dev/null || MISSING+=("$mod")
done
if [[ ${#MISSING[@]} -gt 0 ]]; then
    echo "Missing pkg-config modules: ${MISSING[*]}" >&2
    echo "$APT_HINT" >&2
    exit 1
fi


if has cryptography; then
    CARGO_ENV="${CARGO_HOME:-${HOME:-/root}/.cargo}/env"
    [[ -r "$CARGO_ENV" ]] && . "$CARGO_ENV"
    CARGO_VER="$(cargo --version 2>/dev/null | awk '{print $2}')" || true
    if [[ -z "$CARGO_VER" ]] || [[ "$(printf '%s\n1.83.0\n' "$CARGO_VER" | sort -V | head -1)" != "1.83.0" ]]; then
        echo "cargo >= 1.83.0 required for cryptography, found ${CARGO_VER:-none} (looked in PATH and $CARGO_ENV)" >&2
        echo "curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.83.0" >&2
        exit 1
    fi
fi

# Truthiness of the raw value is what setup.py tests, so =false would still be
# true. Unset it or xmlsec downloads and statically links its own OpenSSL.
unset PYXMLSEC_STATIC_DEPS

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# Only report the libraries this run actually needs - ensure-wheels.sh asks for
# just the subset that is missing, so naming all of them unconditionally prints
# pkg-config errors for ones that are legitimately absent.
BANNER="Building with $($PYTHON --version)"
for mod in $(echo "$NEED" | tr ' ' '\n' | sort -u); do
    BANNER+=" | $mod $(pkg-config --modversion "$mod")"
done
echo "$BANNER"
"$PYTHON" -m venv "$WORK/env"
"$WORK/env/bin/pip" install --quiet --upgrade pip wheel setuptools

mkdir -p "$OUTPUT_DIR"
for spec in "${PACKAGES[@]}"; do
    # --no-binary is per-package so build deps (cython, cffi, maturin) still
    # come from their own wheels.
    "$WORK/env/bin/pip" wheel --no-deps --no-cache-dir \
        --no-binary="${spec%%[=<>!~]*}" "$spec" -w "$OUTPUT_DIR"
done

if [[ "$AUDIT" != "true" ]]; then
    ls -1 "$OUTPUT_DIR"
    exit 0
fi

# Four checks, because the two offenders hide their bundled OpenSSL differently:
# exported symbols catch xmlsec (no version banner at all), the .rodata banner
# catches cryptography (no exported symbols). Dropping either lets one through.
# NB: no `grep -q` or `head` in any of these pipelines. Under `set -o pipefail`
# they exit early, SIGPIPE the producer, and the pipeline then reports 141 - so
# `if nm ... | grep -q ...` silently evaluates FALSE and a bundled OpenSSL sails
# through. Counting greps read all their input, so they are safe.
for t in nm readelf strings unzip; do
    command -v "$t" >/dev/null || { echo "$t not found (apt install binutils unzip)" >&2; exit 1; }
done
SYS_SSL="$(openssl version | awk '{print $2}')"
rc=0
for wheel in "$OUTPUT_DIR"/*.whl; do
    ext="$WORK/$(basename "$wheel" .whl)"
    unzip -qo "$wheel" -d "$ext"
    vendored="$(find "$ext" \( -name 'lib*ssl*.so*' -o -name 'lib*crypto*.so*' \) | wc -l)"
    [ "$vendored" -eq 0 ] || { echo "FAIL $(basename "$wheel"): vendors OpenSSL"; rc=1; }
    while read -r so; do
        [ -n "$so" ] || continue
        rel="${so#"$ext"/}"
        exported="$(nm -D --defined-only "$so" 2>/dev/null | grep -cE ' T (EVP_|SSL_|OSSL_)' || true)"
        banner="$(strings -a "$so" | grep -oE 'OpenSSL [0-9]+\.[0-9]+\.[0-9]+' | sort -u | sed -n 1p || true)"
        # The verdict turns on OpenSSL only; `links` is reported so the output
        # says what an extension actually binds to. xmlsec reaches libcrypto
        # through libxmlsec1-openssl rather than directly, and "no OpenSSL"
        # would be a misleading thing to print about it.
        needed="$(readelf -d "$so" | grep -oE 'lib(ssl|crypto)\.so[^]]*' | sort -u | tr '\n' ' ' || true)"
        links="$(readelf -d "$so" | grep -oE 'lib(ssl|crypto|xml2|xslt|exslt|xmlsec1[a-z-]*|gssapi_krb5|krb5[a-z]*|k5crypto|com_err)\.so[^]]*' | sort -u | tr '\n' ' ' || true)"
        # DT_NEEDED first: a dynamically linked extension still carries the
        # version string it was compiled against, so the banner only means a
        # static link when there is no DT_NEEDED to explain it. cryptography
        # built from source is exactly that case - libcrypto.so.3 in DT_NEEDED
        # and "OpenSSL 3.0.13" in .rodata.
        if [ -n "$needed" ]; then
            echo "ok   $rel -> $links"
        elif [ "${exported:-0}" -gt 0 ]; then
            echo "FAIL $rel: statically links OpenSSL ($exported exported symbols)"; rc=1
        elif [ -n "$banner" ]; then
            echo "FAIL $rel: statically links $banner (hidden symbols)"; rc=1
        else
            echo "ok   $rel${links:+ -> $links}"
        fi
    done < <(find "$ext" \( -name '*.so' -o -name '*.so.*' \))
done

# End to end. --no-deps is what guarantees the modules under test are ours and
# not PyPI's; cffi and decorator have no OpenSSL of their own so they can come
# from the index.
"$WORK/env/bin/pip" install --quiet --no-index --no-deps "$OUTPUT_DIR"/*.whl
"$WORK/env/bin/pip" install --quiet cffi decorator >/dev/null 2>&1 || true
if reported="$("$WORK/env/bin/python" -c 'from cryptography.hazmat.backends.openssl.backend import backend; print(backend.openssl_version_text())' 2>/dev/null)"; then
    [[ "$(echo "$reported" | awk '{print $2}')" = "$SYS_SSL" ]] \
        && echo "ok   cryptography uses the system OpenSSL $SYS_SSL" \
        || { echo "FAIL cryptography reports '$reported', system is $SYS_SSL"; rc=1; }
fi
# xmlsec raises "lxml & xmlsec libxml2 library version mismatch" from its module
# init, so importing it is the whole lxml pairing test.
if ls "$OUTPUT_DIR"/xmlsec-*.whl >/dev/null 2>&1; then
    "$WORK/env/bin/python" -c 'import xmlsec' 2>/dev/null \
        && echo "ok   import xmlsec (agrees with lxml on libxml2)" \
        || { echo "FAIL import xmlsec: $("$WORK/env/bin/python" -c 'import xmlsec' 2>&1 | tail -1)"; rc=1; }
fi
# gssapi reaches its crypto through libgssapi_krb5, so no extension names an
# OpenSSL in DT_NEEDED either way and the per-file verdicts are "ok" whichever
# wheel this is. The checks above still catch the published wheels by the copy
# they vendor, but they say nothing about a build that links an OpenSSL from
# outside the wheel. Importing gssapi and reading the process map covers both:
# it is the only check here that reports what actually gets loaded.
if ls "$OUTPUT_DIR"/gssapi-*.whl >/dev/null 2>&1; then
    if mapped="$("$WORK/env/bin/python" -c '
import gssapi, gssapi.raw
print(" ".join(sorted({l.rsplit(" ",1)[-1].strip() for l in open("/proc/self/maps")
                       if "libcrypto" in l or "libssl" in l})))' 2>&1)"; then
        [ -z "$mapped" ] \
            && echo "ok   import gssapi (maps no OpenSSL)" \
            || { echo "FAIL gssapi pulls in OpenSSL: $mapped"; rc=1; }
    else
        echo "FAIL import gssapi: $(echo "$mapped" | tail -1)"; rc=1
    fi
fi

[[ $rc -eq 0 ]] || { echo "Audit failed - not publishing these wheels" >&2; exit 1; }
echo
ls -1 "$OUTPUT_DIR"
