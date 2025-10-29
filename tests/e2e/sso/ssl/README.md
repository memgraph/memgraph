# OIDC SSL Certificate Testing

Tests OIDC authentication with self-signed certificates using `MEMGRAPH_SSO_*_OIDC_EXTRA_CA_CERTS` environment variables.

## Quick Start

```bash
cd tests/e2e/sso/ssl
./test_oidc_ssl_simple.sh
```

Runs bash tests + pytest suite (4 tests, auto uses `tests/ve3` virtualenv).

## What It Tests

OIDC makes HTTPS requests to fetch JWKS from identity providers. With self-signed certs:
- ❌ Without CA cert → SSL verification fails (secure default)
- ✅ With CA cert → SSL verification succeeds

## Environment Variables

Per-scheme variables:
- `MEMGRAPH_SSO_ENTRA_ID_OIDC_EXTRA_CA_CERTS`
- `MEMGRAPH_SSO_OKTA_OIDC_EXTRA_CA_CERTS`
- `MEMGRAPH_SSO_CUSTOM_OIDC_EXTRA_CA_CERTS`

```bash
export MEMGRAPH_SSO_CUSTOM_OIDC_EXTRA_CA_CERTS=/path/to/ca-bundle.crt
```

## Test Results

```
✅ test_oidc_custom_without_ca_cert_fails
✅ test_oidc_custom_with_ca_cert_succeeds
✅ test_oidc_entra_without_ca_cert_fails
✅ test_oidc_okta_with_ca_cert_succeeds
```

## Manual Testing

```bash
export MEMGRAPH_SSO_CUSTOM_OIDC_EXTRA_CA_CERTS=/path/to/ca.crt
./memgraph --auth-module-path=/path/to/oidc.py
```

## Troubleshooting

**OpenSSL not found:**
```bash
sudo apt-get install openssl
```

**Port 8443 in use:**
```bash
lsof -ti:8443 | xargs kill -9
```

**Hostname mismatch warning:** Expected with test certs. Means SSL handshake succeeded but hostname validation failed (separate check). Production certs with proper SANs won't have this issue.

## Files

- `test_oidc_ssl_simple.sh` - Main test runner (bash + pytest)
- `test_oidc_ssl.py` - Pytest suite (4 test cases)
