# OIDC SSL Certificate Testing

Tests OIDC authentication with self-signed certificates using `MEMGRAPH_SSO_*_OIDC_EXTRA_CA_CERTS` environment variables.

## Quick Start

```bash
source tests/ve3/bin/activate
pytest tests/e2e/sso/ssl/test_oidc_ssl.py -v
```

## What It Tests

OIDC makes HTTPS requests to fetch JWKS from identity providers. With self-signed certs:
- Without CA cert (cert=True): SSL verification fails (secure default)
- With CA cert (cert=/path/to/ca.pem): SSL verification succeeds
- Verification disabled (cert=False): SSL verification bypassed (insecure, testing only)

## Environment Variables

Custom OIDC provider uses:
- `MEMGRAPH_SSO_CUSTOM_OIDC_EXTRA_CA_CERTS`

```bash
export MEMGRAPH_SSO_CUSTOM_OIDC_EXTRA_CA_CERTS=/path/to/ca-bundle.crt
```

Note: Entra ID (`MEMGRAPH_SSO_ENTRA_ID_OIDC_EXTRA_CA_CERTS`) and Okta (`MEMGRAPH_SSO_OKTA_OIDC_EXTRA_CA_CERTS`) also support custom CA certs via their respective environment variables.

## Test Results

```
PASSED test_oidc_custom_without_ca_cert_fails
PASSED test_oidc_custom_with_ca_cert_succeeds
PASSED test_oidc_custom_with_verification_disabled
```
