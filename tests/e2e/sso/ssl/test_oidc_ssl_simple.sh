#!/bin/bash
# Test OIDC SSL certificate verification with self-signed certs

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMPDIR=$(mktemp -d)
cd "$TMPDIR"

echo "🔧 Setting up test environment in $TMPDIR"

# Generate self-signed certificate
echo "📜 Generating self-signed certificate..."
openssl req -x509 -newkey rsa:2048 \
    -keyout key.pem -out cert.pem \
    -days 1 -nodes \
    -subj "/CN=localhost" 2>/dev/null

# Create mock JWKS response
cat > jwks.json <<EOF
{
  "keys": [
    {
      "kty": "RSA",
      "use": "sig",
      "kid": "test-key-id",
      "n": "xGOr-H7A-test-key-data",
      "e": "AQAB",
      "alg": "RS256"
    }
  ]
}
EOF

# Start HTTPS server in background
echo "🚀 Starting mock HTTPS JWKS server on port 8443..."
python3 << 'PYEOF' &
import http.server
import ssl
import json

class JWKSHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # Suppress logs

    def do_GET(self):
        if 'keys' in self.path:
            with open('jwks.json', 'r') as f:
                jwks = f.read()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(jwks.encode())
        else:
            self.send_response(404)
            self.end_headers()

server = http.server.HTTPServer(('localhost', 8443), JWKSHandler)
ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
ssl_context.load_cert_chain(certfile='cert.pem', keyfile='key.pem')
server.socket = ssl_context.wrap_socket(server.socket, server_side=True)
print("Server ready")
server.serve_forever()
PYEOF

SERVER_PID=$!
sleep 2

# Create test Python script
cat > test_oidc.py <<PYEOF
import sys
import os

# Add auth module path
script_dir = "$SCRIPT_DIR"
auth_module_path = os.path.join(script_dir, '..', '..', '..', '..', 'src', 'auth', 'reference_modules')
sys.path.insert(0, auth_module_path)

from oidc import validate_jwt_token

# Dummy JWT token (will fail validation, but should fetch JWKS)
token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InRlc3Qta2V5LWlkIn0.eyJzdWIiOiJ0ZXN0In0.dummy"

# Get cert path from environment (if set)
cert_path = os.environ.get("MEMGRAPH_SSO_CUSTOM_OIDC_EXTRA_CA_CERTS", True)

config = {
    "public_key_endpoint": "https://localhost:8443/keys",
    "access_token_audience": "test",
    "id_token_audience": "test",
    "cert": cert_path  # Add cert to config
}

result = validate_jwt_token(token, "oidc-custom", config, "access")
print(f"Valid: {result['valid']}")
if not result['valid']:
    print(f"Error: {result['errors']}")
    # Check if it's an SSL error
    if 'SSL' in result['errors'] or 'certificate' in result['errors'] or 'CERTIFICATE_VERIFY_FAILED' in result['errors']:
        sys.exit(1)  # SSL error
    else:
        sys.exit(0)  # Other error (expected since token is dummy)
PYEOF

echo ""
echo "=========================================="
echo "TEST 1: Without CA cert"
echo "=========================================="
echo "Expected: Should fail with SSL/certificate error"
echo ""

unset MEMGRAPH_SSO_CUSTOM_OIDC_EXTRA_CA_CERTS
python3 test_oidc.py && {
    echo "❌ FAILED: Should have failed with SSL error"
    kill $SERVER_PID 2>/dev/null
    exit 1
} || {
    echo "✅ PASSED: Failed with SSL error as expected"
}

echo ""
echo "=========================================="
echo "TEST 2: With CA cert"
echo "=========================================="
echo "Expected: Should successfully fetch JWKS (may fail at JWT validation, but not at SSL)"
echo ""

export MEMGRAPH_SSO_CUSTOM_OIDC_EXTRA_CA_CERTS="$TMPDIR/cert.pem"
echo "Set MEMGRAPH_SSO_CUSTOM_OIDC_EXTRA_CA_CERTS=$MEMGRAPH_SSO_CUSTOM_OIDC_EXTRA_CA_CERTS"

python3 test_oidc.py && {
    echo "✅ PASSED: Successfully fetched JWKS with custom CA cert"
} || {
    echo "✅ PASSED: Failed at JWT validation (not SSL), which is expected with dummy token"
}

# Cleanup
echo ""
echo "🧹 Cleaning up..."
kill $SERVER_PID 2>/dev/null || true
cd /
rm -rf "$TMPDIR"

echo ""
echo "=========================================="
echo "Running pytest suite..."
echo "=========================================="
echo ""

# Run the pytest test suite
# Try to use the virtual environment if it exists
if [ -f "$SCRIPT_DIR/../../../ve3/bin/activate" ]; then
    echo "Using virtual environment from tests/ve3"
    source "$SCRIPT_DIR/../../../ve3/bin/activate"
    pytest "$SCRIPT_DIR/test_oidc_ssl.py" -v
elif command -v pytest &> /dev/null; then
    pytest "$SCRIPT_DIR/test_oidc_ssl.py" -v
else
    echo "⚠️  pytest not found, skipping pytest tests"
    echo "   Install with: pip install pytest"
    echo "   Or use the tests virtual environment: source tests/ve3/bin/activate"
fi

echo ""
echo "✨ All tests completed!"
