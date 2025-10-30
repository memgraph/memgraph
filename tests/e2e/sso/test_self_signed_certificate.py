import json
import os
import ssl
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest
from oidc import validate_jwt_token

# TODO: this is probably better way to import oidc
# sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent / "src" / "auth" / "reference_modules"))


class MockJWKSHandler(BaseHTTPRequestHandler):
    """Mock JWKS endpoint handler"""

    def log_message(self, format, *args):
        # Suppress server logs
        pass

    def do_GET(self):
        if self.path.endswith("/keys") or self.path.endswith("/v1/keys") or self.path.endswith("/discovery/v2.0/keys"):
            # Return a mock JWKS response
            jwks = {
                "keys": [
                    {
                        "kty": "RSA",
                        "use": "sig",
                        "kid": "test-key-id",
                        "n": "xGOr-H7A-PWLofZ8fLc8mHfVbP8FMN6qbYxWiHO9kR4m_8aY0dRjNj4gL9TYlH1NtqiUXg2r7z6N8gT9FUhzZ8F3V0HnJZ8vG3bQ5mYqD1gT8-Xr5_9c6eZ1pP8xJ5K6nLzO9FqL7R8VbTzg4H9J0-Q8K6L3V9n4Xm8R7YbP1_L9F8J6K3nV8m0Q9R7Y1",
                        "e": "AQAB",
                        "alg": "RS256",
                    }
                ]
            }
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(jwks).encode())
        else:
            self.send_response(404)
            self.end_headers()


def generate_self_signed_cert():
    """Generate a self-signed certificate for testing"""
    with tempfile.TemporaryDirectory() as tmpdir:
        cert_path = os.path.join(tmpdir, "cert.pem")
        key_path = os.path.join(tmpdir, "key.pem")

        # Generate self-signed certificate
        subprocess.run(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-keyout",
                key_path,
                "-out",
                cert_path,
                "-days",
                "1",
                "-nodes",
                "-subj",
                "/CN=localhost",
            ],
            check=True,
            capture_output=True,
        )

        # Read the cert and key
        with open(cert_path, "r") as f:
            cert_content = f.read()
        with open(key_path, "r") as f:
            key_content = f.read()

        return cert_content, key_content


class SecureHTTPServer:
    """HTTPS server with self-signed certificate"""

    def __init__(self, port=8443):
        self.port = port
        self.server = None
        self.thread = None
        self.cert_file = None
        self.key_file = None

    def start(self):
        # Create temporary files for cert and key
        cert_content, key_content = generate_self_signed_cert()

        self.cert_file = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".pem")
        self.cert_file.write(cert_content)
        self.cert_file.close()

        self.key_file = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".pem")
        self.key_file.write(key_content)
        self.key_file.close()

        # Create HTTPS server
        self.server = HTTPServer(("localhost", self.port), MockJWKSHandler)
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ssl_context.load_cert_chain(certfile=self.cert_file.name, keyfile=self.key_file.name)
        self.server.socket = ssl_context.wrap_socket(self.server.socket, server_side=True)

        # Start server in background thread
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

        # Wait for server to be ready
        time.sleep(0.5)

        return self.cert_file.name

    def stop(self):
        if self.server:
            self.server.shutdown()
            self.server.server_close()
        if self.cert_file:
            os.unlink(self.cert_file.name)
        if self.key_file:
            os.unlink(self.key_file.name)


@pytest.fixture
def https_server():
    """Fixture to create and cleanup HTTPS server"""
    server = SecureHTTPServer(port=8443)
    cert_path = server.start()
    yield cert_path
    server.stop()


def test_oidc_custom_without_ca_cert_fails(https_server):
    """Test that custom OIDC fails when connecting to self-signed cert without CA"""
    original_value = os.environ.pop("MEMGRAPH_SSO_CUSTOM_OIDC_EXTRA_CA_CERTS", None)

    try:
        # Create a dummy token (validation will fail at JWKS fetch, which is what we want)
        dummy_token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InRlc3Qta2V5LWlkIn0.eyJzdWIiOiJ0ZXN0LXVzZXIifQ.dummy"

        config = {
            "public_key_endpoint": "https://localhost:8443/keys",
            "access_token_audience": "test-audience",
            "id_token_audience": "test-audience",
            "cert": True,  # Use default verification (will fail with self-signed cert)
        }

        result = validate_jwt_token(dummy_token, "oidc-custom", config, "access")

        # Should fail with SSL error
        assert result["valid"] is False
        assert "Failed to fetch JWKS" in result["errors"]
        # The error should mention SSL/certificate issues
        assert any(keyword in result["errors"].lower() for keyword in ["ssl", "certificate", "verify"])

    finally:
        # Restore original value
        if original_value is not None:
            os.environ["MEMGRAPH_SSO_CUSTOM_OIDC_EXTRA_CA_CERTS"] = original_value


def test_oidc_custom_with_ca_cert_succeeds(https_server):
    """Test that custom OIDC succeeds when MEMGRAPH_SSO_CUSTOM_OIDC_EXTRA_CA_CERTS is set"""
    cert_path = https_server
    original_value = os.environ.get("MEMGRAPH_SSO_CUSTOM_OIDC_EXTRA_CA_CERTS")

    try:
        os.environ["MEMGRAPH_SSO_CUSTOM_OIDC_EXTRA_CA_CERTS"] = cert_path

        # Create a dummy token with proper structure
        # Note: This will still fail validation because our mock key is fake,
        # but it should successfully FETCH the JWKS, which is what we're testing
        dummy_token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InRlc3Qta2V5LWlkIn0.eyJzdWIiOiJ0ZXN0LXVzZXIiLCJleHAiOjk5OTk5OTk5OTl9.dummy"

        config = {
            "public_key_endpoint": "https://localhost:8443/keys",
            "access_token_audience": "test-audience",
            "id_token_audience": "test-audience",
            "cert": cert_path,  # Use custom CA cert
        }

        result = validate_jwt_token(dummy_token, "oidc-custom", config, "access")

        # The JWKS fetch should succeed (no SSL error)
        assert not result["valid"]
        assert not any(keyword in result["errors"].lower() for keyword in ["ssl", "certificate", "verify"])

    finally:
        # Restore original value
        if original_value is not None:
            os.environ["MEMGRAPH_SSO_CUSTOM_OIDC_EXTRA_CA_CERTS"] = original_value


def test_oidc_custom_with_verification_disabled(https_server):
    """Test that custom OIDC succeeds with cert=False (disables SSL verification)"""
    original_value = os.environ.pop("MEMGRAPH_SSO_CUSTOM_OIDC_EXTRA_CA_CERTS")

    try:
        # Create a dummy token
        dummy_token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InRlc3Qta2V5LWlkIn0.eyJzdWIiOiJ0ZXN0LXVzZXIiLCJleHAiOjk5OTk5OTk5OTl9.dummy"

        config = {
            "public_key_endpoint": "https://localhost:8443/keys",
            "access_token_audience": "test-audience",
            "id_token_audience": "test-audience",
            "cert": False,  # Disable SSL verification entirely (insecure, for testing only)
        }

        result = validate_jwt_token(dummy_token, "oidc-custom", config, "access")

        # The JWKS fetch should succeed (no SSL error) because verification is disabled
        # but the JWT validation should fail because the token is invalid
        assert not result["valid"]
        assert not any(keyword in result["errors"].lower() for keyword in ["ssl", "certificate", "verify"])

    finally:
        # Restore original value
        if original_value is not None:
            os.environ["MEMGRAPH_SSO_CUSTOM_OIDC_EXTRA_CA_CERTS"] = original_value


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v", "-s"]))
