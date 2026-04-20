import base64
import sys
from types import ModuleType
from unittest.mock import MagicMock

import pytest

mock_gssapi = ModuleType("gssapi")
mock_gssapi.Name = MagicMock()
mock_gssapi.NameType = MagicMock()
mock_gssapi.Credentials = MagicMock()
mock_gssapi.SecurityContext = MagicMock()
mock_gssapi.exceptions = ModuleType("gssapi.exceptions")
mock_gssapi.exceptions.GSSError = type("GSSError", (Exception,), {})
sys.modules["gssapi"] = mock_gssapi
sys.modules["gssapi.exceptions"] = mock_gssapi.exceptions

from kerberos import authenticate


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for var in [
        "MEMGRAPH_SSO_KERBEROS_KEYTAB",
        "MEMGRAPH_SSO_KERBEROS_SERVICE_PRINCIPAL",
        "MEMGRAPH_SSO_KERBEROS_REALM",
        "MEMGRAPH_SSO_KERBEROS_USERNAME_FIELD",
        "MEMGRAPH_SSO_KERBEROS_ROLE_MAPPING",
        "KRB5_KTNAME",
    ]:
        monkeypatch.delenv(var, raising=False)
    mock_gssapi.Name.reset_mock(side_effect=True)
    mock_gssapi.Credentials.reset_mock(side_effect=True)
    mock_gssapi.SecurityContext.reset_mock(side_effect=True)


def _setup_env(monkeypatch, overrides=None):
    defaults = {
        "MEMGRAPH_SSO_KERBEROS_SERVICE_PRINCIPAL": "memgraph/dbhost.example.com@EXAMPLE.COM",
        "MEMGRAPH_SSO_KERBEROS_KEYTAB": "/etc/memgraph/memgraph.keytab",
        "MEMGRAPH_SSO_KERBEROS_REALM": "EXAMPLE.COM",
        "MEMGRAPH_SSO_KERBEROS_USERNAME_FIELD": "name",
        "MEMGRAPH_SSO_KERBEROS_ROLE_MAPPING": "*:analyst",
        "MEMGRAPH_SSO_KERBEROS_ROLE_MAPPING_MODE": "principal",
    }
    if overrides:
        defaults.update(overrides)
    for key, value in defaults.items():
        monkeypatch.setenv(key, value)


def _setup_mock_ctx(principal="david@EXAMPLE.COM", complete=True):
    mock_ctx = MagicMock()
    mock_ctx.complete = complete
    mock_ctx.step.return_value = None
    mock_ctx.initiator_name = MagicMock()
    mock_ctx.initiator_name.__str__ = lambda self: principal
    mock_gssapi.SecurityContext.return_value = mock_ctx


TOKEN = base64.b64encode(b"fake-spnego-token").decode()


def test_invalid_scheme():
    result = authenticate(response="dGVzdA==", scheme="invalid-scheme")
    assert result == {"authenticated": False, "errors": "Invalid SSO scheme"}


def test_missing_config(monkeypatch):
    monkeypatch.setenv("MEMGRAPH_SSO_KERBEROS_SERVICE_PRINCIPAL", "memgraph/dbhost@EXAMPLE.COM")
    result = authenticate(response="dGVzdA==", scheme="kerberos")
    assert result["authenticated"] is False
    assert "Missing role mappings" in result["errors"]

    monkeypatch.delenv("MEMGRAPH_SSO_KERBEROS_SERVICE_PRINCIPAL")
    monkeypatch.setenv("MEMGRAPH_SSO_KERBEROS_ROLE_MAPPING", "*:analyst")
    result = authenticate(response="dGVzdA==", scheme="kerberos")
    assert result == {"authenticated": False, "errors": "Missing service principal configuration"}


def test_successful_auth_username_name(monkeypatch):
    _setup_env(monkeypatch, {"MEMGRAPH_SSO_KERBEROS_USERNAME_FIELD": "name"})
    _setup_mock_ctx("david@EXAMPLE.COM")

    result = authenticate(response=TOKEN, scheme="kerberos")

    assert result["authenticated"] is True
    assert result["username"] == "david"
    assert "analyst" in result["roles"]


def test_successful_auth_username_principal(monkeypatch):
    _setup_env(monkeypatch, {"MEMGRAPH_SSO_KERBEROS_USERNAME_FIELD": "principal"})
    _setup_mock_ctx("david@EXAMPLE.COM")

    result = authenticate(response=TOKEN, scheme="kerberos")

    assert result["authenticated"] is True
    assert result["username"] == "david@EXAMPLE.COM"


@pytest.mark.parametrize(
    "realm,client_principal,should_pass",
    [
        ("EXPECTED.COM", "david@WRONG.COM", False),
        ("", "david@ANY.COM", True),
    ],
)
def test_realm_validation(monkeypatch, realm, client_principal, should_pass):
    _setup_env(monkeypatch, {"MEMGRAPH_SSO_KERBEROS_REALM": realm})
    _setup_mock_ctx(client_principal)

    result = authenticate(response=TOKEN, scheme="kerberos")

    assert result["authenticated"] is should_pass
    if not should_pass:
        assert "does not match expected realm" in result["errors"]


def test_mutual_auth_rejected(monkeypatch):
    _setup_env(monkeypatch)
    _setup_mock_ctx()
    mock_gssapi.SecurityContext.return_value.step.return_value = b"response-token"

    result = authenticate(response=TOKEN, scheme="kerberos")

    assert result["authenticated"] is False
    assert "Mutual authentication not supported" in result["errors"]


def test_invalid_role_mapping_mode(monkeypatch):
    _setup_env(monkeypatch, {"MEMGRAPH_SSO_KERBEROS_ROLE_MAPPING_MODE": "bogus"})
    _setup_mock_ctx()

    result = authenticate(response=TOKEN, scheme="kerberos")

    assert result["authenticated"] is False
    assert "Invalid role_mapping_mode" in result["errors"]


def test_role_mapping_with_spn_port(monkeypatch):
    # rsplit(":", 1) must preserve the ":1433" in the SPN
    mapping = "MSSQLSvc/db.example.com:1433@EXAMPLE.COM:admin"
    _setup_env(monkeypatch, {"MEMGRAPH_SSO_KERBEROS_ROLE_MAPPING": mapping})
    _setup_mock_ctx("MSSQLSvc/db.example.com:1433@EXAMPLE.COM")

    result = authenticate(response=TOKEN, scheme="kerberos")

    assert result["authenticated"] is True
    assert result["roles"] == ["admin"]


def test_incomplete_context(monkeypatch):
    _setup_env(monkeypatch)
    _setup_mock_ctx(complete=False)

    result = authenticate(response=TOKEN, scheme="kerberos")

    assert result["authenticated"] is False
    assert "incomplete" in result["errors"]


def test_gssapi_error(monkeypatch):
    _setup_env(monkeypatch)
    mock_gssapi.SecurityContext.side_effect = mock_gssapi.exceptions.GSSError("KRB5KDC_ERR_PREAUTH_FAILED")

    result = authenticate(response=TOKEN, scheme="kerberos")

    assert result["authenticated"] is False
    assert "Kerberos authentication failed" in result["errors"]


@pytest.mark.parametrize(
    "role_mapping,principal,expected_roles,should_pass",
    [
        ("david:admin,editor;alice:viewer", "david@EXAMPLE.COM", ["admin", "editor"], True),
        ("david@EXAMPLE.COM:admin", "david@EXAMPLE.COM", ["admin"], True),
        ("*:viewer;david:admin", "david@EXAMPLE.COM", ["admin", "viewer"], True),
        ("alice:viewer;bob:editor", "david@EXAMPLE.COM", None, False),
    ],
)
def test_role_mapping(monkeypatch, role_mapping, principal, expected_roles, should_pass):
    _setup_env(monkeypatch, {"MEMGRAPH_SSO_KERBEROS_ROLE_MAPPING": role_mapping})
    _setup_mock_ctx(principal)

    result = authenticate(response=TOKEN, scheme="kerberos")

    assert result["authenticated"] is should_pass
    if should_pass:
        assert sorted(result["roles"]) == sorted(expected_roles)
    else:
        assert "cannot be mapped" in result["errors"]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
