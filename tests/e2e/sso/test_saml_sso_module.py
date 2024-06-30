import os
import pytest
import sys

import onelogin.saml2.utils


def skip_signature_validation(*args, **kwargs):
    return True


# Monkey-patch the SAML library to skip signature validation (not needed for tests)
onelogin.saml2.utils.OneLogin_Saml2_Utils.validate_sign = skip_signature_validation

from common import compose_path, load_test_data
import saml


@pytest.fixture(scope="function")
def provide_env():
    os.environ["MEMGRAPH_SSO_ENTRA_ID_SAML_CALLBACK_URL"] = "auth/providers/saml-entra-id/callback"
    os.environ["MEMGRAPH_SSO_ENTRA_ID_SAML_ASSERTION_AUDIENCE"] = "spn:f516a7de-6c3f-4d1d-a289-539301039291"
    os.environ["MEMGRAPH_SSO_ENTRA_ID_SAML_SP_CERT"] = compose_path(filename="dummy_cert.txt")
    os.environ["MEMGRAPH_SSO_ENTRA_ID_SAML_IDP_ID"] = "https://sts.windows.net/371aa2c4-2f9b-4fe1-bc52-23824e906c26/"
    os.environ["MEMGRAPH_SSO_ENTRA_ID_SAML_USERNAME_ATTRIBUTE"] = "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name"
    os.environ["MEMGRAPH_SSO_ENTRA_ID_SAML_USE_NAME_ID"] = "False"
    os.environ["MEMGRAPH_SSO_ENTRA_ID_SAML_ROLE_MAPPING"] = "test-admin: admin; test-reader: reader"

    os.environ["MEMGRAPH_SSO_OKTA_SAML_CALLBACK_URL"] = "auth/providers/saml-okta/callback"
    os.environ["MEMGRAPH_SSO_OKTA_SAML_ASSERTION_AUDIENCE"] = "myApplication"
    os.environ["MEMGRAPH_SSO_OKTA_SAML_SP_CERT"] = compose_path(filename="dummy_cert.txt")
    os.environ["MEMGRAPH_SSO_OKTA_SAML_IDP_ID"] = "http://www.okta.com/exke6ubrkbz9TG2iB697"
    os.environ["MEMGRAPH_SSO_OKTA_SAML_WANT_ATTRIBUTE_STATEMENT"] = "false"
    os.environ["MEMGRAPH_SSO_OKTA_SAML_USERNAME_ATTRIBUTE"] = "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name"
    os.environ["MEMGRAPH_SSO_OKTA_SAML_USE_NAME_ID"] = "True"
    os.environ["MEMGRAPH_SSO_OKTA_SAML_ROLE_ATTRIBUTE"] = "groups"
    os.environ["MEMGRAPH_SSO_OKTA_SAML_ROLE_MAPPING"] = "test-admin: admin; test-reader: reader"

    yield None

    os.environ["MEMGRAPH_SSO_ENTRA_ID_SAML_CALLBACK_URL"] = ""
    os.environ["MEMGRAPH_SSO_ENTRA_ID_SAML_ASSERTION_AUDIENCE"] = ""
    os.environ["MEMGRAPH_SSO_ENTRA_ID_SAML_SP_CERT"] = ""
    os.environ["MEMGRAPH_SSO_ENTRA_ID_SAML_IDP_ID"] = ""
    os.environ["MEMGRAPH_SSO_ENTRA_ID_SAML_USERNAME_ATTRIBUTE"] = ""
    os.environ["MEMGRAPH_SSO_ENTRA_ID_SAML_USE_NAME_ID"] = ""
    os.environ["MEMGRAPH_SSO_ENTRA_ID_SAML_ROLE_MAPPING"] = ""

    os.environ["MEMGRAPH_SSO_OKTA_SAML_CALLBACK_URL"] = ""
    os.environ["MEMGRAPH_SSO_OKTA_SAML_ASSERTION_AUDIENCE"] = ""
    os.environ["MEMGRAPH_SSO_OKTA_SAML_SP_CERT"] = ""
    os.environ["MEMGRAPH_SSO_OKTA_SAML_IDP_ID"] = ""
    os.environ["MEMGRAPH_SSO_OKTA_SAML_WANT_ATTRIBUTE_STATEMENT"] = ""
    os.environ["MEMGRAPH_SSO_OKTA_SAML_USERNAME_ATTRIBUTE"] = ""
    os.environ["MEMGRAPH_SSO_OKTA_SAML_USE_NAME_ID"] = ""
    os.environ["MEMGRAPH_SSO_OKTA_SAML_ROLE_ATTRIBUTE"] = ""
    os.environ["MEMGRAPH_SSO_OKTA_SAML_ROLE_MAPPING"] = ""


def test_unsupported_scheme():
    SCHEME_NAME = "fake-scheme"
    assert saml.authenticate(scheme=SCHEME_NAME, response="dummy") == {
        "authenticated": False,
        "errors": f'The selected auth module is not compatible with the "{SCHEME_NAME}" scheme.',
    }


@pytest.mark.parametrize("scheme", ["saml-entra-id", "saml-okta"])
def test_invalid_settings(provide_env, scheme):
    env_element = "ENTRA_ID" if scheme == "saml-entra-id" else "OKTA"
    os.environ[f"MEMGRAPH_SSO_{env_element}_SAML_ASSERTION_AUDIENCE"] = ""

    assert saml.authenticate(scheme=scheme, response="dummy") == {
        "authenticated": False,
        "errors": "python3-saml settings not configured correctly: Invalid dict settings: sp_entityId_not_found",
    }


@pytest.mark.parametrize("scheme", ["saml-entra-id", "saml-okta"])
def test_invalid_format(provide_env, scheme):
    assert saml.authenticate(scheme=scheme, response="not xml") == {
        "authenticated": False,
        "errors": "Errors while processing SAML response: Incorrect padding",
    }


@pytest.mark.parametrize("scheme", ["saml-entra-id", "saml-okta"])
def test_error_while_processing_response(provide_env, scheme):
    filename_prefix = "entra_id" if scheme == "saml-entra-id" else "okta"
    assert saml.authenticate(
        scheme=scheme, response=load_test_data(filename=f"{filename_prefix}_response_timed_out.txt")
    ) == {
        "authenticated": False,
        "errors": "Errors while processing SAML response: invalid_response",
    }


@pytest.mark.parametrize("scheme", ["saml-entra-id", "saml-okta"])
def test_role_missing_from_response(provide_env, scheme):
    filename_prefix = "entra_id" if scheme == "saml-entra-id" else "okta"
    assert saml.authenticate(
        scheme=scheme, response=load_test_data(filename=f"{filename_prefix}_response_missing_role.txt")
    ) == {"authenticated": False, "errors": "Role not found in the SAML response."}


@pytest.mark.parametrize("scheme", ["saml-entra-id", "saml-okta"])
def test_username_missing_from_response(provide_env, scheme):
    os.environ["MEMGRAPH_SSO_ENTRA_ID_SAML_USERNAME_ATTRIBUTE"] = "wrong_value"
    os.environ["MEMGRAPH_SSO_OKTA_SAML_USE_NAME_ID"] = "False"
    os.environ["MEMGRAPH_SSO_OKTA_SAML_USERNAME_ATTRIBUTE"] = "wrong_value"
    filename_prefix = "entra_id" if scheme == "saml-entra-id" else "okta"
    assert saml.authenticate(scheme=scheme, response=load_test_data(filename=f"{filename_prefix}_response.txt")) == {
        "authenticated": False,
        "errors": "Username attribute not supplied.",
    }


@pytest.mark.parametrize("scheme", ["saml-entra-id", "saml-okta"])
def test_role_missing_from_mappings(provide_env, scheme):
    os.environ["MEMGRAPH_SSO_ENTRA_ID_SAML_ROLE_MAPPING"] = "visitor: reader"
    os.environ["MEMGRAPH_SSO_OKTA_SAML_ROLE_MAPPING"] = "visitor: reader"
    filename_prefix = "entra_id" if scheme == "saml-entra-id" else "okta"
    assert saml.authenticate(scheme=scheme, response=load_test_data(filename=f"{filename_prefix}_response.txt")) == {
        "authenticated": False,
        "errors": 'The role "test-admin" is not present in the given role mappings.',
    }


@pytest.mark.parametrize("scheme", ["saml-entra-id", "saml-okta"])
def test_successful(provide_env, scheme):
    filename_prefix = "entra_id" if scheme == "saml-entra-id" else "okta"
    assert saml.authenticate(scheme=scheme, response=load_test_data(filename=f"{filename_prefix}_response.txt")) == {
        "authenticated": True,
        "role": "admin",
        "username": "Anthony",
    }


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
