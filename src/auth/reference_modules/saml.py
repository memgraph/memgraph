#!/usr/bin/python3
import io
import json
import os

from onelogin.saml2.auth import OneLogin_Saml2_Auth
from onelogin.saml2.settings import OneLogin_Saml2_Settings

ENTRA_IDP_ROLE_ATTRIBUTE = "http://schemas.microsoft.com/ws/2008/06/identity/claims/role"  # NOSONAR
# ^ standardized by Microsoft: https://learn.microsoft.com/en-us/entra/identity-platform/reference-saml-tokens

# Default settings: "sp" refers to the service provider (Memgraph), and "idp" to the identity provider (Entra ID)
SETTINGS_TEMPLATE = {
    "strict": True,
    "debug": True,
    "sp": {
        "entityId": "",
        "assertionConsumerService": {
            "url": "https://memgraph.com/<acs>",
            # Dummy value required by library (this service is Memgraph-internal, not online)
        },
        "NameIDFormat": "urn:oasis:names:tc:SAML:1.1:nameid-format:unspecified",
        "x509cert": "",
        "privateKey": "",
    },
    "idp": {
        "entityId": "",
        "singleSignOnService": {
            "url": "https://app.onelogin.com/trust/saml2/http-post/sso/<onelogin_connector_id>",
            # Dummy value required by library (the library would use it to send auth requests,
            # whereas this module only does auth)
        },
        "x509cert": "",
    },
    "security": {
        "wantAssertionsEncrypted": False,
        "wantNameIdEncrypted": False,
    },
}


# Dummy request (python3-saml library assumes there’s one, and you can only pass the response together with it)
def mock_request(scheme_env):
    return {
        "http_host": "localhost:3000",
        "script_name": os.environ.get(f"MEMGRAPH_SSO_{scheme_env}_SAML_CALLBACK_URL", ""),
    }


def load_from_file(path: str):
    if not path:
        return ""

    with open(path, mode="r") as file:
        return file.read()


def str_to_bool(string: str):
    return string.lower() == "true"


def has_role_attribute(scheme: str, attributes):
    if scheme == "saml-entra-id":
        return ENTRA_IDP_ROLE_ATTRIBUTE in attributes.keys()
    elif scheme == "saml-okta":
        return os.environ.get("MEMGRAPH_SSO_OKTA_SAML_ROLE_ATTRIBUTE", "") in attributes.keys()
    return False


def get_username(auth, attributes, use_nameid, username_attribute):
    if use_nameid:
        return auth.get_nameid()

    if username_attribute not in attributes.keys():
        raise ValueError("Username attribute not supplied.")

    if isinstance(attributes[username_attribute], list):
        return attributes[username_attribute][0]

    return attributes[username_attribute]


def get_role_mappings(raw_role_mappings: str):
    if not raw_role_mappings:
        return {}

    role_mappings = "".join(raw_role_mappings.split(" "))  # remove whitespace
    return {mapping.split(":")[0]: mapping.split(":")[1] for mapping in role_mappings.split(";")}


def authenticate(scheme: str, response: str):
    if scheme not in ("saml-entra-id", "saml-okta"):
        return {
            "authenticated": False,
            "errors": f'The selected auth module is not compatible with the "{scheme}" scheme.',
        }

    scheme_env = "ENTRA_ID" if scheme == "saml-entra-id" else "OKTA"

    # Load default settings
    settings = SETTINGS_TEMPLATE

    # Apply user configuration
    settings["sp"]["entityId"] = os.environ.get(f"MEMGRAPH_SSO_{scheme_env}_SAML_ASSERTION_AUDIENCE", "")
    settings["sp"]["privateKey"] = load_from_file(os.environ.get(f"MEMGRAPH_SSO_{scheme_env}_SAML_SP_PRIVATE_KEY", ""))
    settings["sp"]["x509cert"] = load_from_file(os.environ.get(f"MEMGRAPH_SSO_{scheme_env}_SAML_SP_CERT", ""))
    settings["idp"]["x509cert"] = load_from_file(os.environ.get(f"MEMGRAPH_SSO_{scheme_env}_SAML_IDP_CERT", ""))
    settings["idp"]["entityId"] = os.environ.get(f"MEMGRAPH_SSO_{scheme_env}_SAML_IDP_ID", "")
    settings["security"]["wantAssertionsEncrypted"] = str_to_bool(
        os.environ.get(f"MEMGRAPH_SSO_{scheme_env}_SAML_ASSERTIONS_ENCRYPTED", "")
    )
    settings["security"]["wantNameIdEncrypted"] = str_to_bool(
        os.environ.get(f"MEMGRAPH_SSO_{scheme_env}_SAML_NAME_ID_ENCRYPTED", "")
    )
    settings["security"]["wantAttributeStatement"] = str_to_bool(
        os.environ.get(f"MEMGRAPH_SSO_{scheme_env}_SAML_WANT_ATTRIBUTE_STATEMENT", "true")
    )

    # Create a SAML2 instance
    try:
        saml_settings = OneLogin_Saml2_Settings(settings)
    except Exception as e:
        return {"authenticated": False, "errors": f"python3-saml settings not configured correctly: {str(e)}"}
    request_data = mock_request(scheme_env=scheme_env)
    request_data["post_data"] = {
        "SAMLResponse": response,
    }
    auth = OneLogin_Saml2_Auth(request_data, saml_settings)

    # Process response
    try:
        auth.process_response()
    except Exception as e:
        return {"authenticated": False, "errors": f"Errors while processing SAML response: {str(e)}"}
    errors = auth.get_errors()
    if errors:
        joined = "\n".join(errors)
        return {"authenticated": False, "errors": f"Errors while processing SAML response: {joined}"}

    attributes = auth.get_attributes()

    if not has_role_attribute(scheme, attributes):
        return {"authenticated": False, "errors": "Role not found in the SAML response."}
    attribute_name = (
        ENTRA_IDP_ROLE_ATTRIBUTE if scheme == "saml-entra-id" else os.environ.get("MEMGRAPH_SSO_OKTA_SAML_ROLE_ATTRIBUTE", "")
    )
    idp_role = attributes[attribute_name]
    if not isinstance(idp_role, (str, list)):
        return {"authenticated": False, "errors": "The role attribute must be a string or a list."}
    idp_role = idp_role[0] if isinstance(idp_role, list) else idp_role

    try:
        username = get_username(
            auth,
            attributes,
            use_nameid=str_to_bool(os.environ.get(f"MEMGRAPH_SSO_{scheme_env}_SAML_USE_NAME_ID", "true")),
            username_attribute=os.environ.get(f"MEMGRAPH_SSO_{scheme_env}_SAML_USERNAME_ATTRIBUTE", ""),
        )
    except Exception as e:
        return {"authenticated": False, "errors": str(e)}

    role_mappings = get_role_mappings(os.environ.get(f"MEMGRAPH_SSO_{scheme_env}_SAML_ROLE_MAPPING", ""))
    if idp_role not in role_mappings:
        return {
            "authenticated": False,
            "errors": f'The role "{idp_role}" is not present in the given role mappings.',
        }

    return {
        "authenticated": True,
        "role": role_mappings[idp_role],
        "username": username,
    }


if __name__ == "__main__":
    # I/O with Memgraph
    input_stream = io.FileIO(1000, mode="r")
    output_stream = io.FileIO(1001, mode="w")
    while True:
        params = json.loads(input_stream.readline().decode("ascii"))
        ret = authenticate(**params)
        output_stream.write((json.dumps(ret) + "\n").encode("ascii"))
