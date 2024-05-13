#!/usr/bin/python3
import io
import json
import os

from onelogin.saml2.auth import OneLogin_Saml2_Auth
from onelogin.saml2.settings import OneLogin_Saml2_Settings

ENTRA_IDP_ROLE_ATTRIBUTE = "http://schemas.microsoft.com/ws/2008/06/identity/claims/role"

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
def mock_request():
    return {
        "http_host": "localhost:3000",
        "script_name": "/auth/providers/saml-entra-id/callback",
    }


def load_from_file(path: str):
    with open(path, mode="r") as file:
        return file.read()


def get_username(auth, attributes, use_nameid, username_attribute):
    if use_nameid:
        return auth.get_nameid()

    if username_attribute not in attributes.keys():
        raise Exception("Username attribute not supplied.")

    if isinstance(attributes[username_attribute], list):
        return attributes[username_attribute][0]

    return attributes[username_attribute]


def get_role_mappings(raw_role_mappings: str):
    if not raw_role_mappings:
        return {}

    role_mappings = "".join(raw_role_mappings.split(" "))  # remove whitespace
    return {mapping.split(":")[0]: mapping.split(":")[1] for mapping in role_mappings.split(";")}


def authenticate(response: str):
    # Load default settings
    settings = SETTINGS_TEMPLATE

    # Apply user configuration
    settings["sp"]["entityId"] = os.environ.get("AUTH_SAML_ENTRA_ID_ASSERTION_AUDIENCE", "")
    settings["sp"]["privateKey"] = load_from_file(os.environ.get("AUTH_SAML_ENTRA_ID_SP_PRIVATE_KEY", ""))
    settings["sp"]["x509cert"] = load_from_file(os.environ.get("AUTH_SAML_ENTRA_ID_SP_CERT", ""))
    settings["idp"]["x509cert"] = load_from_file(os.environ.get("AUTH_SAML_ENTRA_ID_IDP_CERT", ""))
    settings["idp"]["entityId"] = os.environ.get("AUTH_SAML_ENTRA_ID_IDP_ID", "")
    settings["security"]["wantAssertionsEncrypted"] = os.environ.get("AUTH_SAML_ENTRA_ID_ASSERTIONS_ENCRYPTED", False)
    settings["security"]["wantNameIdEncrypted"] = os.environ.get("AUTH_SAML_ENTRA_ID_NAME_ID_ENCRYPTED", False)

    # Create a SAML2 instance
    saml_settings = OneLogin_Saml2_Settings(settings)
    request_data = mock_request()
    request_data["post_data"] = {
        "SAMLResponse": response,
    }
    auth = OneLogin_Saml2_Auth(request_data, saml_settings)

    # Process response
    auth.process_response()
    errors = auth.get_errors()
    if errors:
        return {"authenticated": False, "errors": "\n".join(errors)}

    attributes = auth.get_attributes()
    if ENTRA_IDP_ROLE_ATTRIBUTE not in attributes.keys():
        return {"authenticated": False, "errors": "Role not found in the SAML response."}
    idp_role = attributes[ENTRA_IDP_ROLE_ATTRIBUTE]
    if not isinstance(idp_role, (str, list)):
        return {"authenticated": False, "errors": "The role attribute must be a string or a list."}
    idp_role = idp_role[0] if isinstance(idp_role, list) else idp_role

    try:
        username = get_username(
            auth,
            attributes,
            use_nameid=os.environ.get("AUTH_SAML_ENTRA_ID_USE_NAME_ID", False),
            username_attribute=os.environ.get("AUTH_SAML_ENTRA_ID_USERNAME_ATTRIBUTE", False),
        )
    except Exception as e:
        return {"authenticated": False, "errors": str(e)}

    role_mappings = get_role_mappings(os.environ.get("AUTH_SAML_ENTRA_ID_ROLE_MAPPING", ""))
    if idp_role not in role_mappings:
        return {"authenticated": False, "errors": f'The role "{idp_role}" is not present in the given role mappings.'}

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
