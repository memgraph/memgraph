import io
import json

from onelogin.saml2.auth import OneLogin_Saml2_Auth
from onelogin.saml2.settings import OneLogin_Saml2_Settings

IDP = "entra_id"

ENTRA_IDP_ROLE_ATTRIBUTE = "http://schemas.microsoft.com/ws/2008/06/identity/claims/role"

SETTINGS_TEMPLATE = {
    "strict": True,
    # Enable debug mode (outputs errors).
    "debug": True,
    # Service Provider Data that we are deploying.
    "sp": {
        "entityId": "",
        "assertionConsumerService": {
            "url": "https://memgraph.com/?acs",
        },
        "NameIDFormat": "urn:oasis:names:tc:SAML:1.1:nameid-format:unspecified",
        "privateKey": "",
    },
    "idp": {
        "entityId": "",
        "singleSignOnService": {
            "url": "https://app.onelogin.com/trust/saml2/http-post/sso/<onelogin_connector_id>",
        },
        "x509cert": "",
    },
}


def mock_request():
    return {
        "http_host": "localhost:3000",
        "script_name": "/auth/providers/saml-entra-id/callback",
    }


def load_cert(path: str):
    with open(path, mode="r") as cert_file:
        return cert_file.read()


def get_username(auth, attributes, use_nameid, username_attribute):
    if use_nameid:
        return auth.get_nameid()

    if username_attribute not in attributes.keys():
        raise Exception("Username attribute not supplied.")

    if isinstance(attributes[username_attribute], list):
        return attributes[username_attribute][0]

    return attributes[username_attribute]


def get_role_mappings(raw_role_mappings: str):
    role_mappings = "".join(raw_role_mappings.split(" "))  # remove whitespace
    return {mapping.split(":")[0]: mapping.split(":")[1] for mapping in role_mappings.split(";")}


def authenticate(response: str, sso_config: dict):
    if "identity_provider" not in sso_config or sso_config["identity_provider"] != IDP:
        return {"authenticated": False, "errors": "The specified auth module requires SSO through Entra ID."}

    if "protocol" not in sso_config or sso_config["protocol"] != "SAML":
        return {"authenticated": False, "errors": "The specified auth module is used for the SAML protocol."}

    settings = SETTINGS_TEMPLATE
    settings["sp"]["entityId"] = sso_config["sso.saml.assertion_audience"]
    settings["sp"]["privateKey"] = sso_config["sso.saml.private_decryption_key"]
    settings["idp"]["x509cert"] = load_cert(sso_config["sso.saml.x509_certificate"])
    settings["idp"]["entityId"] = sso_config["sso.saml.identity_provider_id"]
    saml_settings = OneLogin_Saml2_Settings(settings)
    request_data = mock_request()
    request_data["post_data"] = {
        "SAMLResponse": response,
    }
    auth = OneLogin_Saml2_Auth(request_data, saml_settings)

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
            use_nameid=sso_config["sso.saml.use_NameID"],
            username_attribute=sso_config["sso.saml.username_attribute"],
        )
    except Exception as e:
        return {"authenticated": False, "errors": str(e)}

    role_mappings = get_role_mappings(sso_config["sso.saml.role_mapping"])
    if idp_role not in role_mappings:
        return {"authenticated": False, "errors": f'The role "{idp_role}" is not present in the given role mappings.'}

    return {
        "authenticated": True,
        "role": role_mappings[idp_role],
        "username": username,
    }


if __name__ == "__main__":
    input_stream = io.FileIO(1000, mode="r")
    output_stream = io.FileIO(1001, mode="w")
    while True:
        params = json.loads(input_stream.readline().decode("ascii"))
        ret = authenticate(**params)
        output_stream.write((json.dumps(ret) + "\n").encode("ascii"))
