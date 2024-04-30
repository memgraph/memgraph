import io
import json

from onelogin.saml2.auth import OneLogin_Saml2_Auth
from onelogin.saml2.settings import OneLogin_Saml2_Settings


def mock_request():
    return {
        "http_host": "example.com",
        "script_name": "/index.html",
    }


def authenticate(response, settings, role_mapping, idp_role_attribute):
    saml_settings = OneLogin_Saml2_Settings(settings=settings)
    request_data = mock_request()
    request_data["post_data"] = {
        "SAMLResponse": response,
    }
    auth = OneLogin_Saml2_Auth(request_data, saml_settings)

    auth.process_response()
    errors = auth.get_errors()
    if errors:
        return {"authenticated": False, "errors": errors}

    attributes = auth.get_attributes()
    if idp_role_attribute not in attributes:
        return {"authenticated": False, "errors": ["Role not found in the SAML response."]}
    idp_role = attributes[idp_role_attribute]
    if idp_role is not str:
        return {"authenticated": False, "errors": ["The role name must be a string."]}

    return {"authenticated": True, "role": role_mapping[idp_role]}


if __name__ == "__main__":
    input_stream = io.FileIO(1000, mode="r")
    output_stream = io.FileIO(1001, mode="w")
    while True:
        params = json.loads(input_stream.readline().decode("ascii"))
        ret = authenticate(**params)
        output_stream.write((json.dumps(ret) + "\n").encode("ascii"))
