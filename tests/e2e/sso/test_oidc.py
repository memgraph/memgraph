import os
import sys

import pytest
from oidc import process_tokens


@pytest.mark.parametrize("scheme", ["oidc-entra-id", "oidc-okta"])
def test_invalid_tokens(scheme):
    config = {}

    access_token = {
        "token": {"username": "username"},
        "errors": "error_access",
    }
    id_token = {
        "token": {
            "sub": "sub-field",
        },
        "errors": "error_id",
    }

    assert process_tokens((access_token, id_token), config, scheme) == {
        "authenticated": False,
        "errors": "Error while decoding access token: error_access",
    }

    del access_token["errors"]
    assert process_tokens((access_token, id_token), config, scheme) == {
        "authenticated": False,
        "errors": "Error while decoding id token: error_id",
    }

    del id_token["errors"]
    roles_field = "roles" if scheme == "oidc-entra-id" else "groups"
    assert process_tokens((access_token, id_token), config, scheme) == {
        "authenticated": False,
        "errors": f"Missing roles field named {roles_field}, roles are probably not correctly configured on the token issuer",
    }

    if scheme == "oidc-entra-id":
        access_token["token"]["roles"] = ["test-role"]
    elif scheme == "oidc-okta":
        access_token["token"]["groups"] = ["test-role"]
    config["role_mapping"] = {}
    assert process_tokens((access_token, id_token), config, scheme) == {
        "authenticated": False,
        "errors": "Cannot map role test-role to Memgraph role",
    }

    config["role_mapping"] = {"test-role": "admin"}
    config["username"] = "id:invalid-field"
    assert process_tokens((access_token, id_token), config, scheme) == {
        "authenticated": False,
        "errors": f"Field invalid-field missing in id token",
    }

    config["username"] = "access:invalid-field"
    assert process_tokens((access_token, id_token), config, scheme) == {
        "authenticated": False,
        "errors": f"Field invalid-field missing in access token",
    }

    config["username"] = "id:sub"
    assert process_tokens((access_token, id_token), config, scheme) == {
        "authenticated": True,
        "role": "admin",
        "username": "sub-field",
    }

    config["username"] = "access:username"
    assert process_tokens((access_token, id_token), config, scheme) == {
        "authenticated": True,
        "role": "admin",
        "username": "username",
    }


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
