import os
import sys

import pytest
from oidc import process_tokens


@pytest.mark.parametrize("scheme", ["oidc-entra-id", "oidc-okta", "oidc-custom"])
@pytest.mark.parametrize(
    "case",
    [
        {
            "name": "error_in_access_token",
            "access_token": {"token": {"username": "username"}, "errors": "error_access"},
            "id_token": {"token": {"sub": "sub-field"}, "errors": "error_id"},
            "config": {},
            "expected": {"authenticated": False, "errors": "Error while decoding access token: error_access"},
        },
        {
            "name": "error_in_id_token",
            "access_token": {"token": {"username": "username"}},
            "id_token": {"token": {"sub": "sub-field"}, "errors": "error_id"},
            "config": {},
            "expected": {"authenticated": False, "errors": "Error while decoding id token: error_id"},
        },
        {
            "name": "missing_roles_field",
            "access_token": {"token": {"username": "username"}},
            "id_token": {"token": {"sub": "sub-field"}},
            "config": {},
            "expected": lambda scheme: {
                "authenticated": False,
                "errors": f"Missing roles field named {'groups' if scheme == 'oidc-okta' else 'roles'}, roles are probably not correctly configured on the token issuer",
            },
        },
        {
            "name": "missing_role_mapping",
            "access_token": {"token": {"username": "username", "roles": "test-role", "groups": "test-role"}},
            "id_token": {"token": {"sub": "sub-field"}},
            "config": {"role_mapping": {}},
            "expected": {"authenticated": False, "errors": "Cannot map role test-role to Memgraph role"},
        },
        {
            "name": "missing_field_in_id_token",
            "access_token": {"token": {"username": "username", "roles": "test-role", "groups": "test-role"}},
            "id_token": {"token": {"sub": "sub-field"}},
            "config": {"role_mapping": {"test-role": "admin"}, "username": "id:invalid-field"},
            "expected": {"authenticated": False, "errors": "Field invalid-field missing in id token"},
        },
        {
            "name": "missing_field_in_access_token",
            "access_token": {"token": {"username": "username", "roles": "test-role", "groups": "test-role"}},
            "id_token": {"token": {"sub": "sub-field"}},
            "config": {"role_mapping": {"test-role": "admin"}, "username": "access:invalid-field"},
            "expected": {"authenticated": False, "errors": "Field invalid-field missing in access token"},
        },
        {
            "name": "successful_with_id_token_username",
            "access_token": {"token": {"username": "username", "roles": "test-role", "groups": "test-role"}},
            "id_token": {"token": {"sub": "sub-field"}},
            "config": {"role_mapping": {"test-role": "admin"}, "username": "id:sub"},
            "expected": {"authenticated": True, "role": "admin", "username": "sub-field"},
        },
        {
            "name": "successful_with_access_token_username",
            "access_token": {"token": {"username": "username", "roles": "test-role", "groups": "test-role"}},
            "id_token": {"token": {"sub": "sub-field"}},
            "config": {"role_mapping": {"test-role": "admin"}, "username": "access:username"},
            "expected": {"authenticated": True, "role": "admin", "username": "username"},
        },
        {
            "name": "multiple roles mapping to same memgraph role",
            "access_token": {
                "token": {
                    "username": "username",
                    "roles": ["test-role1", "test-role2", "test-role3"],
                    "groups": ["test-role1", "test-role2", "test-role3"],
                }
            },
            "id_token": {"token": {"sub": "sub-field"}},
            "config": {"role_mapping": {"test-role1": "admin", "test-role2": "admin"}, "username": "access:username"},
            "expected": {"authenticated": True, "role": "admin", "username": "username"},
        },
        {
            "name": "multiple roles mapping to same different role",
            "access_token": {
                "token": {
                    "username": "username",
                    "roles": ["test-role1", "test-role2", "test-role3"],
                    "groups": ["test-role1", "test-role2", "test-role3"],
                }
            },
            "id_token": {"token": {"sub": "sub-field"}},
            "config": {"role_mapping": {"test-role1": "admin", "test-role2": "tester"}, "username": "access:username"},
            "expected": {
                "authenticated": False,
                "errors": "Multiple roles ['admin', 'tester'] can mapped to Memgraph roles. Only one matching role must exist",
            },
        },
        {
            "name": "one role in a list",
            "access_token": {"token": {"username": "username", "roles": ["test-role1"], "groups": ["test-role1"]}},
            "id_token": {"token": {"sub": "sub-field"}},
            "config": {"role_mapping": {"test-role1": "admin", "test-role2": "tester"}, "username": "access:username"},
            "expected": {"authenticated": True, "role": "admin", "username": "username"},
        },
        {
            "name": "one matching role in a list",
            "access_token": {
                "token": {
                    "username": "username",
                    "roles": ["test-role1", "test-role2", "test-role3"],
                    "groups": ["test-role1", "test-role2", "test-role3"],
                }
            },
            "id_token": {"token": {"sub": "sub-field"}},
            "config": {
                "role_mapping": {"test-role2": "tester", "test-role4": "analyst"},
                "username": "access:username",
            },
            "expected": {"authenticated": True, "role": "tester", "username": "username"},
        },
    ],
)
def test_invalid_tokens(scheme, case):
    config = dict(case["config"])
    if scheme == "oidc-custom":
        config["role_field"] = "roles"
    if "role_mapping" in config and "role_field" not in config:
        config["role_field"] = "roles" if scheme != "oidc-okta" else "groups"

    access_token = dict(case["access_token"])
    id_token = dict(case["id_token"])

    # case 2 has a lambda function as expected value
    expected = case["expected"](scheme) if callable(case["expected"]) else case["expected"]

    result = process_tokens((access_token, id_token), config, scheme)
    assert result == expected, f"Failed case: {case['name']}"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
