import os
import sys

import pytest
from oidc import process_tokens, _load_role_mappings


@pytest.mark.parametrize("scheme", ["oidc-entra-id", "oidc-okta", "oidc-custom"])
@pytest.mark.parametrize(
    "case",
    [
        {
            "name": "error_in_access_token",
            "access_token": {"token": {"username": "username"}, "errors": "error_access"},
            "id_token": {"token": {"sub": "sub-field"}, "errors": "error_id"},
            "config": {"use_id_token": True},
            "expected": {"authenticated": False, "errors": "Error while decoding access token: error_access"},
        },
        {
            "name": "error_in_id_token",
            "access_token": {"token": {"username": "username"}},
            "id_token": {"token": {"sub": "sub-field"}, "errors": "error_id"},
            "config": {"use_id_token": True},
            "expected": {"authenticated": False, "errors": "Error while decoding id token: error_id"},
        },
        {
            "name": "missing_roles_field",
            "access_token": {"token": {"username": "username"}},
            "id_token": {"token": {"sub": "sub-field"}},
            "config": {"use_id_token": True},
            "expected": lambda scheme: {
                "authenticated": False,
                "errors": f"Missing roles field named {'groups' if scheme == 'oidc-okta' else 'roles'}, roles are probably not correctly configured on the token issuer",
            },
        },
        {
            "name": "missing_role_mapping",
            "access_token": {"token": {"username": "username", "roles": "test-role", "groups": "test-role"}},
            "id_token": {"token": {"sub": "sub-field"}},
            "config": {"role_mapping": {}, "use_id_token": True},
            "expected": {"authenticated": False, "errors": "Cannot map role test-role to Memgraph role"},
        },
        {
            "name": "missing_field_in_id_token",
            "access_token": {"token": {"username": "username", "roles": "test-role", "groups": "test-role"}},
            "id_token": {"token": {"sub": "sub-field"}},
            "config": {"role_mapping": {"test-role": ["admin"]}, "username": "id:invalid-field", "use_id_token": True},
            "expected": {"authenticated": False, "errors": "Field invalid-field missing in id token"},
        },
        {
            "name": "missing_field_in_access_token",
            "access_token": {"token": {"username": "username", "roles": "test-role", "groups": "test-role"}},
            "id_token": {"token": {"sub": "sub-field"}},
            "config": {"role_mapping": {"test-role": ["admin"]}, "username": "access:invalid-field", "use_id_token": True},
            "expected": {"authenticated": False, "errors": "Field invalid-field missing in access token"},
        },
        {
            "name": "successful_with_id_token_username",
            "access_token": {"token": {"username": "username", "roles": "test-role", "groups": "test-role"}},
            "id_token": {"token": {"sub": "sub-field"}},
            "config": {"role_mapping": {"test-role": ["admin"]}, "username": "id:sub", "use_id_token": True},
            "expected": {"authenticated": True, "roles": ["admin"], "username": "sub-field"},
        },
        {
            "name": "successful_with_access_token_username",
            "access_token": {"token": {"username": "username", "roles": "test-role", "groups": "test-role"}},
            "id_token": {"token": {"sub": "sub-field"}},
            "config": {"role_mapping": {"test-role": ["admin"]}, "username": "access:username", "use_id_token": False},
            "expected": {"authenticated": True, "roles": ["admin"], "username": "username"},
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
            "config": {"role_mapping": {"test-role1": ["admin"], "test-role2": ["admin"]}, "username": "access:username", "use_id_token": False},
            "expected": {"authenticated": True, "roles": ["admin"], "username": "username"},
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
            "config": {"role_mapping": {"test-role1": ["admin"], "test-role2": ["tester"]}, "username": "access:username", "use_id_token": False},
            "expected": {
                "authenticated": True,
                "roles": ["admin", "tester"],
                "username": "username",
            },
        },
        {
            "name": "one role in a list",
            "access_token": {"token": {"username": "username", "roles": ["test-role1"], "groups": ["test-role1"]}},
            "id_token": {"token": {"sub": "sub-field"}},
            "config": {"role_mapping": {"test-role1": ["admin"], "test-role2": ["tester"]}, "username": "access:username", "use_id_token": False},
            "expected": {"authenticated": True, "roles": ["admin"], "username": "username"},
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
                "role_mapping": {"test-role2": ["tester"], "test-role4": ["analyst"]},
                "username": "access:username",
                "use_id_token": False,
            },
            "expected": {"authenticated": True, "roles": ["tester"], "username": "username"},
        },
        {
            "name": "multiple roles",
            "access_token": {
                "token": {
                    "username": "username",
                    "roles": ["test-role1", "test-role2", "test-role3"],
                    "groups": ["test-role1", "test-role2", "test-role3"],
                }
            },
            "id_token": {"token": {"sub": "sub-field"}},
            "config": {
                "role_mapping": {"test-role2": ["tester1", "tester2"], "test-role4": ["analyst1", "analyst2"]},
                "username": "access:username",
                "use_id_token": False,
            },
            "expected": {"authenticated": True, "roles": ["tester1", "tester2"], "username": "username"},
        },
        {
            "name": "overlapping multiple roles",
            "access_token": {
                "token": {
                    "username": "username",
                    "roles": ["test-role1", "test-role2", "test-role3"],
                    "groups": ["test-role1", "test-role2", "test-role3"],
                }
            },
            "id_token": {"token": {"sub": "sub-field"}},
            "config": {
                "role_mapping": {"test-role2": ["tester1", "tester2"], "test-role3": ["a", "tester2"], "test-role4": ["analyst1", "analyst2"]},
                "username": "access:username",
                "use_id_token": False,
            },
            "expected": {"authenticated": True, "roles": ["a", "tester1", "tester2"], "username": "username"},
        },
        {
            "name": "no_id_token",
            "access_token": {"token": {"username": "test-user", "roles": "admin-role", "groups": "admin-role"}},
            "id_token": None,
            "config": {"role_mapping": {"admin-role": ["admin"]}, "username": "access:username", "use_id_token": False},
            "expected": {"authenticated": True, "roles": ["admin"], "username": "test-user"},
        },
    ],
)
def test_invalid_tokens(scheme, case):
    config = dict(case["config"])
    config["role_field"] = "roles" if scheme != "oidc-okta" else "groups"

    access_token = dict(case["access_token"])
    id_token = dict(case["id_token"]) if config["use_id_token"] else None

    # case 2 has a lambda function as expected value
    expected = case["expected"](scheme) if callable(case["expected"]) else case["expected"]

    tokens = (access_token, id_token) if config.get("use_id_token", True) else (access_token,)
    result = process_tokens(tokens, config, scheme)

    if result["authenticated"]:
        result["roles"] = sorted(result["roles"])
    assert result == expected, f"Failed case: {case['name']}"


def test_parsing_role_mappings():
    mapping = "test-role1:admin,tester;test-role2:tester, toster; test-role3: toster,    taster, meister"

    result = _load_role_mappings(mapping)
    expected = {
        "test-role1": ["admin", "tester"],
        "test-role2": ["tester", "toster"],
        "test-role3": ["toster", "taster", "meister"],
    }
    assert result == expected, f"Expected {expected}, but got {result}"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
