#!/usr/bin/python3
import base64
import io
import json


def authenticate(scheme: str, response: str):
    response = base64.b64decode(response).decode("utf-8")

    if response == "send_error":
        return {
            "authenticated": False,
            "errors": f'[Scheme: {scheme}] The "Memgraph.Architect" role is not present in the given role mappings.',
        }

    if response == "wrong_fields":
        return {
            "authenticated": True,
            "usernameee": 1234,
            "rooole": 2345,
        }

    # Default single role response
    return_dict = {
        "authenticated": True,
        "role": "architect",
        "username": "anthony",
    }

    if response == "wrong_value_types":
        return_dict["role"] = 1234
        return_dict["username"] = 2345

    if response == "nonexistent_role":
        return_dict["role"] = "thisRoleDoesntExist"

    if response == "skip_username":
        return_dict.pop("username")

    # New multi-role interface - roles is just a list of role names
    if response == "multi_role_admin":
        return_dict = {"authenticated": True, "username": "admin_user", "roles": ["admin", "architect", "user"]}

    if response == "multi_role_architect":
        return_dict = {"authenticated": True, "username": "architect_user", "roles": ["architect", "user"]}

    if response == "multi_role_user":
        return_dict = {"authenticated": True, "username": "regular_user", "roles": ["user"]}

    if response == "multi_role_readonly":
        return_dict = {"authenticated": True, "username": "readonly_user", "roles": ["readonly"]}

    if response == "multi_role_limited":
        return_dict = {"authenticated": True, "username": "limited_user", "roles": ["limited"]}

    if response == "multi_role_no_main":
        return_dict = {"authenticated": True, "username": "no_main_user", "roles": ["user", "architect"]}

    if response == "multi_role_invalid_db":
        return_dict = {"authenticated": True, "username": "invalid_db_user", "roles": ["user"]}

    if response == "multi_role_wrong_types":
        return_dict = {
            "authenticated": True,
            "username": "wrong_types_user",
            "roles": [1234, 5678],  # Wrong types for role names
        }

    return return_dict


if __name__ == "__main__":
    # I/O with Memgraph
    input_stream = io.FileIO(1000, mode="r")
    output_stream = io.FileIO(1001, mode="w")
    while True:
        params = json.loads(input_stream.readline().decode("ascii"))
        ret = authenticate(**params)
        output_stream.write((json.dumps(ret) + "\n").encode("ascii"))
