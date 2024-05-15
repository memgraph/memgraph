#!/usr/bin/python3
import base64
import io
import json


def authenticate(response: str):
    response = base64.b64decode(response).decode("utf-8")

    if response == "send_error":
        return {
            "authenticated": False,
            "errors": 'The "Memgraph.Architect" role is not present in the given role mappings.',
        }

    if response == "wrong_fields":
        return {
            "authenticated": True,
            "usernameee": 1234,
            "rooole": 2345,
        }

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

    return return_dict


if __name__ == "__main__":
    # I/O with Memgraph
    input_stream = io.FileIO(1000, mode="r")
    output_stream = io.FileIO(1001, mode="w")
    while True:
        params = json.loads(input_stream.readline().decode("ascii"))
        ret = authenticate(**params)
        output_stream.write((json.dumps(ret) + "\n").encode("ascii"))
