#!/usr/bin/python3
import io
import json

from common import add_call_id_to_response, pop_call_id


def authenticate(username, password):
    return {"authenticated": True, "role": ""}


if __name__ == "__main__":
    # Stateless: each request is independent. Do not cache tokens or user data,
    # or the next connection may get the previous user (wrong-user bug).
    input_stream = io.FileIO(1000, mode="r")
    output_stream = io.FileIO(1001, mode="w")
    while True:
        params = json.loads(input_stream.readline().decode("ascii"))
        call_id = pop_call_id(params)
        ret = authenticate(**params)
        add_call_id_to_response(ret, call_id)
        output_stream.write((json.dumps(ret) + "\n").encode("ascii"))
