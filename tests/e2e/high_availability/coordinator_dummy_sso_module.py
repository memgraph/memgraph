#!/usr/bin/python3
# Copyright 2026 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

# Dummy SSO auth module driven by the coordinator SSO path through the real Bolt handshake. Memgraph spawns this as a
# subprocess and talks to it over fds 1000 (read) / 1001 (write), one JSON object per line, exactly like the
# data-instance auth modules.
#
# The coordinator SSO path is scheme-agnostic, so OIDC / SAML / Kerberos are all handled identically here: the module
# just echoes back whatever roles the (dummy) identity-provider token asks for. The base64-decoded token is a
# comma-separated list of role names the "IdP" returns for the identity, which lets a test request any role set (or an
# error) without a real IdP:
#   - "bad_token" (or an empty / undecodable token) -> an authentication error response.
#   - otherwise                                     -> authenticated with the listed roles.
# Coordinator-side role existence and privileges are NOT decided here; the coordinator validates the returned role
# names against its Raft-replicated role set.

import base64
import io
import json

MEMGRAPH_CALL_ID_KEY = "memgraph_call_id"


def authenticate(scheme, response):
    try:
        token = base64.b64decode(response).decode("utf-8")
    except Exception:
        return {"authenticated": False, "errors": "malformed identity provider token"}

    if token in ("", "bad_token"):
        return {"authenticated": False, "errors": f"invalid identity provider token for scheme {scheme}"}

    roles = [role.strip() for role in token.split(",") if role.strip()]
    if not roles:
        return {"authenticated": False, "errors": "no roles in identity provider token"}

    return {"authenticated": True, "roles": roles, "username": f"{scheme}-user"}


if __name__ == "__main__":
    # I/O with Memgraph over the fixed communication file descriptors.
    input_stream = io.FileIO(1000, mode="r")
    output_stream = io.FileIO(1001, mode="w")
    while True:
        params = json.loads(input_stream.readline().decode("ascii"))
        call_id = params.pop(MEMGRAPH_CALL_ID_KEY, None)
        ret = authenticate(**params)
        if call_id is not None:
            ret[MEMGRAPH_CALL_ID_KEY] = call_id
        output_stream.write((json.dumps(ret) + "\n").encode("ascii"))
