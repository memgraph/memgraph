#!/usr/bin/env python3

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

"""
Loaded automatically by Python (tests/e2e is on PYTHONPATH) in every test process started by runner_parallel.py.
When MEMGRAPH_E2E_PORT_MAP is set, connections to a hardcoded local port are redirected to the port the runner gave
this worker. Covers the Python socket module (neo4j driver, gqlalchemy, urllib, requests, ...) and mgclient, which
is a C extension with its own sockets.
"""

import json
import os
import socket

PORT_MAP_ENV = "MEMGRAPH_E2E_PORT_MAP"
LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1", "0.0.0.0", "::"}


def _load_port_map():
    payload = os.getenv(PORT_MAP_ENV, "")
    if not payload:
        return {}
    try:
        parsed = json.loads(payload)
    except Exception:
        return {}
    if not isinstance(parsed, dict):
        return {}
    port_map = {}
    for key, value in parsed.items():
        try:
            port_map[int(key)] = int(value)
        except Exception:
            continue
    return port_map


def _host_is_local(host):
    return host in (None, "") or host in LOCAL_HOSTS


def _remap_address(address, port_map):
    # (host, port) for AF_INET, (host, port, flowinfo, scope_id) for AF_INET6.
    if not isinstance(address, tuple) or len(address) < 2 or not _host_is_local(address[0]):
        return address
    try:
        port = int(address[1])
    except Exception:
        return address
    if port not in port_map:
        return address
    return (address[0], port_map[port]) + tuple(address[2:])


def _patch_socket(port_map):
    original_connect = socket.socket.connect
    original_connect_ex = socket.socket.connect_ex

    def connect(self, address):
        return original_connect(self, _remap_address(address, port_map))

    def connect_ex(self, address):
        return original_connect_ex(self, _remap_address(address, port_map))

    socket.socket.connect = connect
    socket.socket.connect_ex = connect_ex


def _patch_mgclient(port_map):
    try:
        import mgclient
    except Exception:
        return

    original_connect = mgclient.connect

    def connect(*args, **kwargs):
        args = list(args)
        host = kwargs.get("host", args[0] if args else None)
        if "port" in kwargs:
            kwargs["port"] = _remap_address((host, kwargs["port"]), port_map)[1]
        elif len(args) >= 2:
            args[1] = _remap_address((host, args[1]), port_map)[1]
        return original_connect(*args, **kwargs)

    mgclient.connect = connect


def _apply_patches():
    port_map = _load_port_map()
    if not port_map:
        return
    _patch_socket(port_map)
    _patch_mgclient(port_map)


_apply_patches()
