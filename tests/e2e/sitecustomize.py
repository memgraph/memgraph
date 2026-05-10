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

import json
import os

PORT_MAP_ENV = "MEMGRAPH_E2E_PORT_MAP"
LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}


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

    normalized = {}
    for key, value in parsed.items():
        try:
            original = int(key)
            remapped = int(value)
        except Exception:
            continue
        normalized[original] = remapped
    return normalized


def _host_is_local(host):
    return host in (None, "") or host in LOCAL_HOSTS


def _remap_port(host, port, port_map):
    if port is None:
        return None
    if not _host_is_local(host):
        return port
    return port_map.get(port, port)


def _patch_mgclient_connect(port_map):
    try:
        import mgclient
    except Exception:
        return

    original_connect = mgclient.connect

    def patched_connect(*args, **kwargs):
        host = kwargs.get("host")
        if "port" in kwargs:
            try:
                kwargs["port"] = _remap_port(host, int(kwargs["port"]), port_map)
            except Exception:
                pass
        elif len(args) >= 2:
            mutable_args = list(args)
            host = mutable_args[0]
            try:
                mutable_args[1] = _remap_port(host, int(mutable_args[1]), port_map)
                args = tuple(mutable_args)
            except Exception:
                pass
        return original_connect(*args, **kwargs)

    mgclient.connect = patched_connect


def _patch_gqlalchemy_memgraph(port_map):
    try:
        from gqlalchemy import Memgraph
    except Exception:
        return

    original_init = Memgraph.__init__

    def patched_init(self, *args, **kwargs):
        mutable_args = list(args)

        host = kwargs.get("host")
        if host is None and len(mutable_args) >= 1:
            host = mutable_args[0]
        if host is None:
            host = "127.0.0.1"

        if "port" in kwargs:
            try:
                kwargs["port"] = _remap_port(host, int(kwargs["port"]), port_map)
            except Exception:
                pass
        elif len(mutable_args) >= 2:
            try:
                mutable_args[1] = _remap_port(host, int(mutable_args[1]), port_map)
            except Exception:
                pass
        else:
            mutable_port = _remap_port(host, 7687, port_map)
            if mutable_port != 7687:
                kwargs["port"] = mutable_port
            if len(mutable_args) == 0 and "host" not in kwargs:
                kwargs["host"] = host

        return original_init(self, *tuple(mutable_args), **kwargs)

    Memgraph.__init__ = patched_init


def _apply_patches():
    port_map = _load_port_map()
    if not port_map:
        return
    _patch_mgclient_connect(port_map)
    _patch_gqlalchemy_memgraph(port_map)


_apply_patches()
