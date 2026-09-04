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
When the runner hands the process a port window (see PortRemap in memgraph.py), client connections to a hardcoded
local port are redirected to the remapped port, ports inside queries are rewritten on the way out and mapped back in
the results, so tests keep working with the ports they were written with. Covers the Python socket module (neo4j
driver, urllib, requests, ...) and mgclient, which is a C extension with its own sockets.
"""

import socket

try:
    from memgraph import LOCAL_HOSTS, PORT_REMAP
except Exception:  # Not an e2e test process.
    PORT_REMAP = None


def _remap_address(address, allocate):
    """
    (host, port) for AF_INET, (host, port, flowinfo, scope_id) for AF_INET6. Binding allocates a window port, so a
    mock server a test starts moves along with its clients. Connecting only looks up ports that an instance start, a
    bind or a query already mapped, so a local service nothing here started (e.g. Kafka) keeps its real port.
    """
    if not isinstance(address, tuple) or len(address) < 2 or (address[0] not in LOCAL_HOSTS and address[0] != ""):
        return address
    if not isinstance(address[1], int):
        return address
    port = PORT_REMAP.map_port(address[1]) if allocate else PORT_REMAP.lookup_port(address[1])
    return (address[0], port) + tuple(address[2:])


def _patch_socket():
    original_connect = socket.socket.connect
    original_connect_ex = socket.socket.connect_ex
    original_bind = socket.socket.bind

    def connect(self, address):
        return original_connect(self, _remap_address(address, allocate=False))

    def connect_ex(self, address):
        return original_connect_ex(self, _remap_address(address, allocate=False))

    def bind(self, address):
        return original_bind(self, _remap_address(address, allocate=True))

    socket.socket.connect = connect
    socket.socket.connect_ex = connect_ex
    socket.socket.bind = bind


class _CursorProxy:
    """Rewrites ports in executed queries and maps them back in fetched rows."""

    def __init__(self, cursor):
        object.__setattr__(self, "_cursor", cursor)

    def execute(self, query, *args, **kwargs):
        return self._cursor.execute(PORT_REMAP.map_text(query), *args, **kwargs)

    def fetchall(self):
        return PORT_REMAP.unmap_value(self._cursor.fetchall())

    def fetchone(self):
        return PORT_REMAP.unmap_value(self._cursor.fetchone())

    def fetchmany(self, *args, **kwargs):
        return PORT_REMAP.unmap_value(self._cursor.fetchmany(*args, **kwargs))

    def __iter__(self):
        return iter(self.fetchall())

    def __getattr__(self, name):
        return getattr(self._cursor, name)

    def __setattr__(self, name, value):
        setattr(self._cursor, name, value)


class _ConnectionProxy:
    def __init__(self, connection):
        object.__setattr__(self, "_connection", connection)

    def cursor(self, *args, **kwargs):
        return _CursorProxy(self._connection.cursor(*args, **kwargs))

    def __getattr__(self, name):
        return getattr(self._connection, name)

    def __setattr__(self, name, value):
        setattr(self._connection, name, value)


def _patch_mgclient():
    try:
        import mgclient
    except Exception:
        return

    original_connect = mgclient.connect

    def connect(*args, **kwargs):
        args = list(args)
        host = kwargs.get("host", args[0] if args else None)
        if "port" in kwargs:
            kwargs["port"] = _remap_address((host, kwargs["port"]), allocate=False)[1]
        elif len(args) >= 2:
            args[1] = _remap_address((host, args[1]), allocate=False)[1]
        return _ConnectionProxy(original_connect(*args, **kwargs))

    mgclient.connect = connect


def _patch_neo4j():
    """Only the query text is rewritten. Results are not mapped back, unlike mgclient's, so tests that assert on
    endpoint strings have to read them through mgclient."""
    try:
        import neo4j
    except Exception:
        return

    def wrap_run(cls):
        original_run = cls.run

        def run(self, query, *args, **kwargs):
            return original_run(self, PORT_REMAP.map_text(query), *args, **kwargs)

        cls.run = run

    for name in ("Session", "Transaction", "ManagedTransaction"):
        if hasattr(neo4j, name):
            wrap_run(getattr(neo4j, name))


if PORT_REMAP is not None and PORT_REMAP.active:
    _patch_socket()
    _patch_mgclient()
    _patch_neo4j()
