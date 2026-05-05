# Copyright 2023 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import pytest
from gqlalchemy import Memgraph


@pytest.fixture
def memgraph(**kwargs) -> Memgraph:
    memgraph = Memgraph()

    yield memgraph

    memgraph.drop_indexes()
    memgraph.ensure_constraints([])
    memgraph.drop_database()

    try:
        memgraph.execute("DROP USER mrma;")
    except Exception:
        pass
    try:
        memgraph.execute("DROP USER sha256;")
    except Exception:
        pass
    try:
        memgraph.execute("DROP USER sha256_multiple;")
    except Exception:
        pass
    try:
        memgraph.execute("DROP USER bcrypt;")
    except Exception:
        pass
    try:
        memgraph.execute("DROP ROLE mrma;")
    except Exception:
        pass
    try:
        memgraph.execute("DROP ROLE admin;")
    except Exception:
        pass
    try:
        memgraph.execute("DROP ROLE readwrite;")
    except Exception:
        pass
    try:
        memgraph.execute("DROP ROLE readonly;")
    except Exception:
        pass
    try:
        memgraph.execute("DROP PROFILE profile;")
    except Exception:
        pass
    try:
        memgraph.execute("DROP PROFILE Profile;")
    except Exception:
        pass
    try:
        memgraph.execute("DROP PROFILE profile2;")
    except Exception:
        pass


@pytest.fixture
def provide_user() -> Memgraph:
    memgraph = Memgraph()

    # Create superuser first so anthony is not the first user and gets no builtin role
    memgraph.execute("CREATE USER superuser IDENTIFIED BY 'superpassword';")
    memgraph.execute("CREATE USER anthony IDENTIFIED BY 'password';")

    yield None

    memgraph = Memgraph(username="superuser", password="superpassword")
    memgraph.execute("DROP USER anthony;")
    memgraph.execute("DROP USER superuser;")
