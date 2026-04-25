"""
Test setup for migrate.py.

migrate.py imports a dozen third-party DB drivers at module load time; in a
test environment most of them may be absent. This conftest installs minimal
stubs into sys.modules so `import migrate` succeeds, and the tests themselves
patch specific symbols per backend.
"""

import os
import sys
import types

import pytest


def _stub(name):
    if name in sys.modules:
        return sys.modules[name]
    module = types.ModuleType(name)
    sys.modules[name] = module
    return module


def _install_mgp_stub():
    if "mgp" in sys.modules:
        return

    mgp = types.ModuleType("mgp")

    class _Record:
        def __init__(self, **fields):
            self.fields = fields

        def __repr__(self):
            return f"Record({self.fields!r})"

    def _record_factory(**fields):
        # migrate.py both constructs `mgp.Record(...)` (a record) and uses
        # `mgp.Record(row=mgp.Map)` as a type annotation. Both go through the
        # same callable; we just return an instance either way.
        return _Record(**fields)

    def _noop_add(*_args, **_kwargs):
        return None

    class _Subscriptable:
        """Supports `Nullable[X]` and similar type-annotation usage."""

        def __class_getitem__(cls, _item):
            return cls

    class _Nullable(_Subscriptable):
        pass

    mgp.Record = _record_factory
    mgp.Map = dict
    mgp.Any = object
    mgp.Nullable = _Nullable
    mgp.add_batch_read_proc = _noop_add
    mgp.add_batch_write_proc = _noop_add
    mgp.read_proc = lambda f: f
    mgp.write_proc = lambda f: f

    sys.modules["mgp"] = mgp


def _install_driver_stubs():
    # Each of these is imported unconditionally at the top of migrate.py.
    # We only need stubs sufficient to let `import migrate` succeed; tests
    # that exercise a specific backend patch the symbols they need.
    boto3_mod = _stub("boto3")
    boto3_mod.client = lambda *a, **kw: None

    duckdb_mod = _stub("duckdb")
    duckdb_mod.connect = lambda *a, **kw: None

    mysql_mod = _stub("mysql")
    mysql_connector = _stub("mysql.connector")
    mysql_mod.connector = mysql_connector
    mysql_connector.connect = lambda *a, **kw: None

    oracledb_mod = _stub("oracledb")
    oracledb_mod.connect = lambda *a, **kw: None

    psycopg2_mod = _stub("psycopg2")
    psycopg2_mod.connect = lambda *a, **kw: None

    pyarrow_mod = _stub("pyarrow")
    flight_mod = _stub("pyarrow.flight")
    pyarrow_mod.flight = flight_mod
    flight_mod.connect = lambda *a, **kw: None
    flight_mod.FlightCallOptions = lambda *a, **kw: None

    class _FlightDescriptor:
        @staticmethod
        def for_command(_q):
            return None

    flight_mod.FlightDescriptor = _FlightDescriptor

    pyodbc_mod = _stub("pyodbc")
    pyodbc_mod.connect = lambda *a, **kw: None

    gqla_mod = _stub("gqlalchemy")

    class _Memgraph:
        def __init__(self, *a, **kw):
            pass

        def execute_and_fetch(self, *a, **kw):
            return iter([])

        def close(self):
            pass

    gqla_mod.Memgraph = _Memgraph

    neo4j_mod = _stub("neo4j")

    class _Driver:
        def session(self, *a, **kw):
            return None

        def close(self):
            pass

    class _GraphDatabase:
        @staticmethod
        def driver(*a, **kw):
            return _Driver()

    neo4j_mod.GraphDatabase = _GraphDatabase
    neo4j_time = _stub("neo4j.time")
    neo4j_mod.time = neo4j_time

    class _Neo4jDate:
        pass

    class _Neo4jDateTime:
        pass

    neo4j_time.Date = _Neo4jDate
    neo4j_time.DateTime = _Neo4jDateTime

    requests_mod = _stub("requests")
    requests_mod.get = lambda *a, **kw: None


_install_mgp_stub()
_install_driver_stubs()

_MIGRATE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, "migrate.py"))


@pytest.fixture
def migrate_module():
    """Load migrate.py fresh for each test so module-level state dicts don't
    leak between tests. Loaded by explicit file path to avoid colliding with
    the `migrate` test directory name on sys.path."""
    import importlib.util

    sys.modules.pop("migrate_under_test", None)
    spec = importlib.util.spec_from_file_location("migrate_under_test", _MIGRATE_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules["migrate_under_test"] = module
    spec.loader.exec_module(module)
    return module
