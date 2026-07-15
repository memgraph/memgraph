#!/usr/bin/env python3
"""diag — read-only Memgraph diagnostic TIMESERIES logger (grounded at v3.10.1).

WHAT THIS IS: a background poller that logs a complete timeseries of Memgraph's own
SHOW/INFO query results (plus optional OS/cgroup and HTTP metrics signals) to JSONL, so
a customer's engineer -- or an LLM handed the output -- can graph and analyze it
afterward. It draws NO conclusions and performs NO diagnosis: it never decides "this is
a leak" or "this is GC pinning", it only measures and records. The only "derived" values
it computes are plain arithmetic on the raw data (a memory-attribution gap, a
per-interval rate, a sum across tenants, ...), never a judgment about what that
arithmetic means. It is also entirely READ-ONLY: it never writes to the target database
and never changes its configuration.

WHAT IT COLLECTS, via three independently-optional collectors (each enabled only if its
flags are given):

  1. Bolt collector (primary) -- read-only `SHOW ...` admin queries over Bolt:
       SHOW VERSION, SHOW BUILD INFO, SHOW CONFIG, SHOW DATABASES, SHOW REPLICATION
       ROLE, SHOW REPLICAS, SHOW TRIGGERS                    (inventory, infrequent)
       SHOW STORAGE INFO                        (instance-wide fields; every tick)
       SHOW STORAGE INFO ON DATABASE <name>, per tenant       (every tick)
       SHOW INDEX INFO / SHOW CONSTRAINT INFO, per tenant     (inventory, infrequent;
         via `USE DATABASE <name>` -- these two queries have no ON DATABASE variant)
       SHOW TRANSACTIONS, SHOW METRICS INFO, SHOW MEMORY INFO (instance-wide; every tick)
     By default polls EVERY tenant returned by SHOW DATABASES -- a customer's real data
     commonly lives in a non-default tenant, not `memgraph` -- restrict with
     --databases. Requires the `neo4j` package, imported lazily and only if
     --bolt-address is given: pip install neo4j
  2. HTTP metrics collector (secondary, optional) -- GETs a JSON metrics endpoint (e.g.
     Memgraph's Enterprise --metrics-port) and flattens it to a flat name->value map.
     Disables itself with a one-line notice if license-gated/unreachable (SHOW METRICS
     INFO over Bolt already covers the same counters).
  3. OS collector (opt-in) -- best-effort /proc/<pid>/status (VmRSS/VmData/Threads),
     /proc/<pid>/fd count, cgroup v1/v2 memory.{current,max}, /proc/meminfo, and
     dmesg/journalctl -k for OOM-killer lines. Meant to run inside the memgraph
     pod/container/namespace, alongside the server it's watching.

OUTPUT ARTIFACTS (in --output-dir, default: current directory):
  diag_<UTC-ts>.jsonl   the timeseries: one JSON object per tick -- see the JSONL
                          RECORD SCHEMA comment directly below this docstring
  diag_<UTC-ts>.log     one plain `key=value` line per tick, mirroring that same
                          tick's JSONL row -- no interpretation, no editorializing
  diag_crashes.log      CRASH_DETECTED events (a restart was observed) plus the
                          preceding N ticks for later inspection -- still just data,
                          not a claim about WHY it restarted

MODES: normal operation loops forever (Ctrl-C / SIGTERM triggers a clean shutdown), or:
  --once              take a single tick, print its JSON record to stdout, and exit
  --to-csv PATH.jsonl mechanically flatten a completed timeseries into a wide CSV
                      (one row per tick) for graphing -- see --csv-out; no analysis
  --selftest          run built-in checks against synthetic v3.10.1-shaped data (no
                      live server needed); proves the field map + tenant-correct
                      arithmetic, not any interpretation (there is none to test)
Run `diag.py --help` for the full grouped option reference and copy-paste examples.

FILE MAP (this file, top to bottom):
  Constants                    query catalog + version-aware field-candidate tables
  Small generic helpers        size-string/number parsing, /proc reading, JSON flatten
  Bolt collector               BoltCollector + collect_inventory/collect_sample/
                                collect_per_tenant_storage/collect_per_tenant_schema
  HTTP metrics collector       MetricsHttpCollector
  OS collector                 OsCollector
  Derived signal tracking      TxnAgeTracker, RateTracker
  Poller state + main loop     PollerState, _collect_bolt_tick, _compute_derived,
                                _emit_human_line, run_loop, _run_once
  --to-csv mode                mechanical JSONL -> wide-CSV flattening, no analysis
  --selftest mode               synthetic-data checks for the field map + tenant math
  Logging / banner / CLI       argparse (grouped, with usage examples), main()

Safety: every collector call is wrapped so a single failure is logged and the loop
continues; all queries are read-only with a bounded per-query timeout; SIGINT/SIGTERM
trigger a clean shutdown after the in-flight tick.
"""

# ------------------------------------------------------------------------------------------
# JSONL RECORD SCHEMA -- the shape of ONE line in a diag_<ts>.jsonl timeseries file.
# This is the single most useful reference for anything (human or LLM) parsing the output.
# Every field below is either a raw SHOW-query result or a plain arithmetic MEASUREMENT
# on that raw data -- none of it is a conclusion, verdict, severity, or diagnosis.
#
# {
#   "ts": "2026-07-15T09:10:28+00:00",   # UTC, ISO-8601, second precision
#   "tick": 42,                          # 1-based sample counter for this run
#   "bolt": {                            # present iff --bolt-address was given
#     "connected": true,                 # Bolt connection state THIS tick
#     "errors": {"<query_name>": "<error text>", ...},  # per-query failures, if any --
#                                         # a failure here never aborts the tick, see
#                                         # _safe_query()/collect_sample()
#     "crash_events": [...],             # CRASH_DETECTED events that fired this tick
#                                         # (also appended verbatim to diag_crashes.log)
#     "sample": {                        # every tick: instance-wide + per-tenant reads
#       "storage_info_global": {...},        # raw SHOW STORAGE INFO (bare) row, flattened
#                                             # to {metric_name: value}; see rows_to_kv_dict
#       "storage_info_by_db": {              # raw per-tenant row, one per polled DB:
#         "<db_name>": {...} | null,         #   SHOW STORAGE INFO ON DATABASE <name>
#       },                                   #   (or the USE DATABASE fallback -- see
#                                            #   collect_per_tenant_storage)
#       "transactions": [...],               # raw SHOW TRANSACTIONS rows (instance-wide)
#       "metrics": {"<name>": <value>, ...},  # raw SHOW METRICS INFO, flattened
#       "memory_info": [...],                 # raw SHOW MEMORY INFO rows
#     },
#     "inventory": {...} | absent,       # only present on an inventory-cadence tick:
#                                         # version/build_info/config/databases/
#                                         # replication_role/replicas/triggers (all
#                                         # instance-wide) + per_tenant_schema (per-DB
#                                         # SHOW INDEX INFO/SHOW CONSTRAINT INFO)
#   },
#   "metrics_http": {...} | null,        # flattened HTTP metrics endpoint JSON, if enabled
#   "os": {...} | null,                  # /proc + cgroup + meminfo reads, if --os
#   "timings_s": {"bolt": 0.03, ...},    # wall-clock seconds spent in each collector
#   "derived": {                         # MEASUREMENTS computed from the raw data above
#                                         # (pure arithmetic -- see _compute_derived())
#     "version": {"raw": "3.10.1", "major": 3, "minor": 10},   # from SHOW VERSION
#     "resolved_fields": {"storage.total_memory_tracked": "global_memory_tracked", ...},
#                                         # which PHYSICAL field name backed each logical
#                                         # measurement this tick -- field names drift by
#                                         # server version, see resolve_fields()
#     "total_memory_tracked_bytes": 48097661337,     # = global_memory_tracked, instance-wide,
#                                                     #   from the bare SHOW STORAGE INFO row
#     "runtime_limit_bytes": 121578924851,           # instance-wide --memory-limit-ish figure
#     "memory_res_bytes": 48214619340,               # instance-wide process RSS as tracked
#     "memory_vs_runtime_limit_pct": 40.00,          # = memory_res / runtime_limit * 100
#     "tenant_total_memory_bytes_by_db": {"<db>": 19468061818, ...},  # per-tenant
#                                                     # tenant_memory_tracked (storage + query +
#                                                     # embedding combined -- the DB's full total)
#     "tenant_total_memory_bytes_sum": 19468061818,  # sum of the above across all tenants
#     "non_storage_gap_bytes": 28629599519,          # = total_memory_tracked_bytes minus
#                                                     #   tenant_total_memory_bytes_sum: process-
#                                                     #   global memory NOT attributed to any
#                                                     #   database. On v3.10.1 there is no
#                                                     #   dedicated per-DB or dedicated AST-cache
#                                                     #   counter -- this figure is dominated by
#                                                     #   the AST/query-plan cache but also
#                                                     #   includes Bolt buffers, thread stacks,
#                                                     #   RPC, etc. A MEASUREMENT, not a claim
#                                                     #   about which of those is responsible.
#     "counter_deltas": {"ReadQuery": {"delta": 5, "rate_per_s": 2.5}, ...},
#                                         # per-interval delta/rate for each monotonic
#                                         # counter (see MONOTONIC_COUNTER_CANDIDATES);
#                                         # null until the 2nd tick (needs a previous value)
#     "counters_reset": ["SuccessfulQuery", ...],  # counters that went BACKWARDS this
#                                         # tick vs last tick -- a restart signal, see
#                                         # RateTracker.update()
#     "active_txn_count": 3,             # len(SHOW TRANSACTIONS) this tick
#     "longest_txn_age_s": 812.4,        # max(now - first_seen) across active
#                                         # transaction_ids (or a native start_time field,
#                                         # if the server provides one) -- see TxnAgeTracker
#     "longest_txn_query": "MATCH (n) ...",  # query text of the oldest active transaction
#     "used_native_txn_start_time": false,   # true iff a native start_time was available
#     "strict_sync_replicas": ["replica1"],  # names with sync_mode == STRICT_SYNC -- a raw
#                                             # passthrough of SHOW REPLICAS, not a flag
#     "per_tenant": {                    # one entry per polled DB, combining this tick's
#       "<db_name>": {                   # storage sweep with the last inventory sweep's
#         "graph_memory_tracked_bytes": 19468061818,        # storage bucket
#         "query_memory_tracked_bytes": 5570,               # query bucket        } all four
#         "vector_index_memory_tracked_bytes": 0,           # embedding bucket    } buckets are
#         "tenant_memory_tracked_bytes": 19468061818,       # = graph+query+embedding (the
#                                                            #   DB's full tracked total --
#                                                            #   this is what non_storage_gap
#                                                            #   sums across tenants)
#         "vertex_count": 1000000,
#         "edge_count": 0,
#         "unreleased_delta_objects": 0,
#         "index_count": 2,              # len(SHOW INDEX INFO) while switched to this DB
#         "constraint_count": 1,         # len(SHOW CONSTRAINT INFO) while switched to it
#       },
#     },
#     "vertex_count_total": 1000000,    # sum of vertex_count across every tenant
#     "edge_count_total": 0,             # sum of edge_count across every tenant
#     "rss_vs_cgroup_max_pct": 41.2,     # = os.vm_rss_kb*1024 / os.cgroup_memory_max_bytes
#                                        # * 100 -- only present if --os and both readable
#   },
# }
# ------------------------------------------------------------------------------------------

from __future__ import annotations

import argparse
import concurrent.futures
import contextlib
import csv
import json
import logging
import os
import re
import signal
import subprocess
import sys
import threading
import time
from collections import deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# --------------------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------------------

PROG_NAME = "diag"

DEFAULT_INTERVAL_S = 20.0
DEFAULT_INVENTORY_INTERVAL_S = 300.0
DEFAULT_QUERY_TIMEOUT_S = 10.0
DEFAULT_CRASH_HISTORY = 30
DEFAULT_CRASH_OUTAGE_THRESHOLD_S = 60.0

RECONNECT_BACKOFF_START_S = 1.0
RECONNECT_BACKOFF_CAP_S = 60.0

# Monotonic (process-lifetime) counters from SHOW METRICS INFO that we diff per tick.
# Mapped logical-name -> ordered candidate physical names, since a counter can be
# renamed across server versions (e.g. RollbackedTransactions -> RolledBackTransactions
# between v3.10.x and v3.11.x). First match wins; see resolve_fields()/RateTracker.
MONOTONIC_COUNTER_CANDIDATES: dict[str, tuple[str, ...]] = {
    "ReadQuery": ("ReadQuery",),
    "WriteQuery": ("WriteQuery",),
    "ReadWriteQuery": ("ReadWriteQuery",),
    "SuccessfulQuery": ("SuccessfulQuery",),
    "FailedQuery": ("FailedQuery",),
    "DeletedNodes": ("DeletedNodes",),
    "DeletedEdges": ("DeletedEdges",),
    "CommitedTransactions": ("CommitedTransactions",),
    "RollbackedTransactions": ("RollbackedTransactions", "RolledBackTransactions"),
    "WriteWriteConflicts": ("WriteWriteConflicts",),
    "MergeOperator": ("MergeOperator",),
    "SetPropertiesOperator": ("SetPropertiesOperator",),
    "PeriodicCommitOperator": ("PeriodicCommitOperator",),
}

# Instance-wide (truly global, regardless of "current DB") logical fields from the bare
# `SHOW STORAGE INFO` (no ON DATABASE/USE DATABASE involved) — grounded field names per
# version below. v3.10.x is the PRIMARY target (the customer's pinned version); v3.11.x
# support exists only because that's the sibling binary available for e2e validation.
#   logical                v3.10.x (PRIMARY)                    v3.11.x
#   total_memory_tracked    global_memory_tracked                memory_tracked
#   runtime_limit           global_runtime_allocation_limit      memory_limit
#   license_limit           global_license_allocation_limit      license_memory_limit
#   memory_res              memory_res                           memory_res
#   peak_memory_res         peak_memory_res                      peak_memory_res
#   storage_mode            storage_mode                         global_storage_mode
GLOBAL_STORAGE_FIELD_CANDIDATES: dict[str, tuple[str, ...]] = {
    "total_memory_tracked": ("global_memory_tracked", "memory_tracked"),
    "runtime_limit": ("global_runtime_allocation_limit", "memory_limit"),
    "license_limit": ("global_license_allocation_limit", "license_memory_limit"),
    "memory_res": ("memory_res",),
    "peak_memory_res": ("peak_memory_res",),
    "storage_mode": ("storage_mode", "global_storage_mode"),
}

# Per-tenant logical fields. PRIMARY source is the dedicated `SHOW STORAGE INFO ON
# DATABASE <name>` query, whose field names are STABLE across v3.10.1 and v3.11.x (both
# grounded + empirically confirmed against the v3.11 sibling): graph_memory_tracked
# (storage), query_memory_tracked, vector_index_memory_tracked (embedding),
# tenant_memory_tracked (= the DB's full tracked total: storage + query + embedding).
# FALLBACK source (only if that query errors on some build) is `USE DATABASE <name>`
# then bare `SHOW STORAGE INFO` -- at that point the bare query's "current DB" fields
# happen to use a DIFFERENT naming scheme (db_storage_memory_tracked/
# db_query_memory_tracked/db_embedding_memory_tracked/db_memory_tracked), which is why
# each logical field below carries both names as candidates.
PER_TENANT_STORAGE_FIELD_CANDIDATES: dict[str, tuple[str, ...]] = {
    "graph_memory_tracked": ("graph_memory_tracked", "db_storage_memory_tracked"),
    "query_memory_tracked": ("query_memory_tracked", "db_query_memory_tracked"),
    "vector_index_memory_tracked": ("vector_index_memory_tracked", "db_embedding_memory_tracked"),
    "tenant_memory_tracked": ("tenant_memory_tracked", "db_memory_tracked"),
    "vertex_count": ("vertex_count",),
    "edge_count": ("edge_count",),
    "unreleased_delta_objects": ("unreleased_delta_objects",),
}

# Cypher query catalog, verbatim per the v3.10.1 grounded catalog. Do not "upgrade" syntax.
Q_SHOW_VERSION = "SHOW VERSION"
Q_SHOW_BUILD_INFO = "SHOW BUILD INFO"
Q_SHOW_CONFIG = "SHOW CONFIG"
Q_SHOW_DATABASES = "SHOW DATABASES"
Q_SHOW_INDEX_INFO = "SHOW INDEX INFO"
Q_SHOW_CONSTRAINT_INFO = "SHOW CONSTRAINT INFO"
Q_SHOW_REPLICATION_ROLE = "SHOW REPLICATION ROLE"
Q_SHOW_REPLICAS = "SHOW REPLICAS"
Q_SHOW_TRIGGERS = "SHOW TRIGGERS"
Q_SHOW_STORAGE_INFO = "SHOW STORAGE INFO"
Q_SHOW_TRANSACTIONS = "SHOW TRANSACTIONS"
Q_SHOW_METRICS_INFO = "SHOW METRICS INFO"
Q_SHOW_MEMORY_INFO = "SHOW MEMORY INFO"

LOGGER_NAME = "diag"

# --------------------------------------------------------------------------------------
# Small generic helpers
# --------------------------------------------------------------------------------------

_SIZE_UNITS_BINARY = {
    "b": 1,
    "kib": 1024,
    "mib": 1024**2,
    "gib": 1024**3,
    "tib": 1024**4,
    "pib": 1024**5,
}
_SIZE_UNITS_DECIMAL = {
    "kb": 1000,
    "mb": 1000**2,
    "gb": 1000**3,
    "tb": 1000**4,
    "pb": 1000**5,
}
_SIZE_RE = re.compile(r"^\s*([0-9]*\.?[0-9]+)\s*([A-Za-z]*)\s*$")


def parse_size_to_bytes(value: Any) -> int | None:
    """Parse a human-readable size ("40.00GiB", "128", 128) into an int byte count.

    Returns None if the value can't be interpreted as a size at all — callers must
    tolerate that (field renamed/missing/changed type across server versions).
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    match = _SIZE_RE.match(text)
    if not match:
        return None
    number_part, unit_part = match.groups()
    try:
        number = float(number_part)
    except ValueError:
        return None
    unit = unit_part.strip().lower()
    if not unit or unit == "b":
        return int(number)
    if unit in _SIZE_UNITS_BINARY:
        return int(number * _SIZE_UNITS_BINARY[unit])
    if unit in _SIZE_UNITS_DECIMAL:
        return int(number * _SIZE_UNITS_DECIMAL[unit])
    return None


def _to_number(value: Any) -> float | None:
    """Best-effort coercion of a metric value to float; None if not numeric."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def _human_bytes(n: int | float | None) -> str:
    """Render a byte count as a human-readable binary-unit string (e.g. 25.00GiB), for
    log lines only -- the JSONL always keeps the raw integer byte counts."""
    if n is None:
        return "?"
    value = float(n)
    sign = "-" if value < 0 else ""
    value = abs(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024.0:
            return f"{sign}{value:.2f}{unit}"
        value /= 1024.0
    return f"{sign}{value:.2f}PiB"


def _find_key(row: Mapping[str, Any], candidates: Sequence[str]) -> str | None:
    """Case-insensitive lookup of the first matching key present in `row`."""
    lowered = {k.lower(): k for k in row.keys()}
    for candidate in candidates:
        found = lowered.get(candidate.lower())
        if found is not None:
            return found
    return None


def rows_to_kv_dict(
    rows: Sequence[Mapping[str, Any]] | None,
    name_candidates: Sequence[str],
    value_candidates: Sequence[str],
) -> dict[str, Any]:
    """Flatten a key/value-shaped result table (e.g. SHOW STORAGE INFO: one row per
    metric, columns like "storage info"/"value") into a single {metric_name: value} dict.
    Tolerant of exact column-name drift: falls back to first/last column positionally.
    """
    flat: dict[str, Any] = {}
    if not rows:
        return flat
    for row in rows:
        name_key = _find_key(row, name_candidates)
        value_key = _find_key(row, value_candidates)
        keys = list(row.keys())
        if name_key is None or value_key is None:
            if len(keys) < 2:
                continue
            name_key = name_key or keys[0]
            value_key = value_key or keys[-1]
        name = row.get(name_key)
        if name is None:
            continue
        flat[str(name)] = row.get(value_key)
    return flat


def quote_symbolic_name(name: str) -> str:
    """Backtick-quote a Cypher symbolic name (e.g. a database name from SHOW DATABASES)."""
    escaped = name.replace("`", "``")
    return f"`{escaped}`"


@dataclass(slots=True)
class VersionInfo:
    """Parsed `SHOW VERSION` result. `major`/`minor` are None when the version string
    couldn't be parsed at all (e.g. the query failed) -- callers must tolerate that and
    fall back to the v3.10.1-first field ordering (see _ordered_candidates)."""

    raw: str | None = None
    major: int | None = None
    minor: int | None = None


def parse_version(rows: list[dict[str, Any]] | None) -> VersionInfo:
    """Parse the (major, minor) out of a `SHOW VERSION` result row, tolerating a
    leading "v" and any patch/build suffix (e.g. "v3.11.0+69~a0033f7a758e")."""
    if not rows:
        return VersionInfo()
    key = _find_key(rows[0], ("version",))
    raw = rows[0].get(key) if key else None
    if raw is None:
        return VersionInfo()
    match = re.match(r"v?(\d+)\.(\d+)", str(raw))
    if not match:
        return VersionInfo(raw=str(raw))
    return VersionInfo(raw=str(raw), major=int(match.group(1)), minor=int(match.group(2)))


def _ordered_candidates(candidates: Sequence[str], version: VersionInfo) -> tuple[str, ...]:
    """v3.10.x is the PRIMARY target, so `candidates` is authored v3.10.x-name-first.
    For a detected version >= 3.11 (or a genuinely unknown/future version, since we'd
    rather guess the newer shape than the older one) try the later name first --
    but always keep every candidate as a fallback so an unrecognized patch build never
    silently resolves to None just because of field-name drift."""
    newer_first = version.major is None or (version.major, version.minor or 0) >= (3, 11)
    if not newer_first:
        return tuple(candidates)
    ordered: list[str] = []
    for candidate in tuple(reversed(candidates)) + tuple(candidates):
        if candidate not in ordered:
            ordered.append(candidate)
    return tuple(ordered)


def resolve_fields(
    row: Mapping[str, Any], candidates_map: Mapping[str, Sequence[str]], version: VersionInfo
) -> tuple[dict[str, Any], dict[str, str | None]]:
    """Resolve a set of logical field names against a flattened result row, trying the
    version-preferred physical name first and falling back across all candidates.
    Returns (logical_name -> raw_value, logical_name -> physical_key_used_or_None) so
    callers can report exactly which field backed each derived number."""
    values: dict[str, Any] = {}
    resolved: dict[str, str | None] = {}
    for logical, candidates in candidates_map.items():
        chosen_key: str | None = None
        chosen_value: Any = None
        for candidate in _ordered_candidates(candidates, version):
            key = _find_key(row, (candidate,))
            if key is not None and row.get(key) is not None:
                chosen_key = key
                chosen_value = row.get(key)
                break
        values[logical] = chosen_value
        resolved[logical] = chosen_key
    return values, resolved


def _iso_now() -> str:
    """UTC timestamp for a single tick's "ts" field: ISO-8601, second precision."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _utc_ts_compact() -> str:
    """UTC timestamp used in output filenames (diag_<this>.jsonl/.log) -- compact,
    filesystem-safe, and naturally sortable."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _flatten_json(payload: Any, prefix: str = "") -> dict[str, Any]:
    """Flatten an arbitrarily-nested JSON payload into a flat {dotted.path: value} dict."""
    flat: dict[str, Any] = {}
    if isinstance(payload, Mapping):
        for key, value in payload.items():
            new_prefix = f"{prefix}.{key}" if prefix else str(key)
            if isinstance(value, (Mapping, list)):
                flat.update(_flatten_json(value, new_prefix))
            else:
                flat[new_prefix] = value
    elif isinstance(payload, list):
        for idx, item in enumerate(payload):
            new_prefix = f"{prefix}[{idx}]"
            if isinstance(item, (Mapping, list)):
                flat.update(_flatten_json(item, new_prefix))
            else:
                flat[new_prefix] = item
    else:
        flat[prefix or "value"] = payload
    return flat


def _read_text_file(path: Path) -> str | None:
    """Best-effort read of a /proc or /sys/fs/cgroup pseudo-file: None (never an
    exception) on any OSError, since these paths can vanish or be permission-denied at
    any moment (the target process can exit mid-read) -- callers must tolerate that."""
    try:
        return path.read_text().strip()
    except OSError:
        return None


def _read_int_file(path: Path) -> int | None:
    """Like _read_text_file, but for a pseudo-file that holds a single integer (e.g.
    cgroup memory.current/memory.usage_in_bytes)."""
    text = _read_text_file(path)
    if text is None:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _to_int(text: str | None) -> int | None:
    """Parse a string to int, tolerating None and non-numeric text (e.g. cgroup v2's
    literal string "max" for "no limit", handled by the caller before this is reached)."""
    if text is None:
        return None
    try:
        return int(text)
    except ValueError:
        return None


_MEMINFO_RE = re.compile(r"^(\w+):\s+(\d+)\s*kB\s*$")
_KB_FIELD_RE = re.compile(r":\s*(\d+)\s*kB")


def _parse_kb_field(line: str) -> int | None:
    """Extract the numeric kB value from a /proc/<pid>/status line like "VmRSS:\t 123 kB"."""
    match = _KB_FIELD_RE.search(line)
    if not match:
        return None
    return int(match.group(1))


# --------------------------------------------------------------------------------------
# Bolt collector
# --------------------------------------------------------------------------------------


class BoltQueryError(RuntimeError):
    """A single Bolt query failed; caught and logged by the orchestrator, never fatal."""


@dataclass(slots=True)
class BoltConfig:
    """Bolt connection settings. `database` is the driver's default/landing database
    (not a restriction on what gets polled -- see --databases for that); it also
    doubles as the reset target after each per-tenant `USE DATABASE` switch."""

    address: str
    username: str = ""
    password: str = ""
    database: str | None = None
    query_timeout_s: float = DEFAULT_QUERY_TIMEOUT_S


class BoltCollector:
    """Runs read-only `SHOW ...` admin queries over Bolt. Owns its own driver + a
    single-worker executor used purely to enforce a hard wall-clock timeout per query
    (the neo4j driver's own transaction timeout is a server-side hint that Memgraph may
    or may not honor; the executor guarantees we never block the poller loop)."""

    def __init__(self, config: BoltConfig, logger: logging.Logger) -> None:
        """Lazily import `neo4j` -- this is the ONLY third-party dependency in the whole
        script, and only needed at all when the Bolt collector is enabled -- and set up
        (but do not yet start) the connection + timeout-enforcement machinery."""
        try:
            import neo4j
        except ImportError as exc:
            raise RuntimeError(
                "--bolt-address was given but the 'neo4j' package is not installed. "
                "Install it with: pip install neo4j"
            ) from exc
        self._neo4j = neo4j
        self._config = config
        self._logger = logger
        self._driver: Any = None
        self._connected = False
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix="diag-bolt")

    @property
    def connected(self) -> bool:
        """Whether the last connection attempt/query succeeded; drives the reconnect
        loop in _collect_bolt_tick and the crash/restart detection in _record_crash."""
        return self._connected

    @property
    def default_database(self) -> str | None:
        """The driver's configured landing database (--database), used as the reset
        target for per-tenant USE DATABASE switches."""
        return self._config.database

    def connect(self) -> None:
        """(Re)establish the Bolt connection. Raises on failure -- the caller
        (_collect_bolt_tick) is responsible for catching that and driving the
        reconnect/backoff loop; this method itself does not retry."""
        uri = f"bolt://{self._config.address}"
        auth = (self._config.username, self._config.password) if self._config.username else None
        driver = self._neo4j.GraphDatabase.driver(uri, auth=auth, connection_timeout=self._config.query_timeout_s)
        driver.verify_connectivity()
        self._close_driver()
        self._driver = driver
        self._connected = True

    def _close_driver(self) -> None:
        """Best-effort driver teardown; swallows any close-time error since we're
        abandoning this driver instance regardless (about to replace or discard it)."""
        if self._driver is not None:
            with contextlib.suppress(Exception):
                self._driver.close()
        self._driver = None

    def close(self) -> None:
        """Full shutdown: drop the connection and stop the timeout-enforcement
        executor. `cancel_futures=True` is safe even if a query is still in flight --
        we're exiting the process, not trying to finish that query."""
        self._connected = False
        self._close_driver()
        self._executor.shutdown(wait=False, cancel_futures=True)

    def run_query(self, query: str) -> list[dict[str, Any]]:
        """Run one read-only query with a hard wall-clock timeout. Raises BoltQueryError
        on any failure. Only flips `connected` False for failures that indicate the
        connection itself is dead — a query-level server error (e.g. an unsupported
        `SHOW` variant on this edition) leaves the connection marked healthy."""
        if not self._connected or self._driver is None:
            raise BoltQueryError("not connected")

        def _do_run() -> list[dict[str, Any]]:
            """The actual blocking Bolt call, run on the single-worker executor so the
            `future.result(timeout=...)` below can enforce a hard deadline regardless of
            whether the neo4j driver's own (server-side, advisory) timeout fires."""
            kwargs: dict[str, Any] = {}
            if self._config.database:
                kwargs["database"] = self._config.database
            with self._driver.session(**kwargs) as session:
                q = self._neo4j.Query(query, timeout=self._config.query_timeout_s)
                result = session.run(q)
                return [dict(record) for record in result]

        future = self._executor.submit(_do_run)
        try:
            return future.result(timeout=self._config.query_timeout_s + 2.0)
        except concurrent.futures.TimeoutError as exc:
            # The query is stuck (e.g. a dead TCP peer never notified us). Force a
            # reconnect next tick; closing the driver unblocks the abandoned thread.
            self._connected = False
            self._close_driver()
            raise BoltQueryError(f"{query!r} timed out after {self._config.query_timeout_s}s") from exc
        except self._neo4j.exceptions.Neo4jError as exc:
            # Server executed the query and returned an error; connection is still fine.
            raise BoltQueryError(f"{query!r} failed: {exc}") from exc
        except Exception as exc:  # ServiceUnavailable, SessionExpired, socket errors, ...
            self._connected = False
            raise BoltQueryError(f"{query!r} failed: {exc}") from exc

    def run_query_sequence(
        self, queries: Sequence[str], trailing_best_effort: Sequence[str] = ()
    ) -> list[list[dict[str, Any]]]:
        """Run several queries in ONE session, so Memgraph session state set by one
        statement (most importantly `USE DATABASE <tenant>`) is visible to the next.
        This is required for per-tenant collection: a fresh session per query would
        silently drop back to the default DB before the follow-up SHOW query runs,
        making every tenant look like the default database (the exact bug this exists
        to prevent). Returns one row-list per query in `queries`, in order; raises
        BoltQueryError on the first failure among `queries`.

        `trailing_best_effort` queries run afterward in the SAME session (e.g. `USE
        DATABASE <default>` to reset the tenant context before the connection is
        returned to the pool) but never raise -- a failure there doesn't invalidate the
        results already collected from `queries`."""
        if not self._connected or self._driver is None:
            raise BoltQueryError("not connected")

        def _do_run() -> list[list[dict[str, Any]]]:
            """Run every `queries` statement, then every `trailing_best_effort`
            statement, all on ONE session/connection so USE DATABASE's effect carries
            through -- see the enclosing method's docstring for why that matters."""
            results: list[list[dict[str, Any]]] = []
            with self._driver.session() as session:
                for query in queries:
                    q = self._neo4j.Query(query, timeout=self._config.query_timeout_s)
                    result = session.run(q)
                    results.append([dict(record) for record in result])
                for query in trailing_best_effort:
                    with contextlib.suppress(Exception):
                        q = self._neo4j.Query(query, timeout=self._config.query_timeout_s)
                        session.run(q).consume()
            return results

        budget = self._config.query_timeout_s * (len(queries) + len(trailing_best_effort)) + 2.0
        future = self._executor.submit(_do_run)
        try:
            return future.result(timeout=budget)
        except concurrent.futures.TimeoutError as exc:
            self._connected = False
            self._close_driver()
            raise BoltQueryError(f"query sequence timed out: {list(queries)!r}") from exc
        except self._neo4j.exceptions.Neo4jError as exc:
            raise BoltQueryError(f"query sequence {list(queries)!r} failed: {exc}") from exc
        except Exception as exc:
            self._connected = False
            raise BoltQueryError(f"query sequence {list(queries)!r} failed: {exc}") from exc


def _safe_query(collector: BoltCollector, name: str, query: str, errors: dict[str, str]) -> list[dict[str, Any]] | None:
    """Run one query, catching BoltQueryError so a single failed SHOW never aborts the
    whole inventory/sample collection -- the failure is recorded under `name` in
    `errors` (surfaced in the JSONL as bolt.errors) and the caller gets None back."""
    try:
        return collector.run_query(query)
    except BoltQueryError as exc:
        errors[name] = str(exc)
        return None


def _extract_database_names(rows: list[dict[str, Any]] | None, fallback: str | None) -> list[str]:
    """Pull the database names out of SHOW DATABASES rows (case-insensitively -- the
    real column header is "Name", capital N). Falls back to a single implicit database
    (--database or "memgraph") if SHOW DATABASES failed or returned nothing, which is
    the normal case on a non-enterprise/single-tenant build."""
    if not rows:
        return [fallback] if fallback else ["memgraph"]
    names = []
    for row in rows:
        key = _find_key(row, ("name",))
        if key is not None and row.get(key):
            names.append(str(row[key]))
    return names or ([fallback] if fallback else ["memgraph"])


def collect_per_tenant_storage(
    collector: BoltCollector, db_names: Sequence[str], default_database: str | None, errors: dict[str, str]
) -> dict[str, Any]:
    """Per-tenant storage attribution. PRIMARY path: the dedicated `SHOW STORAGE INFO
    ON DATABASE <name>` query -- a single, session-state-free statement, confirmed
    correct on both v3.10.1 (grounded catalog) and the v3.11 sibling used for e2e
    validation (empirically: a non-default tenant with 5000 vertices correctly showed
    vertex_count=5000 there while `memgraph`/other tenants showed 0). FALLBACK, only if
    that variant errors on some build: `USE DATABASE <name>` then bare `SHOW STORAGE
    INFO` IN THE SAME SESSION, so the switch is visible to the follow-up query -- a
    fresh session per statement would silently keep returning the default DB's numbers
    for every tenant. Note this fallback is NOT guaranteed to carry current-DB fields on
    every build (observed empirically on the v3.11 sibling: the bare query kept
    returning the same fixed global-shaped row regardless of the preceding USE
    DATABASE), so it is a best-effort secondary attempt, not a guaranteed win -- ON
    DATABASE is the reliable path we actually rely on. Resets back to
    `default_database` (best-effort) before the fallback session's connection returns
    to the pool."""
    reset_target = default_database or "memgraph"
    per_db: dict[str, Any] = {}
    for db_name in db_names:
        on_database_key = f"per_tenant_storage_on_database:{db_name}"
        query = f"SHOW STORAGE INFO ON DATABASE {quote_symbolic_name(db_name)}"
        rows = _safe_query(collector, on_database_key, query, errors)
        if rows is not None:
            per_db[db_name] = rows_to_kv_dict(rows, ("storage info", "name", "key"), ("value",))
            continue

        use_database_key = f"per_tenant_storage_use_database:{db_name}"
        try:
            results = collector.run_query_sequence(
                [f"USE DATABASE {quote_symbolic_name(db_name)}", Q_SHOW_STORAGE_INFO],
                trailing_best_effort=[f"USE DATABASE {quote_symbolic_name(reset_target)}"],
            )
            per_db[db_name] = rows_to_kv_dict(results[1], ("storage info", "name", "key"), ("value",))
        except BoltQueryError as exc:
            errors[use_database_key] = str(exc)
            per_db[db_name] = None
    return per_db


def collect_per_tenant_schema(
    collector: BoltCollector, db_names: Sequence[str], default_database: str | None, errors: dict[str, str]
) -> dict[str, Any]:
    """Per-tenant index/constraint counts: `SHOW INDEX INFO`/`SHOW CONSTRAINT INFO` are
    CURRENT-DB scoped with no `ON DATABASE` variant, so switch first via `USE DATABASE
    <name>` and run both in that SAME session (same reasoning as
    collect_per_tenant_storage). Infrequent/inventory-cadence, since schema rarely
    changes tick-to-tick."""
    reset_target = default_database or "memgraph"
    per_db: dict[str, Any] = {}
    for db_name in db_names:
        key = f"per_tenant_schema:{db_name}"
        try:
            results = collector.run_query_sequence(
                [
                    f"USE DATABASE {quote_symbolic_name(db_name)}",
                    Q_SHOW_INDEX_INFO,
                    Q_SHOW_CONSTRAINT_INFO,
                ],
                trailing_best_effort=[f"USE DATABASE {quote_symbolic_name(reset_target)}"],
            )
            per_db[db_name] = {"index_info": results[1], "constraint_info": results[2]}
        except BoltQueryError as exc:
            errors[key] = str(exc)
            per_db[db_name] = {"index_info": None, "constraint_info": None}
    return per_db


def collect_inventory(
    collector: BoltCollector,
    default_database: str | None,
    requested_databases: Sequence[str] | None,
    errors: dict[str, str],
) -> dict[str, Any]:
    """Infrequent full inventory: SHOW VERSION/BUILD INFO/CONFIG/DATABASES/REPLICATION
    ROLE/REPLICAS/TRIGGERS (each instance-global, run once) plus a per-tenant SHOW INDEX
    INFO/SHOW CONSTRAINT INFO sweep (see collect_per_tenant_schema). SHOW DATABASES may
    fail on a non-enterprise/single-db build -- in that case there is only ever one
    (implicit) database, `USE DATABASE` would be meaningless/unsupported too, so we
    query index/constraint info directly with no tenant switch."""
    inv: dict[str, Any] = {
        "version": _safe_query(collector, "show_version", Q_SHOW_VERSION, errors),
        "build_info": _safe_query(collector, "show_build_info", Q_SHOW_BUILD_INFO, errors),
        "config": _safe_query(collector, "show_config", Q_SHOW_CONFIG, errors),
        "replication_role": _safe_query(collector, "show_replication_role", Q_SHOW_REPLICATION_ROLE, errors),
        "replicas": _safe_query(collector, "show_replicas", Q_SHOW_REPLICAS, errors),
        "triggers": _safe_query(collector, "show_triggers", Q_SHOW_TRIGGERS, errors),
    }

    databases_rows = _safe_query(collector, "show_databases", Q_SHOW_DATABASES, errors)
    single_db_fallback = databases_rows is None
    if databases_rows is None:
        databases_rows = [{"name": default_database or "memgraph"}]
    inv["databases"] = databases_rows
    inv["single_db_fallback"] = single_db_fallback

    all_db_names = _extract_database_names(databases_rows, default_database)
    db_names = list(requested_databases) if requested_databases else all_db_names
    inv["polled_databases"] = db_names

    if single_db_fallback:
        index_rows = _safe_query(collector, "show_index_info", Q_SHOW_INDEX_INFO, errors)
        constraint_rows = _safe_query(collector, "show_constraint_info", Q_SHOW_CONSTRAINT_INFO, errors)
        inv["index_info"] = index_rows
        inv["constraint_info"] = constraint_rows
        inv["per_tenant_schema"] = (
            {db_names[0]: {"index_info": index_rows, "constraint_info": constraint_rows}} if db_names else {}
        )
    else:
        inv["index_info"] = None
        inv["constraint_info"] = None
        inv["per_tenant_schema"] = collect_per_tenant_schema(collector, db_names, default_database, errors)

    return inv


def collect_sample(
    collector: BoltCollector,
    db_names: Sequence[str],
    default_database: str | None,
    single_db_fallback: bool,
    errors: dict[str, str],
) -> dict[str, Any]:
    """High-frequency sample: instance-wide `SHOW STORAGE INFO` (captured ONCE), a
    per-tenant storage sweep (see collect_per_tenant_storage), SHOW TRANSACTIONS, SHOW
    METRICS INFO, SHOW MEMORY INFO (each instance-global, run once -- NOT per tenant)."""
    sample: dict[str, Any] = {}

    storage_global_rows = _safe_query(collector, "show_storage_info", Q_SHOW_STORAGE_INFO, errors)
    global_flat = rows_to_kv_dict(storage_global_rows, ("storage info", "name", "key"), ("value",))
    sample["storage_info_global"] = global_flat

    if single_db_fallback:
        # Only one (implicit) database exists; its current-DB fields are already
        # sitting right there in the bare global row -- no USE DATABASE needed/possible.
        sample["storage_info_by_db"] = {db_names[0]: global_flat} if db_names else {}
    else:
        sample["storage_info_by_db"] = collect_per_tenant_storage(collector, db_names, default_database, errors)

    sample["transactions"] = _safe_query(collector, "show_transactions", Q_SHOW_TRANSACTIONS, errors) or []

    metrics_rows = _safe_query(collector, "show_metrics_info", Q_SHOW_METRICS_INFO, errors)
    sample["metrics"] = rows_to_kv_dict(metrics_rows, ("name",), ("value",))

    sample["memory_info"] = _safe_query(collector, "show_memory_info", Q_SHOW_MEMORY_INFO, errors) or []

    return sample


# --------------------------------------------------------------------------------------
# HTTP metrics collector (secondary, optional)
# --------------------------------------------------------------------------------------


@dataclass(slots=True)
class MetricsHttpConfig:
    """--metrics-url endpoint settings."""

    url: str
    timeout_s: float = DEFAULT_QUERY_TIMEOUT_S


class MetricsHttpCollector:
    """GETs a JSON metrics endpoint and flattens it. Permanently disables itself (with a
    one-line notice) on a 403/license-gated response or if the endpoint is unreachable —
    SHOW METRICS INFO over Bolt already covers the same counters, so this is redundant."""

    def __init__(self, config: MetricsHttpConfig, logger: logging.Logger) -> None:
        """Nothing to connect yet -- this collector is entirely stateless besides the
        one-way `disabled` latch, so there's no persistent connection to set up here."""
        self._config = config
        self._logger = logger
        self._disabled = False

    @property
    def disabled(self) -> bool:
        """Once True, stays True for the life of the process (see _disable())."""
        return self._disabled

    def collect(self) -> dict[str, Any] | None:
        """GET the metrics endpoint and flatten its JSON body. Returns None (never
        raises) on any failure; a license-gated or unreachable response also flips
        `disabled` so subsequent ticks skip the HTTP round-trip entirely."""
        if self._disabled:
            return None
        import urllib.error
        import urllib.request

        request = urllib.request.Request(self._config.url, headers={"Accept": "application/json"})
        body = ""
        try:
            with urllib.request.urlopen(request, timeout=self._config.timeout_s) as response:
                status = getattr(response, "status", 200)
                body = response.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            with contextlib.suppress(Exception):
                body = exc.read().decode("utf-8", errors="replace")
            if exc.code == 403 or "enterprise license" in body.lower():
                self._disable(f"HTTP {exc.code} (license-gated)")
                return None
            self._logger.warning("metrics HTTP collector: HTTP %s from %s", exc.code, self._config.url)
            return None
        except (urllib.error.URLError, OSError, TimeoutError) as exc:
            self._disable(f"unreachable ({exc})")
            return None

        if status == 403 or "enterprise license" in body.lower():
            self._disable(f"HTTP {status} (license-gated)")
            return None

        try:
            payload = json.loads(body)
        except json.JSONDecodeError as exc:
            self._logger.warning("metrics HTTP collector: non-JSON response (%s)", exc)
            return None

        return _flatten_json(payload)

    def _disable(self, reason: str) -> None:
        """Latch `disabled` permanently and log why, once, at INFO (not WARNING) level
        -- this is expected/benign behavior (SHOW METRICS INFO over Bolt already covers
        the same data), not a fault."""
        self._disabled = True
        self._logger.info(
            "metrics HTTP collector disabled: %s -- SHOW METRICS INFO over Bolt already " "covers the same counters",
            reason,
        )


# --------------------------------------------------------------------------------------
# OS collector (opt-in)
# --------------------------------------------------------------------------------------


@dataclass(slots=True)
class OsConfig:
    """--os collector settings. `pid` pins the target process explicitly; if unset the
    collector auto-detects it by scanning /proc for a process named "memgraph"."""

    pid: int | None = None


class OsCollector:
    """Best-effort /proc + cgroup + dmesg/journalctl reads for the memgraph process.
    Every read is individually guarded: a missing path/permission/command just adds a
    note to the record instead of failing the tick."""

    def __init__(self, config: OsConfig, logger: logging.Logger) -> None:
        """`_resolved_pid`/`_start_time_ticks` are the only state carried between ticks
        (auto-detected PID caching + the restart-via-starttime-change heuristic)."""
        self._config = config
        self._logger = logger
        self._resolved_pid: int | None = config.pid
        self._start_time_ticks: int | None = None
        self._warned_no_proc = False

    def collect(self) -> dict[str, Any]:
        """One tick's worth of OS-level reads for the memgraph process: /proc/<pid>
        status + fd count + restart detection, cgroup memory, and host-wide meminfo.
        Never raises; every individual read failure is appended to `notes` instead."""
        notes: list[str] = []
        result: dict[str, Any] = {"notes": notes}

        pid = self._resolve_pid(notes)
        if pid is None:
            return result
        result["pid"] = pid

        status = self._read_status(pid, notes)
        if status:
            result.update(status)

        fd_count = self._read_fd_count(pid, notes)
        if fd_count is not None:
            result["open_fds"] = fd_count

        restarted = self._check_restart(pid, notes)
        if restarted is not None:
            result["restarted"] = restarted

        cgroup = self._read_cgroup_memory(pid, notes)
        if cgroup:
            result.update(cgroup)

        meminfo = self._read_meminfo(notes)
        if meminfo:
            result["meminfo"] = meminfo

        return result

    def check_oom(self) -> tuple[list[str], list[str]]:
        """Best-effort OOM-killer scan via dmesg/journalctl. Called at inventory cadence
        only (subprocess calls are heavier than a /proc read)."""
        notes: list[str] = []
        lines = self._try_dmesg()
        if lines is None:
            lines = self._try_journalctl()
        if lines is None:
            notes.append("dmesg/journalctl -k unavailable (no permission or not installed)")
            return [], notes
        oom_lines = [line for line in lines if "oom" in line.lower() or "out of memory" in line.lower()]
        return oom_lines[-20:], notes

    # -- pid resolution & liveness -----------------------------------------------------

    def _resolve_pid(self, notes: list[str]) -> int | None:
        """If --pid was given, always trust it (the user explicitly pinned this PID; no
        liveness check second-guesses that choice). Otherwise re-verify the cached
        auto-detected PID is still a memgraph process each tick -- if the process died
        and a new one started (even reusing the same PID, e.g. if memgraph is PID 1 in
        its container), re-scan /proc rather than silently reporting stale/wrong data."""
        if self._config.pid is not None:
            return self._config.pid
        if self._resolved_pid is not None and self._pid_is_memgraph(self._resolved_pid):
            return self._resolved_pid
        self._resolved_pid = None
        pid = self._detect_pid()
        if pid is None:
            if not self._warned_no_proc:
                self._logger.warning("OS collector: no memgraph process found via auto-detect; pass --pid explicitly")
                self._warned_no_proc = True
            notes.append("process not found")
            return None
        self._resolved_pid = pid
        return pid

    def _pid_is_memgraph(self, pid: int) -> bool:
        """Cheap liveness/identity check: does /proc/<pid>/comm say "memgraph"?"""
        comm = _read_text_file(Path(f"/proc/{pid}/comm"))
        return comm is not None and comm.strip() == "memgraph"

    def _detect_pid(self) -> int | None:
        """Scan /proc for the first process (other than ourselves) named "memgraph"."""
        proc_dir = Path("/proc")
        try:
            entries = list(proc_dir.iterdir())
        except OSError:
            return None
        self_pid = os.getpid()
        for entry in entries:
            if not entry.name.isdigit():
                continue
            pid = int(entry.name)
            if pid == self_pid:
                continue
            comm = _read_text_file(entry / "comm")
            if comm is not None and comm.strip() == "memgraph":
                return pid
        return None

    # -- /proc reads ---------------------------------------------------------------

    def _read_status(self, pid: int, notes: list[str]) -> dict[str, Any] | None:
        """Parse /proc/<pid>/status for VmRSS/VmData/Threads -- the customer-facing
        memory (and thread-count) picture straight from the kernel, independent of
        whatever Memgraph itself reports via SHOW STORAGE INFO."""
        text = _read_text_file(Path(f"/proc/{pid}/status"))
        if text is None:
            notes.append(f"/proc/{pid}/status unavailable")
            return None
        out: dict[str, Any] = {}
        for line in text.splitlines():
            if line.startswith("VmRSS:"):
                out["vm_rss_kb"] = _parse_kb_field(line)
            elif line.startswith("VmData:"):
                out["vm_data_kb"] = _parse_kb_field(line)
            elif line.startswith("Threads:"):
                parts = line.split(":", 1)
                if len(parts) == 2:
                    try:
                        out["threads"] = int(parts[1].strip())
                    except ValueError:
                        pass
        return out

    def _read_fd_count(self, pid: int, notes: list[str]) -> int | None:
        """Open file descriptor count -- some environments' memory accounting folds
        mapped files/sockets in here too, so it's a useful cross-check figure."""
        try:
            return len(os.listdir(f"/proc/{pid}/fd"))
        except OSError as exc:
            notes.append(f"/proc/{pid}/fd unavailable: {exc}")
            return None

    def _read_start_time_ticks(self, pid: int) -> int | None:
        """Process start time in clock ticks since boot (/proc/<pid>/stat field 22) --
        the raw material for _check_restart()'s "did this PID actually restart"
        heuristic (a PID can be reused by a fresh process, e.g. if memgraph runs as
        PID 1 in its container, so PID alone doesn't prove continuity)."""
        text = _read_text_file(Path(f"/proc/{pid}/stat"))
        if text is None:
            return None
        # comm (field 2) is parenthesized and may itself contain spaces/parens; resume
        # parsing after the LAST ')' to safely skip it.
        idx = text.rfind(")")
        if idx == -1:
            return None
        fields = text[idx + 2 :].split()
        # fields[0] here is stat field 3 (state); starttime is field 22 -> fields[19].
        try:
            return int(fields[19])
        except (IndexError, ValueError):
            return None

    def _check_restart(self, pid: int, notes: list[str]) -> bool | None:
        """True iff this PID's start time changed since the last tick -- i.e. the
        process at this PID is NOT the same process instance anymore (a restart
        happened, whether or not the PID number itself changed). None on the very first
        tick (nothing to compare against yet)."""
        ticks = self._read_start_time_ticks(pid)
        if ticks is None:
            notes.append(f"/proc/{pid}/stat starttime unavailable")
            return None
        restarted = self._start_time_ticks is not None and ticks != self._start_time_ticks
        self._start_time_ticks = ticks
        return restarted

    def _read_cgroup_memory(self, pid: int, notes: list[str]) -> dict[str, Any] | None:
        """Container memory usage/limit, trying cgroup v2 first then falling back to
        v1 -- this is what actually kills the process on an OOM in a containerized
        deployment, and can differ sharply from Memgraph's own --memory-limit (e.g. the
        container limit may be tighter than what Memgraph believes it can use)."""
        cgroup_text = _read_text_file(Path(f"/proc/{pid}/cgroup"))
        if cgroup_text is None:
            notes.append(f"/proc/{pid}/cgroup unavailable")
            return None

        # /proc/<pid>/cgroup line format: "<hierarchy-id>:<controllers>:<path>".
        # cgroup v2 (unified hierarchy): a single "0::/<path>" line (no named controller).
        # cgroup v1: one line per controller, e.g. "5:memory:/<path>".
        v2_path: str | None = None
        v1_memory_path: str | None = None
        for line in cgroup_text.splitlines():
            parts = line.split(":", 2)
            if len(parts) != 3:
                continue
            hierarchy_id, controllers, sub_path = parts
            if hierarchy_id == "0" and controllers == "":
                v2_path = sub_path
            elif "memory" in controllers.split(","):
                v1_memory_path = sub_path

        if v2_path is not None:
            base = Path("/sys/fs/cgroup") / v2_path.lstrip("/")
            current = _read_int_file(base / "memory.current")
            max_raw = _read_text_file(base / "memory.max")
            limit = None if max_raw == "max" else _to_int(max_raw)
            if current is not None or limit is not None:
                return {
                    "cgroup_version": 2,
                    "cgroup_memory_current_bytes": current,
                    "cgroup_memory_max_bytes": limit,
                }
            notes.append(f"cgroup v2 memory files unavailable under {base}")

        if v1_memory_path is not None:
            base = Path("/sys/fs/cgroup/memory") / v1_memory_path.lstrip("/")
            usage = _read_int_file(base / "memory.usage_in_bytes")
            limit = _to_int(_read_text_file(base / "memory.limit_in_bytes"))
            if limit is not None and limit > 2**62:
                limit = None  # cgroup v1's "no limit" sentinel
            if usage is not None or limit is not None:
                return {
                    "cgroup_version": 1,
                    "cgroup_memory_current_bytes": usage,
                    "cgroup_memory_max_bytes": limit,
                }
            notes.append(f"cgroup v1 memory files unavailable under {base}")

        notes.append("no cgroup memory controller found")
        return None

    def _read_meminfo(self, notes: list[str]) -> dict[str, int] | None:
        """Host-wide (not per-process) memory pressure -- useful outside a container,
        or to see if OTHER processes on the same host are the real memory consumer."""
        text = _read_text_file(Path("/proc/meminfo"))
        if text is None:
            notes.append("/proc/meminfo unavailable")
            return None
        wanted = {"MemTotal", "MemFree", "MemAvailable", "SwapTotal", "SwapFree"}
        out: dict[str, int] = {}
        for line in text.splitlines():
            match = _MEMINFO_RE.match(line)
            if match and match.group(1) in wanted:
                out[match.group(1)] = int(match.group(2)) * 1024
        return out or None

    def _try_dmesg(self) -> list[str] | None:
        """Kernel ring buffer via `dmesg -T`, used by check_oom(). None (not an empty
        list) if the command is missing or we lack permission -- distinguishes "no OOM
        lines" from "couldn't check at all"."""
        try:
            proc = subprocess.run(["dmesg", "-T"], capture_output=True, text=True, timeout=5, check=False)
        except (OSError, subprocess.SubprocessError):
            return None
        return proc.stdout.splitlines() if proc.returncode == 0 else None

    def _try_journalctl(self) -> list[str] | None:
        """Fallback for _try_dmesg() on systems where dmesg is restricted/unavailable
        but the systemd journal's kernel ring is still readable."""
        try:
            proc = subprocess.run(
                ["journalctl", "-k", "-n", "500", "--no-pager"],
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
        except (OSError, subprocess.SubprocessError):
            return None
        return proc.stdout.splitlines() if proc.returncode == 0 else None


# --------------------------------------------------------------------------------------
# Derived signal tracking
# --------------------------------------------------------------------------------------


class TxnAgeTracker:
    """Tracks first-seen wall-clock time per transaction_id, since 3.10.1's
    SHOW TRANSACTIONS carries no start timestamp. Opportunistically prefers a native
    start_time field if a newer server build happens to provide one."""

    def __init__(self) -> None:
        """`_first_seen` persists across ticks (keyed by transaction_id) -- it's the
        whole reason this is a stateful tracker rather than a pure function."""
        self._first_seen: dict[str, float] = {}

    def update(self, rows: Sequence[Mapping[str, Any]], now: float) -> dict[str, Any]:
        """Given this tick's SHOW TRANSACTIONS rows, return the active count, the
        oldest transaction's age + query text, and whether a native start_time was
        available. Also prunes `_first_seen` entries for transactions that ended (not
        in `rows` anymore) -- otherwise a long-running poller would leak memory holding
        onto every transaction_id it had ever seen."""
        seen_ids: set[str] = set()
        max_age = 0.0
        oldest_query: Any = None
        used_native = False

        for row in rows:
            txn_id = row.get("transaction_id")
            if txn_id is None:
                continue
            txn_id = str(txn_id)
            seen_ids.add(txn_id)

            age = self._native_age(row, now)
            if age is not None:
                used_native = True
            else:
                first_seen = self._first_seen.setdefault(txn_id, now)
                age = max(now - first_seen, 0.0)

            if age >= max_age:
                max_age = age
                oldest_query = row.get("query")

        for txn_id in list(self._first_seen):
            if txn_id not in seen_ids:
                del self._first_seen[txn_id]

        return {
            "active_txn_count": len(rows),
            "longest_txn_age_s": round(max_age, 3) if rows else None,
            "longest_txn_query": oldest_query,
            "used_native_txn_start_time": used_native,
        }

    @staticmethod
    def _native_age(row: Mapping[str, Any], now: float) -> float | None:
        """Opportunistic path for a server build that DOES include a start_time field
        on SHOW TRANSACTIONS rows (unlike 3.10.1, our primary target): compute the age
        directly instead of relying on our own first-seen tracking. Handles a neo4j
        temporal wrapper type, a bare datetime, or a raw epoch seconds/milliseconds
        number defensively, since we can't be sure which shape a given build returns."""
        start = row.get("start_time")
        if start is None:
            return None
        try:
            if hasattr(start, "to_native"):
                start = start.to_native()
            if hasattr(start, "timestamp"):
                epoch = start.timestamp()
            elif isinstance(start, (int, float)):
                epoch = float(start) / 1000.0 if start > 1e12 else float(start)
            else:
                return None
        except (TypeError, ValueError, OverflowError):
            return None
        return max(now - epoch, 0.0)


class RateTracker:
    """Diffs monotonic counters between ticks into per-interval deltas + rates, and
    flags any counter that goes backwards (a strong restart signal). Each logical
    counter resolves against an ordered list of candidate physical names (see
    MONOTONIC_COUNTER_CANDIDATES) to tolerate renames across server versions (e.g.
    RollbackedTransactions -> RolledBackTransactions)."""

    def __init__(self, counter_candidates: Mapping[str, Sequence[str]]) -> None:
        """`counter_candidates` is normally MONOTONIC_COUNTER_CANDIDATES; a fresh
        RateTracker is also used in --selftest to exercise the rename-tolerance logic
        in isolation. `_prev_values`/`_prev_time` are the only cross-tick state."""
        self._counter_candidates = dict(counter_candidates)
        self._prev_values: dict[str, float] | None = None
        self._prev_time: float | None = None

    def update(
        self, metrics: Mapping[str, Any] | None, now: float
    ) -> tuple[dict[str, Any], list[str], dict[str, str | None]]:
        """Diff this tick's SHOW METRICS INFO values against the previous tick's.
        Returns (deltas_and_rates, counters_that_went_backwards, physical_key_used_per_
        logical_counter). A missing `metrics` (e.g. the query failed, or this build is
        non-enterprise/license-gated) resets the tracker's memory of previous values --
        we'd rather report a gap than compute a bogus delta across a missing sample."""
        reset_counters: list[str] = []
        if not metrics:
            self._prev_values = None
            self._prev_time = now
            return (
                {name: {"delta": None, "rate_per_s": None} for name in self._counter_candidates},
                reset_counters,
                {name: None for name in self._counter_candidates},
            )

        dt = None if self._prev_time is None else max(now - self._prev_time, 1e-6)
        current_values: dict[str, float] = {}
        deltas: dict[str, Any] = {}
        resolved: dict[str, str | None] = {}
        for name, candidates in self._counter_candidates.items():
            physical_key: str | None = None
            value: float | None = None
            for candidate in candidates:
                if candidate in metrics and metrics[candidate] is not None:
                    physical_key = candidate
                    value = _to_number(metrics[candidate])
                    break
            resolved[name] = physical_key
            if value is not None:
                current_values[name] = value
            prev = self._prev_values.get(name) if self._prev_values else None
            if value is None or prev is None or dt is None:
                deltas[name] = {"delta": None, "rate_per_s": None}
                continue
            delta = value - prev
            if delta < 0:
                reset_counters.append(name)
                deltas[name] = {"delta": None, "rate_per_s": None}
                continue
            deltas[name] = {"delta": delta, "rate_per_s": round(delta / dt, 4)}

        self._prev_values = current_values
        self._prev_time = now
        return deltas, reset_counters, resolved


# --------------------------------------------------------------------------------------
# Poller state + main loop
# --------------------------------------------------------------------------------------


@dataclass
class PollerState:
    """All state carried between ticks in the main loop -- mutable by design (unlike
    the value-type dataclasses elsewhere in this file), since it IS the accumulated
    memory of the run. `last_*`/`single_db_fallback`/`version_info` are refreshed only
    at inventory cadence and reused every sample tick in between (schema/topology/
    version rarely change tick-to-tick); `history` backs the crash-dump trajectory."""

    rate_tracker: RateTracker
    txn_tracker: TxnAgeTracker = field(default_factory=TxnAgeTracker)
    last_databases: list[str] = field(default_factory=list)
    last_replicas: list[Mapping[str, Any]] | None = None
    last_per_tenant_schema: dict[str, Any] = field(default_factory=dict)
    single_db_fallback: bool = False
    version_info: VersionInfo = field(default_factory=VersionInfo)
    last_inventory_at: float = 0.0
    history: deque = field(default_factory=lambda: deque(maxlen=DEFAULT_CRASH_HISTORY))
    disconnected_since: float | None = None
    backoff_s: float = RECONNECT_BACKOFF_START_S
    next_reconnect_attempt: float = 0.0
    tick: int = 0


def _record_crash(
    crash_path: Path,
    state: PollerState,
    reason: str,
    downtime_s: float | None,
    logger: logging.Logger,
    detail: Any = None,
) -> dict[str, Any]:
    """Write one CRASH_DETECTED event -- reason + the preceding `state.history` ticks
    (the pre-restart trajectory) -- to diag_crashes.log. This records a FACT (a
    restart was observed, via `reason`: "counter_reset" or "reconnect_after_outage"),
    not a claim about why it happened; the caller decides when a restart is likely
    (see _collect_bolt_tick/_compute_derived) and merely reports it here."""
    event = {
        "event": "CRASH_DETECTED",
        "ts": _iso_now(),
        "reason": reason,
        "downtime_s": downtime_s,
        "detail": detail,
        "history": list(state.history),
    }
    logger.warning(
        "CRASH_DETECTED (%s)%s -- dumping last %d sample(s) to %s",
        reason,
        f", downtime={downtime_s:.1f}s" if downtime_s is not None else "",
        len(state.history),
        crash_path,
    )
    try:
        with crash_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(event, default=str) + "\n")
    except OSError as exc:
        logger.error("failed to write crash record to %s: %s", crash_path, exc)
    return event


def _collect_bolt_tick(
    bolt: BoltCollector,
    state: PollerState,
    args: argparse.Namespace,
    logger: logging.Logger,
) -> dict[str, Any]:
    """One tick's worth of Bolt collection: (re)connect with capped exponential
    backoff if needed, run the infrequent inventory sweep if its interval has elapsed,
    then always run the per-tick sample. Also where restart-via-reconnect-after-outage
    is detected and reported to _record_crash() (counter-reset-based detection lives in
    _compute_derived() instead, since it needs the parsed metrics)."""
    now = time.time()

    if not bolt.connected:
        if now < state.next_reconnect_attempt:
            return {"connected": False, "errors": {"connect": "backing off before next reconnect attempt"}}
        try:
            bolt.connect()
        except Exception as exc:  # neo4j driver's own exception hierarchy, plus OSError
            if state.disconnected_since is None:
                state.disconnected_since = now
                logger.warning("Bolt connection lost/unavailable: %s", exc)
            state.backoff_s = min(state.backoff_s * 2, RECONNECT_BACKOFF_CAP_S)
            state.next_reconnect_attempt = now + state.backoff_s
            return {"connected": False, "errors": {"connect": str(exc)}}

        bolt_crash_events: list[dict[str, Any]] = []
        if state.disconnected_since is not None:
            downtime = now - state.disconnected_since
            logger.warning("Bolt reconnected after %.1fs outage", downtime)
            state.disconnected_since = None
            if downtime >= args.crash_outage_threshold:
                bolt_crash_events.append(
                    _record_crash(args.crash_path, state, "reconnect_after_outage", downtime, logger)
                )
        state.backoff_s = RECONNECT_BACKOFF_START_S
    else:
        bolt_crash_events = []

    errors: dict[str, str] = {}
    do_inventory = state.last_inventory_at == 0.0 or (now - state.last_inventory_at) >= args.inventory_interval
    inventory = None
    if do_inventory:
        inventory = collect_inventory(bolt, args.database, args.databases_list, errors)
        state.last_databases = inventory.get("polled_databases") or state.last_databases
        state.last_replicas = inventory.get("replicas")
        state.last_per_tenant_schema = inventory.get("per_tenant_schema") or {}
        state.single_db_fallback = bool(inventory.get("single_db_fallback"))
        state.version_info = parse_version(inventory.get("version"))
        state.last_inventory_at = now

    db_names = state.last_databases or [args.database or "memgraph"]
    sample = collect_sample(bolt, db_names, args.database, state.single_db_fallback, errors)

    if not bolt.connected and state.disconnected_since is None:
        state.disconnected_since = now
        logger.warning("Bolt connection dropped mid-tick: %s", errors)

    result: dict[str, Any] = {
        "connected": bolt.connected,
        "sample": sample,
        "errors": errors,
        "crash_events": bolt_crash_events,
    }
    if inventory is not None:
        result["inventory"] = inventory
    return result


def _build_per_tenant_summary(
    storage_by_db: Mapping[str, Any],
    schema_by_db: Mapping[str, Any],
    version: VersionInfo,
) -> dict[str, Any]:
    """Combine per-tenant storage (this tick) + per-tenant schema (cached from the last
    inventory sweep) into one compact per-DB view, reused by the JSONL `derived.per_tenant`
    field, the human-log `tenants=[...]` fragment, and --to-csv's per-tenant columns.
    Captures ALL four per-tenant memory buckets (not just storage) so nothing is
    hidden: graph_memory_tracked (storage), query_memory_tracked, vector_index_
    memory_tracked (embedding), and tenant_memory_tracked (the DB's full tracked
    total -- storage + query + embedding -- which _compute_derived sums across
    tenants for non_storage_gap_bytes)."""
    per_tenant: dict[str, Any] = {}
    for db_name, row in storage_by_db.items():
        entry: dict[str, Any] = {
            "graph_memory_tracked_bytes": None,
            "query_memory_tracked_bytes": None,
            "vector_index_memory_tracked_bytes": None,
            "tenant_memory_tracked_bytes": None,
            "vertex_count": None,
            "edge_count": None,
            "unreleased_delta_objects": None,
            "index_count": None,
            "constraint_count": None,
        }
        if row:
            values, _resolved = resolve_fields(row, PER_TENANT_STORAGE_FIELD_CANDIDATES, version)
            entry["graph_memory_tracked_bytes"] = parse_size_to_bytes(values.get("graph_memory_tracked"))
            entry["query_memory_tracked_bytes"] = parse_size_to_bytes(values.get("query_memory_tracked"))
            entry["vector_index_memory_tracked_bytes"] = parse_size_to_bytes(values.get("vector_index_memory_tracked"))
            entry["tenant_memory_tracked_bytes"] = parse_size_to_bytes(values.get("tenant_memory_tracked"))
            entry["vertex_count"] = _to_number(values.get("vertex_count"))
            entry["edge_count"] = _to_number(values.get("edge_count"))
            entry["unreleased_delta_objects"] = _to_number(values.get("unreleased_delta_objects"))
        schema = schema_by_db.get(db_name) if schema_by_db else None
        if schema:
            index_rows = schema.get("index_info")
            constraint_rows = schema.get("constraint_info")
            entry["index_count"] = len(index_rows) if index_rows is not None else None
            entry["constraint_count"] = len(constraint_rows) if constraint_rows is not None else None
        per_tenant[db_name] = entry
    return per_tenant


def _compute_derived(
    record: Mapping[str, Any],
    state: PollerState,
    now: float,
    args: argparse.Namespace,
    logger: logging.Logger,
) -> dict[str, Any]:
    """Compute this tick's MEASUREMENTS (see the module-level JSONL RECORD SCHEMA
    comment for the exact meaning of each field) from the raw SHOW-query results
    already collected in `record["bolt"]`/`record["os"]`. Pure arithmetic only -- no
    thresholds, no severity, no "this means X" text is produced anywhere in this
    function. Also where counter-reset-based restart detection happens (needs the
    parsed metrics, unlike the reconnect-based detection in _collect_bolt_tick)."""
    derived: dict[str, Any] = {}
    bolt = record.get("bolt") or {}
    sample = bolt.get("sample") or {}
    global_storage = sample.get("storage_info_global") or {}
    storage_by_db = sample.get("storage_info_by_db") or {}
    metrics = sample.get("metrics") or {}
    version = state.version_info

    resolved_fields: dict[str, str | None] = {}

    # Instance-wide values, captured ONCE from the bare (no ON DATABASE/USE DATABASE)
    # SHOW STORAGE INFO row -- these are the same regardless of "current DB".
    global_values, global_resolved = resolve_fields(global_storage, GLOBAL_STORAGE_FIELD_CANDIDATES, version)
    resolved_fields.update({f"storage.{k}": v for k, v in global_resolved.items()})

    total_memory_tracked = parse_size_to_bytes(global_values.get("total_memory_tracked"))
    runtime_limit = parse_size_to_bytes(global_values.get("runtime_limit"))
    memory_res = parse_size_to_bytes(global_values.get("memory_res"))
    derived["total_memory_tracked_bytes"] = total_memory_tracked
    derived["runtime_limit_bytes"] = runtime_limit
    derived["memory_res_bytes"] = memory_res

    # non_storage_gap_bytes = global_memory_tracked - Sum(per-tenant tenant_memory_tracked)
    # i.e. process-global memory NOT attributed to any database. On v3.10.1, AST/query-
    # plan-cache memory is tracked ONLY in the process-global total_memory_tracker --
    # there is no dedicated per-DB or dedicated AST-cache counter at all -- so this gap
    # is dominated by that cache, but it also includes Bolt session buffers, thread
    # stacks, RPC buffers, and anything else genuinely global. This is a MEASUREMENT,
    # not a claim about what's causing it; we deliberately do NOT label it "AST cache"
    # anywhere in the output. The subtrahend must be the SUM of each tenant's full
    # tracked total (tenant_memory_tracked = storage + query + embedding, from the
    # per-tenant SHOW STORAGE INFO ON DATABASE sweep) -- never the bare global row's own
    # current-DB fields, which reflect only whichever DB the poller's plain session
    # happens to be sitting on (for this customer, the empty default `memgraph`, which
    # would make the gap look enormous and meaningless).
    tenant_total_bytes: dict[str, int] = {}
    for db_name, row in storage_by_db.items():
        if not row:
            continue
        values, _resolved = resolve_fields(row, PER_TENANT_STORAGE_FIELD_CANDIDATES, version)
        parsed = parse_size_to_bytes(values.get("tenant_memory_tracked"))
        if parsed is not None:
            tenant_total_bytes[db_name] = parsed
    tenant_total_sum = sum(tenant_total_bytes.values()) if tenant_total_bytes else None
    derived["tenant_total_memory_bytes_by_db"] = tenant_total_bytes
    derived["tenant_total_memory_bytes_sum"] = tenant_total_sum
    derived["non_storage_gap_bytes"] = (
        total_memory_tracked - tenant_total_sum
        if total_memory_tracked is not None and tenant_total_sum is not None
        else None
    )

    derived["memory_vs_runtime_limit_pct"] = (
        round(memory_res / runtime_limit * 100.0, 2) if memory_res is not None and runtime_limit else None
    )

    derived["version"] = {"raw": version.raw, "major": version.major, "minor": version.minor}

    os_info = record.get("os") or {}
    rss_bytes = None
    if os_info.get("vm_rss_kb") is not None:
        rss_bytes = os_info["vm_rss_kb"] * 1024
    cgroup_max = os_info.get("cgroup_memory_max_bytes")
    derived["rss_vs_cgroup_max_pct"] = (
        round(rss_bytes / cgroup_max * 100.0, 2) if rss_bytes is not None and cgroup_max else None
    )

    deltas, reset_counters, counter_resolved = state.rate_tracker.update(metrics if metrics else None, now)
    derived["counter_deltas"] = deltas
    derived["counters_reset"] = reset_counters
    resolved_fields.update({f"metric.{k}": v for k, v in counter_resolved.items()})
    derived["resolved_fields"] = resolved_fields

    crash_events: list[dict[str, Any]] = list(bolt.get("crash_events") or [])
    if reset_counters and metrics:
        logger.warning(
            "Monotonic counter(s) went backwards (%s) -- likely a Memgraph restart",
            ", ".join(reset_counters),
        )
        crash_events.append(_record_crash(args.crash_path, state, "counter_reset", None, logger, detail=reset_counters))
    derived["crash_events"] = crash_events

    txn_rows = sample.get("transactions") or []
    derived.update(state.txn_tracker.update(txn_rows, now))

    # Raw fact, straight from SHOW REPLICAS -- not an interpretation of significance.
    strict_sync = []
    for row in state.last_replicas or []:
        name = row.get("name")
        sync_mode = row.get("sync_mode")
        if sync_mode is not None and str(sync_mode).strip().upper() == "STRICT_SYNC":
            strict_sync.append(name)
    derived["strict_sync_replicas"] = strict_sync

    per_tenant = _build_per_tenant_summary(storage_by_db, state.last_per_tenant_schema, version)
    derived["per_tenant"] = per_tenant
    derived["vertex_count_total"] = (
        sum(v["vertex_count"] for v in per_tenant.values() if v["vertex_count"] is not None) if per_tenant else None
    )
    derived["edge_count_total"] = (
        sum(v["edge_count"] for v in per_tenant.values() if v["edge_count"] is not None) if per_tenant else None
    )

    return derived


def _emit_human_line(logger: logging.Logger, record: Mapping[str, Any], derived: Mapping[str, Any]) -> None:
    """Log ONE plain `key=value` line per tick -- a human-scannable mirror of the same
    tick's JSONL row, nothing more. Deliberately NOT interpretive: no "WARN: memory
    high" / "long-running transaction" / "delete activity detected" style editorializing
    anywhere in this function or its output -- diag logs measurements, it does not
    diagnose. Every value here is copied straight out of `derived` (or the raw sample);
    see the module-level JSONL RECORD SCHEMA comment for what each one means."""
    bolt = record.get("bolt") or {}
    connected = bolt.get("connected")
    version = derived.get("version") or {}
    deltas = derived.get("counter_deltas") or {}
    per_tenant = derived.get("per_tenant") or {}

    def _fmt(value: Any) -> str:
        """Render None as an empty string rather than the literal text "None" -- makes
        a missing/not-yet-resolved field visually obvious without claiming a 0 value."""
        return "" if value is None else str(value)

    read_rate = (deltas.get("ReadQuery") or {}).get("rate_per_s")
    write_rate = (deltas.get("WriteQuery") or {}).get("rate_per_s")
    delete_rate = sum((deltas.get(n) or {}).get("rate_per_s") or 0 for n in ("DeletedNodes", "DeletedEdges"))

    parts = [
        f"ts={record.get('ts')}",
        f"conn={'up' if connected else 'down'}",
        f"v={_fmt(version.get('raw'))}",
        f"mem_pct={_fmt(derived.get('memory_vs_runtime_limit_pct'))}",
        f"gap_bytes={_fmt(derived.get('non_storage_gap_bytes'))}",
        f"unreleased_deltas={_fmt(sum(v.get('unreleased_delta_objects') or 0 for v in per_tenant.values()) if per_tenant else None)}",
        f"active_txns={_fmt(derived.get('active_txn_count'))}",
        f"oldest_txn_s={_fmt(derived.get('longest_txn_age_s'))}",
        f"read_rate={_fmt(read_rate)}",
        f"write_rate={_fmt(write_rate)}",
        f"delete_rate={delete_rate}",
    ]

    if per_tenant:
        # Capped to keep one log line readable when there are many tenants; the JSONL
        # row for this same tick always has the complete, uncapped per-tenant map.
        cap = 10
        bits = [
            f"{db_name}:v={_fmt(info.get('vertex_count'))},idx={_fmt(info.get('index_count'))}"
            for db_name, info in list(per_tenant.items())[:cap]
        ]
        if len(per_tenant) > cap:
            bits.append(f"...(+{len(per_tenant) - cap} more)")
        parts.append("tenants=[" + " ".join(bits) + "]")

    logger.info(" ".join(parts))


def _append_jsonl(path: Path, record: Mapping[str, Any]) -> None:
    """Append one tick's record as a single JSON line. Opens/closes the file every
    call (rather than holding it open for the process lifetime) so a `tail -f` or a
    concurrent reader always sees complete, flushed lines, and an unclean process exit
    can't leave a half-written buffer."""
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, default=str) + "\n")


def _run_tick(
    tick_no: int,
    args: argparse.Namespace,
    logger: logging.Logger,
    bolt: BoltCollector | None,
    metrics_http: MetricsHttpCollector | None,
    os_collector: OsCollector | None,
    state: PollerState,
) -> dict[str, Any]:
    """Assemble ONE complete timeseries record: run every enabled collector (each
    independently wrapped so one raising an unexpected exception can't crash the
    others or the loop), time each one, then compute the derived measurements. Shared
    by the main loop, --once, and (indirectly, via _selftest_state) --selftest."""
    now = time.time()
    record: dict[str, Any] = {
        "ts": _iso_now(),
        "tick": tick_no,
        "bolt": None,
        "metrics_http": None,
        "os": None,
        "derived": {},
        "timings_s": {},
    }

    if bolt is not None:
        t0 = time.monotonic()
        try:
            record["bolt"] = _collect_bolt_tick(bolt, state, args, logger)
        except Exception as exc:  # belt-and-suspenders: a collector must never kill the loop
            logger.error("Bolt collector raised unexpectedly: %s", exc)
            record["bolt"] = {"connected": False, "errors": {"unexpected": str(exc)}}
        record["timings_s"]["bolt"] = round(time.monotonic() - t0, 4)

    if metrics_http is not None and not metrics_http.disabled:
        t0 = time.monotonic()
        try:
            record["metrics_http"] = metrics_http.collect()
        except Exception as exc:
            logger.warning("metrics HTTP collector raised unexpectedly: %s", exc)
        record["timings_s"]["metrics_http"] = round(time.monotonic() - t0, 4)

    if os_collector is not None:
        t0 = time.monotonic()
        try:
            record["os"] = os_collector.collect()
        except Exception as exc:
            logger.warning("OS collector raised unexpectedly: %s", exc)
        record["timings_s"]["os"] = round(time.monotonic() - t0, 4)

    record["derived"] = _compute_derived(record, state, now, args, logger)
    return record


def run_loop(
    args: argparse.Namespace,
    logger: logging.Logger,
    bolt: BoltCollector | None,
    metrics_http: MetricsHttpCollector | None,
    os_collector: OsCollector | None,
    jsonl_path: Path,
    stop_event: threading.Event,
) -> None:
    """The main polling loop: tick, append to JSONL, log the plain human-readable
    line, then sleep for whatever's left of --interval (accounting for how long the
    tick itself took). `stop_event.wait(timeout=...)` doubles as the sleep AND the
    SIGINT/SIGTERM-responsive wakeup (see main()'s signal handler) -- a signal sets
    the event, which wakes this wait immediately instead of blocking for the full
    remaining interval."""
    state = PollerState(rate_tracker=RateTracker(MONOTONIC_COUNTER_CANDIDATES))
    state.history = deque(maxlen=args.crash_history)

    while not stop_event.is_set():
        tick_start_mono = time.monotonic()
        state.tick += 1

        record = _run_tick(state.tick, args, logger, bolt, metrics_http, os_collector, state)

        _append_jsonl(jsonl_path, record)
        state.history.append(record)
        _emit_human_line(logger, record, record["derived"])

        elapsed = time.monotonic() - tick_start_mono
        sleep_for = max(args.interval - elapsed, 0.0)
        stop_event.wait(timeout=sleep_for)

    logger.info("shutdown requested, exiting cleanly")


def _run_once(
    args: argparse.Namespace,
    logger: logging.Logger,
    bolt: BoltCollector | None,
    metrics_http: MetricsHttpCollector | None,
    os_collector: OsCollector | None,
) -> None:
    """`--once`: a single tick, printed as pretty JSON to stdout (not appended to a
    JSONL file) -- for smoke-testing the collectors interactively, e.g. `--once | jq`."""
    state = PollerState(rate_tracker=RateTracker(MONOTONIC_COUNTER_CANDIDATES))
    if bolt is not None and not bolt.connected:
        try:
            bolt.connect()
        except Exception as exc:
            logger.error("Bolt connect failed: %s", exc)
    record = _run_tick(1, args, logger, bolt, metrics_http, os_collector, state)
    print(json.dumps(record, indent=2, default=str))


# --------------------------------------------------------------------------------------
# --to-csv mode
#
# diag draws NO conclusions of its own -- this mode does no analysis either. It is a
# PURELY MECHANICAL flattener: read the JSONL timeseries back, pick out the numeric
# measurement fields (never the raw nested SHOW-query blobs, those aren't graphable),
# and write one CSV row per tick so the customer/support engineer can load it into a
# spreadsheet or plotting tool and look at the trends themselves.
# --------------------------------------------------------------------------------------

# Fixed-position columns emitted by _flatten_record_to_row(), in this exact order.
# Every one of these is a plain number (or a version string / bool) lifted straight out
# of `derived` -- see the JSONL RECORD SCHEMA comment near the top of this file for what
# each of these means and how it was computed.
_TO_CSV_BASE_COLUMNS: tuple[str, ...] = (
    "ts",
    "tick",
    "connected",
    "version",
    "total_memory_tracked_bytes",  # = global_memory_tracked (instance-wide)
    "runtime_limit_bytes",
    "memory_res_bytes",
    "memory_vs_runtime_limit_pct",
    "non_storage_gap_bytes",  # = total_memory_tracked_bytes - tenant_total_memory_bytes_sum
    "tenant_total_memory_bytes_sum",  # Sum of every tenant's tenant_memory_tracked (storage+query+embedding)
    "unreleased_delta_objects_total",
    "active_txn_count",
    "longest_txn_age_s",
    "vertex_count_total",
    "edge_count_total",
)

# For each of these logical counters (see MONOTONIC_COUNTER_CANDIDATES) we emit two
# columns: the raw cumulative counter value this tick (resolved via derived.resolved_fields,
# since the physical field name drifts by version -- see RateTracker/resolve_fields), and
# the already-computed per-interval rate. Both are plain numbers, not conclusions.
_TO_CSV_COUNTER_NAMES: tuple[str, ...] = (
    "ReadQuery",
    "WriteQuery",
    "DeletedNodes",
    "DeletedEdges",
    "CommitedTransactions",
    "RollbackedTransactions",
)

# Per-tenant fields; each becomes a dynamic column named "<db_name>__<field>" (e.g.
# "tenant-a__vertex_count"). The set of tenant columns is discovered by
# scanning every record in the file first (a tenant created partway through a run must
# still get a column, just blank for earlier rows) -- see _run_to_csv(). All four
# memory buckets are included (not just storage) so nothing is hidden and everything is
# chartable: graph_mem (storage), query_mem, embedding_mem (vector index), and total_mem
# (= graph + query + embedding; this is what non_storage_gap_bytes's subtrahend sums).
_TO_CSV_PER_TENANT_FIELDS: tuple[str, ...] = (
    "vertex_count",
    "edge_count",
    "graph_mem",
    "query_mem",
    "embedding_mem",
    "total_mem",
    "unreleased_delta_objects",
    "index_count",
    "constraint_count",
)


def _flatten_record_to_row(record: Mapping[str, Any]) -> dict[str, Any]:
    """Turn one JSONL timeseries record into one flat {column: value} row.

    This mirrors (but does not recompute) the numbers already sitting in `derived` --
    it is purely a reshape from "one nested JSON object per tick" to "one wide row per
    tick", which is the shape spreadsheets/plotting tools want. See the module-level
    JSONL RECORD SCHEMA comment for what each source field means.
    """
    bolt = record.get("bolt") or {}
    sample = bolt.get("sample") or {}
    metrics = sample.get("metrics") or {}
    derived = record.get("derived") or {}
    resolved_fields = derived.get("resolved_fields") or {}
    per_tenant = derived.get("per_tenant") or {}

    row: dict[str, Any] = {
        "ts": record.get("ts"),
        "tick": record.get("tick"),
        "connected": bool(bolt.get("connected")),
        "version": (derived.get("version") or {}).get("raw"),
        "total_memory_tracked_bytes": derived.get("total_memory_tracked_bytes"),
        "runtime_limit_bytes": derived.get("runtime_limit_bytes"),
        "memory_res_bytes": derived.get("memory_res_bytes"),
        "memory_vs_runtime_limit_pct": derived.get("memory_vs_runtime_limit_pct"),
        "non_storage_gap_bytes": derived.get("non_storage_gap_bytes"),
        "tenant_total_memory_bytes_sum": derived.get("tenant_total_memory_bytes_sum"),
        # unreleased_delta_objects is tracked per-tenant (see PER_TENANT_STORAGE_FIELD_CANDIDATES),
        # so the instance-wide figure is the sum across every tenant present this tick.
        "unreleased_delta_objects_total": (
            sum(v.get("unreleased_delta_objects") or 0 for v in per_tenant.values()) if per_tenant else None
        ),
        "active_txn_count": derived.get("active_txn_count"),
        "longest_txn_age_s": derived.get("longest_txn_age_s"),
        "vertex_count_total": derived.get("vertex_count_total"),
        "edge_count_total": derived.get("edge_count_total"),
    }

    counter_deltas = derived.get("counter_deltas") or {}
    for name in _TO_CSV_COUNTER_NAMES:
        # resolved_fields maps the logical counter name to whichever physical metric
        # name actually backed it this tick (e.g. "RollbackedTransactions" on v3.10.x
        # vs "RolledBackTransactions" on v3.11.x) -- look the raw cumulative value up
        # by that physical key so the CSV always has a value regardless of version.
        physical_key = resolved_fields.get(f"metric.{name}")
        row[name] = metrics.get(physical_key) if physical_key else None
        row[f"{name}_rate_per_s"] = (counter_deltas.get(name) or {}).get("rate_per_s")

    for db_name, info in per_tenant.items():
        row[f"{db_name}__vertex_count"] = info.get("vertex_count")
        row[f"{db_name}__edge_count"] = info.get("edge_count")
        row[f"{db_name}__graph_mem"] = info.get("graph_memory_tracked_bytes")
        row[f"{db_name}__query_mem"] = info.get("query_memory_tracked_bytes")
        row[f"{db_name}__embedding_mem"] = info.get("vector_index_memory_tracked_bytes")
        row[f"{db_name}__total_mem"] = info.get("tenant_memory_tracked_bytes")
        row[f"{db_name}__unreleased_delta_objects"] = info.get("unreleased_delta_objects")
        row[f"{db_name}__index_count"] = info.get("index_count")
        row[f"{db_name}__constraint_count"] = info.get("constraint_count")

    return row


def _read_jsonl_records(path: Path) -> list[dict[str, Any]] | None:
    """Read a diag JSONL timeseries file back into a list of dicts, one per tick.
    Skips (and warns to stderr about) any malformed line rather than failing the whole
    read -- a truncated file (e.g. the process was killed mid-write) should still yield
    every complete row that came before the truncation."""
    if not path.exists():
        print(f"error: {path} does not exist", file=sys.stderr)
        return None
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as exc:
                print(f"warning: skipping malformed line {line_no}: {exc}", file=sys.stderr)
    return records


def _run_to_csv(path: Path, csv_out: Path | None) -> int:
    """`--to-csv`: read a JSONL timeseries and mechanically flatten it into a wide CSV
    (one row per tick) for graphing in a spreadsheet or plotting tool. Column set:
    fixed base/counter columns (see _TO_CSV_BASE_COLUMNS/_TO_CSV_COUNTER_NAMES) plus one
    dynamic block of columns per tenant ever seen in the file (see
    _TO_CSV_PER_TENANT_FIELDS) -- ticks before a tenant existed, or where a per-tenant
    query failed that tick, simply get a blank cell for that tenant's columns via
    `restval=""`. No scoring, ranking, or "verdict" text is produced anywhere here."""
    records = _read_jsonl_records(path)
    if records is None:
        return 2
    if not records:
        print("no usable records found in the input file", file=sys.stderr)
        return 1

    rows = [_flatten_record_to_row(r) for r in records]

    # Discover the full set of per-tenant columns across ALL rows (a tenant created or
    # first-successfully-polled partway through the run must still get a column).
    tenant_columns: set[str] = set()
    for row in rows:
        tenant_columns.update(key for key in row if "__" in key)

    fieldnames: list[str] = list(_TO_CSV_BASE_COLUMNS)
    for name in _TO_CSV_COUNTER_NAMES:
        fieldnames.append(name)
        fieldnames.append(f"{name}_rate_per_s")
    fieldnames.extend(sorted(tenant_columns))

    out_path = csv_out if csv_out is not None else path.with_suffix(".csv")
    with out_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, restval="")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    print(f"wrote {len(rows)} row(s), {len(fieldnames)} columns, to {out_path}", file=sys.stderr)
    return 0


# --------------------------------------------------------------------------------------
# --selftest mode: proves the v3.10.1 field map + tenant-correct math without needing a
# live v3.10.1 server (only a v3.11 sibling binary is available for e2e validation).
# NOTE: this does NOT test any "conclusion" logic -- diag draws none. It only checks
# the purely-arithmetic derived measurements (gap bytes, percentages, per-tenant counts)
# against known-correct expected values.
# --------------------------------------------------------------------------------------


def _selftest_state(version: VersionInfo, per_tenant_schema: Mapping[str, Any] | None = None) -> PollerState:
    """Build a PollerState pre-seeded as if an inventory sweep had already run --
    lets a selftest case call _compute_derived() directly without a live connection."""
    state = PollerState(rate_tracker=RateTracker(MONOTONIC_COUNTER_CANDIDATES))
    state.version_info = version
    state.last_per_tenant_schema = dict(per_tenant_schema or {})
    return state


def _selftest_check(condition: bool, message: str, failures: list[str]) -> None:
    """assert-like helper that COLLECTS failures instead of raising immediately, so
    _run_selftest() can report every failing check in one run rather than stopping at
    the first one."""
    if not condition:
        failures.append(message)


def _run_selftest() -> int:
    """Feeds SYNTHETIC, version-shaped rows through resolve_fields()/_compute_derived()
    (and RateTracker directly) to prove the v3.10.1 field map, the tenant-correct
    non_storage_gap computation, per-tenant index/constraint attribution, and counter-
    rename tolerance -- all without a live v3.10.1 server (only a v3.11 sibling binary
    is available for e2e validation; see --once against that instance for the real,
    non-synthetic multi-tenant proof). Exit 0 iff every assertion passes."""
    failures: list[str] = []
    logger = logging.getLogger("diag.selftest")
    logger.addHandler(logging.NullHandler())
    logger.propagate = False
    args = argparse.Namespace(crash_path=Path(os.devnull))
    version_3_10 = VersionInfo(raw="v3.10.1", major=3, minor=10)
    now = time.time()

    # -- (a) Customer's real v3.10.1 snapshot: data lives entirely in a non-default
    # tenant, default `memgraph` is empty. Grounded values from the live customer box.
    state_a = _selftest_state(version_3_10)
    record_a: dict[str, Any] = {
        "bolt": {
            "connected": True,
            "sample": {
                "storage_info_global": {
                    "global_memory_tracked": "40.00GiB",
                    "global_runtime_allocation_limit": "100.00GiB",
                    "memory_res": "40.00GiB",
                    "peak_memory_res": "40.00GiB",
                    "storage_mode": "IN_MEMORY_TRANSACTIONAL",
                },
                "storage_info_by_db": {
                    "memgraph": {
                        "graph_memory_tracked": "0B",
                        "query_memory_tracked": "0B",
                        "vector_index_memory_tracked": "0B",
                        "tenant_memory_tracked": "0B",
                        "vertex_count": 0,
                        "edge_count": 0,
                        "unreleased_delta_objects": 0,
                    },
                    # Real customer snapshot: query_memory (5.44KiB) and embedding (0B)
                    # are negligible next to storage at GiB precision, so tenant_memory
                    # (the full total) rounds to the same 15.00GiB as graph_memory alone.
                    "tenant-a": {
                        "graph_memory_tracked": "15.00GiB",
                        "query_memory_tracked": "5.44KiB",
                        "vector_index_memory_tracked": "0B",
                        "tenant_memory_tracked": "15.00GiB",
                        "vertex_count": 1000000,
                        "edge_count": 0,
                        "unreleased_delta_objects": 0,
                    },
                },
                "transactions": [],
                "metrics": {},
                "memory_info": [],
            },
            "errors": {},
            "crash_events": [],
        },
        "os": None,
    }
    derived_a = _compute_derived(record_a, state_a, now, args, logger)

    # Expected gap = global_memory_tracked - Sum(tenant_memory_tracked), NOT
    # graph_memory_tracked -- see point 1 of the gap-metric refinement.
    expected_gap_a = parse_size_to_bytes("40.00GiB") - parse_size_to_bytes("15.00GiB")
    gap_a = derived_a["non_storage_gap_bytes"]
    _selftest_check(
        gap_a is not None and abs(gap_a - expected_gap_a) <= 0.01 * expected_gap_a,
        f"(a) non_storage_gap_bytes: expected ~{_human_bytes(expected_gap_a)} (~25.00GiB), got {_human_bytes(gap_a)}",
        failures,
    )
    mem_pct_a = derived_a["memory_vs_runtime_limit_pct"]
    _selftest_check(
        mem_pct_a is not None and abs(mem_pct_a - 40.0) < 0.2,
        f"(a) memory_vs_runtime_limit_pct: expected ~40.0%, got {mem_pct_a}",
        failures,
    )
    _selftest_check(
        derived_a["resolved_fields"].get("storage.total_memory_tracked") == "global_memory_tracked",
        "(a) v3.10.1 field map should resolve total_memory_tracked via 'global_memory_tracked'",
        failures,
    )
    _selftest_check(
        derived_a["resolved_fields"].get("storage.runtime_limit") == "global_runtime_allocation_limit",
        "(a) v3.10.1 field map should resolve runtime_limit via 'global_runtime_allocation_limit'",
        failures,
    )
    _selftest_check(
        derived_a["per_tenant"]["tenant-a"]["vertex_count"] == 1000000
        and derived_a["per_tenant"]["memgraph"]["vertex_count"] == 0,
        "(a) vertex_count should attribute to the tenant that actually has data, not the default DB",
        failures,
    )

    # -- (b) Synthetic 2-tenant case: data + indexes/constraints only in the
    # non-default tenant `t1`; `memgraph` stays empty with zero schema.
    state_b = _selftest_state(
        version_3_10,
        per_tenant_schema={
            "memgraph": {"index_info": [], "constraint_info": []},
            "t1": {
                "index_info": [{"index type": "label", "label": "Node", "property": None, "count": 1000}] * 7,
                "constraint_info": [{"constraint type": "unique", "label": "Node", "properties": "id"}] * 2,
            },
        },
    )
    record_b: dict[str, Any] = {
        "bolt": {
            "connected": True,
            "sample": {
                "storage_info_global": {
                    "global_memory_tracked": "10GiB",
                    "global_runtime_allocation_limit": "20GiB",
                    "memory_res": "10GiB",
                },
                "storage_info_by_db": {
                    "memgraph": {
                        "graph_memory_tracked": "0.1GiB",
                        "query_memory_tracked": "0B",
                        "vector_index_memory_tracked": "0B",
                        "tenant_memory_tracked": "0.1GiB",
                        "vertex_count": 0,
                        "edge_count": 0,
                        "unreleased_delta_objects": 0,
                    },
                    # Deliberately graph_memory (2.9GiB) != tenant_memory (3GiB) here --
                    # this tenant also has a non-trivial query_memory bucket (0.1GiB),
                    # so the gap assertion below only passes if the code sums
                    # tenant_memory_tracked (the full total), not graph_memory_tracked
                    # (storage only) as an earlier, less-correct version of this script did.
                    "t1": {
                        "graph_memory_tracked": "2.9GiB",
                        "query_memory_tracked": "0.1GiB",
                        "vector_index_memory_tracked": "0B",
                        "tenant_memory_tracked": "3GiB",
                        "vertex_count": 500000,
                        "edge_count": 200000,
                        "unreleased_delta_objects": 10,
                    },
                },
                "transactions": [],
                "metrics": {},
                "memory_info": [],
            },
            "errors": {},
            "crash_events": [],
        },
        "os": None,
    }
    derived_b = _compute_derived(record_b, state_b, now, args, logger)

    expected_gap_b = parse_size_to_bytes("10GiB") - (parse_size_to_bytes("0.1GiB") + parse_size_to_bytes("3GiB"))
    gap_b = derived_b["non_storage_gap_bytes"]
    _selftest_check(
        gap_b is not None and abs(gap_b - expected_gap_b) <= 0.01 * abs(expected_gap_b or 1),
        "(b) non_storage_gap_bytes must use the SUM of each tenant's FULL tracked total "
        f"(tenant_memory_tracked, not just graph_memory_tracked): expected {_human_bytes(expected_gap_b)}, "
        f"got {_human_bytes(gap_b)}",
        failures,
    )
    per_tenant_b = derived_b["per_tenant"]
    _selftest_check(
        per_tenant_b.get("t1", {}).get("graph_memory_tracked_bytes") == parse_size_to_bytes("2.9GiB")
        and per_tenant_b.get("t1", {}).get("query_memory_tracked_bytes") == parse_size_to_bytes("0.1GiB")
        and per_tenant_b.get("t1", {}).get("tenant_memory_tracked_bytes") == parse_size_to_bytes("3GiB"),
        "(b) all per-tenant memory buckets (graph/query/embedding/total) must be captured "
        "distinctly, not conflated into a single figure",
        failures,
    )
    _selftest_check(
        per_tenant_b.get("memgraph", {}).get("index_count") == 0,
        f"(b) default DB should show 0 indexes, got {per_tenant_b.get('memgraph', {}).get('index_count')}",
        failures,
    )
    _selftest_check(
        per_tenant_b.get("t1", {}).get("index_count") == 7,
        f"(b) tenant t1 should show 7 indexes, got {per_tenant_b.get('t1', {}).get('index_count')}",
        failures,
    )
    _selftest_check(
        per_tenant_b.get("t1", {}).get("constraint_count") == 2,
        f"(b) tenant t1 should show 2 constraints, got {per_tenant_b.get('t1', {}).get('constraint_count')}",
        failures,
    )
    _selftest_check(
        per_tenant_b.get("t1", {}).get("vertex_count") == 500000
        and per_tenant_b.get("memgraph", {}).get("vertex_count") == 0,
        "(b) vertex counts must attribute to the correct tenant (t1=500000, memgraph=0)",
        failures,
    )

    # -- (c) Metrics counter rename tolerance: RollbackedTransactions (v3.10.x) vs
    # RolledBackTransactions (v3.11.x) must both resolve via the candidate list.
    tracker_310 = RateTracker(MONOTONIC_COUNTER_CANDIDATES)
    tracker_310.update({"RollbackedTransactions": 5}, now)
    _, _, resolved_310 = tracker_310.update({"RollbackedTransactions": 7}, now + 1.0)
    _selftest_check(
        resolved_310.get("RollbackedTransactions") == "RollbackedTransactions",
        "(c) v3.10.x rollback counter should resolve via the literal name 'RollbackedTransactions'",
        failures,
    )
    tracker_311 = RateTracker(MONOTONIC_COUNTER_CANDIDATES)
    tracker_311.update({"RolledBackTransactions": 5}, now)
    _, _, resolved_311 = tracker_311.update({"RolledBackTransactions": 9}, now + 1.0)
    _selftest_check(
        resolved_311.get("RollbackedTransactions") == "RolledBackTransactions",
        "(c) v3.11.x-renamed rollback counter should still resolve via the candidate fallback",
        failures,
    )

    if failures:
        print(f"SELFTEST FAILED ({len(failures)} of {len(failures)} shown):")
        for failure in failures:
            print(f"  - {failure}")
        return 1
    print(
        "SELFTEST PASSED: v3.10.1 field map, tenant-correct non_storage_gap, "
        "per-tenant index/constraint attribution, and counter-rename tolerance all verified."
    )
    return 0


# --------------------------------------------------------------------------------------
# Logging / banner / CLI
# --------------------------------------------------------------------------------------


def _build_logger(log_path: Path) -> logging.Logger:
    """One logger, two destinations: stderr (so stdout stays a clean JSON channel for
    --once) and the per-run .log file. `propagate = False` keeps this from also
    going through the root logger (avoiding duplicate lines if something else in the
    process configures logging)."""
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    # Logs go to stderr so stdout stays a clean JSON channel (e.g. `--once | jq`).
    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setFormatter(formatter)
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
    logger.propagate = False
    return logger


def _print_banner(
    logger: logging.Logger,
    args: argparse.Namespace,
    bolt: BoltCollector | None,
    metrics_http: MetricsHttpCollector | None,
    os_collector: OsCollector | None,
    jsonl_path: Path,
    log_path: Path,
    crash_path: Path,
) -> None:
    """Print a startup summary of which collectors are active and where output is
    going -- the first thing an operator (or an LLM tailing the log) sees, so a
    misconfiguration (e.g. forgot --bolt-address) is obvious immediately rather than
    discovered after the fact from an empty JSONL file."""
    lines = [
        "diag -- Memgraph v3.10.1 read-only diagnostic poller",
        "  Bolt collector:  "
        + (f"ENABLED  {args.bolt_address}" if bolt else "disabled (pass --bolt-address to enable)"),
        "  HTTP metrics:    "
        + (f"ENABLED  {args.metrics_url}" if metrics_http else "disabled (pass --metrics-url to enable)"),
        "  OS collector:    "
        + (
            f"ENABLED  pid={args.pid if args.pid else 'auto-detect'}"
            if os_collector
            else "disabled (pass --os to enable)"
        ),
        f"  interval={args.interval}s  inventory_interval={args.inventory_interval}s  query_timeout={args.query_timeout}s",
        f"  jsonl={jsonl_path}",
        f"  log={log_path}",
        f"  crash_log={crash_path}",
    ]
    if bolt is None and metrics_http is None:
        lines.append(
            "  NOTE: neither Bolt nor HTTP metrics is configured -- only OS-level signals (if --os) will be collected."
        )
    for line in lines:
        logger.info(line)


_ARG_PARSER_EPILOG = """\
IMPORTANT: diag is READ-ONLY and draws NO conclusions. It logs a complete timeseries
of Memgraph SHOW/INFO query results (JSONL, one object per tick) plus purely-arithmetic
measurements (memory-attribution gap, per-tenant vertex/edge/memory counts, counter
rates, ...) for YOU to graph and analyze -- it never diagnoses, flags, or ranks
anything itself. See the module docstring (top of this file) for the full JSONL record
schema.

Examples
--------
  External, Bolt-only, polling every tenant on a customer's instance:
    python3 diag.py --bolt-address db.example.com:7687 --username diag --password '...' \\
        --output-dir ./diag-out

  Same, backgrounded for a long unattended run:
    nohup python3 diag.py --bolt-address db.example.com:7687 --username diag \\
        --password '...' --output-dir ./diag-out > diag.nohup.log 2>&1 &

  In-pod, with OS/cgroup signals (run this INSIDE the memgraph pod/container):
    python3 diag.py --bolt-address localhost:7687 --os --output-dir /var/log/diag

  Turn a completed run into a CSV for graphing in a spreadsheet/plotting tool:
    python3 diag.py --to-csv ./diag-out/diag_20260101T000000Z.jsonl
"""


def _build_arg_parser() -> argparse.ArgumentParser:
    """Build the CLI. Options are grouped by collector (Bolt / HTTP metrics / OS),
    then Sampling (loop timing + crash-detection knobs), Output (where files go), and
    Modes (alternate one-shot invocations: --once, --to-csv, --selftest)."""
    parser = argparse.ArgumentParser(
        prog=PROG_NAME,
        description=__doc__,
        epilog=_ARG_PARSER_EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    bolt_group = parser.add_argument_group(
        "Bolt collector", "Primary collector: runs read-only SHOW ... admin queries over Bolt."
    )
    bolt_group.add_argument(
        "--bolt-address",
        metavar="HOST:PORT",
        help="e.g. localhost:7687. Enables the Bolt collector (default: disabled).",
    )
    bolt_group.add_argument("--username", default="", help="Bolt username (default: empty, i.e. no auth)")
    bolt_group.add_argument("--password", default="", help="Bolt password (default: empty)")
    bolt_group.add_argument(
        "--database",
        default=None,
        metavar="NAME",
        help="Driver's default/landing database; also the reset target after each per-tenant "
        "USE DATABASE switch (default: memgraph)",
    )
    bolt_group.add_argument(
        "--databases",
        default=None,
        metavar="db1,db2",
        help="Comma-separated tenant/database names to poll (default: ALL databases returned "
        "by SHOW DATABASES -- a customer's real data commonly lives in a non-default tenant, "
        "so the default is every tenant, not just 'memgraph')",
    )

    metrics_group = parser.add_argument_group(
        "HTTP metrics collector",
        "Secondary, optional: GETs a JSON metrics endpoint. Redundant with SHOW METRICS INFO "
        "over Bolt, so it disables itself automatically if unreachable/license-gated.",
    )
    metrics_group.add_argument(
        "--metrics-url",
        metavar="URL",
        help="e.g. http://host:9091/. Enables the HTTP metrics collector (default: disabled).",
    )

    os_group = parser.add_argument_group(
        "OS collector",
        "Opt-in: best-effort /proc + cgroup + dmesg/journalctl reads. Only useful when run "
        "INSIDE the same pod/container/namespace as the memgraph process.",
    )
    os_group.add_argument(
        "--os", action="store_true", help="Enable /proc + cgroup + dmesg collection (default: disabled)."
    )
    os_group.add_argument(
        "--pid", type=int, default=None, metavar="PID", help="memgraph PID (default: auto-detect by process name)"
    )

    sampling_group = parser.add_argument_group(
        "Sampling", "Poll cadence and the crash/restart-detection knobs (see run_loop/_collect_bolt_tick)."
    )
    sampling_group.add_argument(
        "--interval",
        type=float,
        default=DEFAULT_INTERVAL_S,
        metavar="SECONDS",
        help="High-frequency sample tick interval (default: %(default)s)",
    )
    sampling_group.add_argument(
        "--inventory-interval",
        type=float,
        default=DEFAULT_INVENTORY_INTERVAL_S,
        metavar="SECONDS",
        help="Full inventory (SHOW CONFIG/DATABASES/REPLICAS/... + per-tenant index/constraint "
        "sweep) interval; inventory changes rarely, so this is much less frequent than "
        "--interval (default: %(default)s)",
    )
    sampling_group.add_argument(
        "--query-timeout",
        type=float,
        default=DEFAULT_QUERY_TIMEOUT_S,
        metavar="SECONDS",
        help="Hard wall-clock timeout for any single Bolt query (default: %(default)s)",
    )
    sampling_group.add_argument(
        "--crash-history",
        type=int,
        default=DEFAULT_CRASH_HISTORY,
        metavar="N",
        help="Number of pre-restart ticks to dump to diag_crashes.log when a restart is "
        "detected (default: %(default)s)",
    )
    sampling_group.add_argument(
        "--crash-outage-threshold",
        type=float,
        default=DEFAULT_CRASH_OUTAGE_THRESHOLD_S,
        metavar="SECONDS",
        help="Outage duration above which a reconnect is logged as a CRASH_DETECTED event "
        "rather than a routine transient blip (default: %(default)s)",
    )

    output_group = parser.add_argument_group("Output", "Where the timeseries/log/crash files are written.")
    output_group.add_argument(
        "--output-dir",
        type=Path,
        default=Path.cwd(),
        metavar="DIR",
        help="Directory for the jsonl/log/crash files (default: current directory)",
    )

    modes_group = parser.add_argument_group(
        "Modes", "Alternate one-shot invocations; each of these runs once and exits (no loop)."
    )
    modes_group.add_argument(
        "--once",
        action="store_true",
        help="Take a single timeseries tick, print its JSON record to stdout, and exit.",
    )
    modes_group.add_argument(
        "--to-csv",
        type=Path,
        default=None,
        metavar="PATH.jsonl",
        help="Read a JSONL timeseries file and mechanically flatten it into a wide CSV "
        "(one row per tick, one column per numeric measurement) for graphing, then exit. "
        "No analysis is performed -- see --csv-out.",
    )
    modes_group.add_argument(
        "--csv-out",
        type=Path,
        default=None,
        metavar="FILE.csv",
        help="Output path for --to-csv (default: the input path with its suffix replaced by .csv)",
    )
    modes_group.add_argument(
        "--selftest",
        action="store_true",
        help="Run built-in checks against synthetic v3.10.1-shaped data (no live server "
        "needed) and exit -- proves the version-aware field map and the tenant-correct "
        "arithmetic, not any interpretation (diag has none).",
    )

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point: parse args, dispatch to one of the one-shot modes (--selftest,
    --to-csv) if requested, otherwise set up the configured collectors and either take
    a single snapshot (--once) or run the loop forever until SIGINT/SIGTERM."""
    parser = _build_arg_parser()
    args = parser.parse_args(argv)
    args.databases_list = [d.strip() for d in args.databases.split(",") if d.strip()] if args.databases else None

    if args.selftest:
        return _run_selftest()

    if args.to_csv is not None:
        return _run_to_csv(args.to_csv, args.csv_out)

    if not args.bolt_address and not args.metrics_url and not args.os:
        parser.error("no collector configured: pass --bolt-address, --metrics-url, and/or --os")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    ts = _utc_ts_compact()
    jsonl_path = args.output_dir / f"diag_{ts}.jsonl"
    log_path = args.output_dir / f"diag_{ts}.log"
    crash_path = args.output_dir / "diag_crashes.log"
    args.crash_path = crash_path  # threaded through to _compute_derived / _collect_bolt_tick

    logger = _build_logger(log_path)

    bolt: BoltCollector | None = None
    metrics_http: MetricsHttpCollector | None = None
    os_collector: OsCollector | None = None

    if args.bolt_address:
        try:
            bolt = BoltCollector(
                BoltConfig(
                    address=args.bolt_address,
                    username=args.username,
                    password=args.password,
                    database=args.database,
                    query_timeout_s=args.query_timeout,
                ),
                logger,
            )
        except RuntimeError as exc:
            logger.error(str(exc))
            return 2

    if args.metrics_url:
        metrics_http = MetricsHttpCollector(
            MetricsHttpConfig(url=args.metrics_url, timeout_s=args.query_timeout), logger
        )

    if args.os:
        os_collector = OsCollector(OsConfig(pid=args.pid), logger)

    _print_banner(logger, args, bolt, metrics_http, os_collector, jsonl_path, log_path, crash_path)

    if args.once:
        _run_once(args, logger, bolt, metrics_http, os_collector)
        if bolt is not None:
            bolt.close()
        return 0

    stop_event = threading.Event()

    def _handle_signal(signum: int, _frame: Any) -> None:
        """Set the stop event rather than raising/exiting directly -- run_loop's
        `stop_event.wait(timeout=...)` wakes up immediately, finishes flushing the
        current tick's files, and returns, giving a clean shutdown instead of an
        abrupt kill mid-write."""
        logger.warning("received signal %s, shutting down after this tick", signal.Signals(signum).name)
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        run_loop(args, logger, bolt, metrics_http, os_collector, jsonl_path, stop_event)
    finally:
        if bolt is not None:
            bolt.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
