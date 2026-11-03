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

"""Shared helpers for the commit-lock-narrowing e2e suites (bounded calls, DDL contender, log probe, client runner)."""

import os
import re
import threading
from collections.abc import Callable, Iterator
from typing import Any

from neo4j import Driver, GraphDatabase

URI = f"bolt://127.0.0.1:{os.getenv('MEMGRAPH_BOLT_PORT', '7687')}"
CALL_TIMEOUT_S = 60.0

# The harness writes the log under <build>/e2e/logs/. spdlog's daily_file_sink inserts _YYYY-MM-DD before the extension,
# and the parallel runner appends -w<slot> to the stem. The strict pattern keeps the auth workload's
# commit_lock_narrowing_auth*.log out; the newest match is the live one.
_BUILD_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
LOG_DIR = os.path.join(_BUILD_DIR, "e2e", "logs")
LOG_NAME_RE = re.compile(r"commit_lock_narrowing(-w\d+)?(_\d{4}-\d{2}-\d{2})?\.log")


def find_log_files(log_dir: str = LOG_DIR) -> list[str]:
    """Paths of this suite's memgraph log files (plain or date-suffixed) in log_dir."""
    try:
        names = os.listdir(log_dir)
    except FileNotFoundError:
        return []
    return [os.path.join(log_dir, n) for n in names if LOG_NAME_RE.fullmatch(n)]


# (driver, client index, iteration, nodes) -> nodes_created reported by the write's summary
WriteFn = Callable[[Driver, int, int, int], int]


def run_bounded(fn: Callable[[], object], timeout: float = CALL_TIMEOUT_S) -> object:
    """Run fn in a daemon thread; fail (instead of hanging CI) if it does not return in time."""
    out: dict = {}

    def target() -> None:
        try:
            out["value"] = fn()
        except Exception as e:  # noqa: BLE001 - reported to the caller
            out["error"] = e

    t = threading.Thread(target=target, daemon=True)
    t.start()
    t.join(timeout)
    assert not t.is_alive(), f"call did not return within {timeout}s (hung client / lost acknowledgement)"
    if "error" in out:
        raise out["error"]
    return out.get("value")


class LogProbe:
    """Incrementally scans the memgraph log, from the moment of construction, for lines containing a needle."""

    def __init__(self) -> None:
        candidates = find_log_files()
        assert candidates, f"memgraph did not create a log file matching {LOG_NAME_RE.pattern} in {LOG_DIR}"
        self._path = max(candidates, key=os.path.getmtime)
        self._pos = os.path.getsize(self._path)
        self._hits: set[str] = set()
        self._lock = threading.Lock()

    def seen(self, needle: str) -> bool:
        with self._lock:
            if needle in self._hits:
                return True
            with open(self._path, "rb") as f:
                f.seek(self._pos)
                chunk = f.read()
            end = chunk.rfind(b"\n") + 1  # whole lines only; a partial last line is re-read next call
            self._pos += end
            text = chunk[:end].decode(errors="replace")
            if needle in text:
                self._hits.add(needle)
                return True
            return False


class DdlCounter:
    """Successful contender DDL statements; written by the contender thread only."""

    def __init__(self) -> None:
        self.ok = 0
        self.first = threading.Event()  # set after the first successful DDL


def start_contender(pause_s: float, auth: Any = None) -> Iterator[DdlCounter]:
    """Back-to-back (pause_s apart) index DDL, i.e. unique access, to keep main_lock_ contended; yields the counter."""
    stop = threading.Event()
    counter = DdlCounter()

    def loop() -> None:
        with GraphDatabase.driver(URI, auth=auth) as d:
            while not stop.is_set():
                for q in ("CREATE INDEX ON :L(p)", "DROP INDEX ON :L(p)"):
                    try:
                        with d.session() as s:
                            s.run(q).consume()
                        counter.ok += 1
                        counter.first.set()
                    except Exception:  # noqa: BLE001 - DDL races are expected; contention is asserted via the counter
                        pass
                    stop.wait(pause_s)

    t = threading.Thread(target=loop, daemon=True)
    t.start()
    try:
        yield counter
    finally:
        stop.set()
        t.join(CALL_TIMEOUT_S)


def run_clients(
    write: WriteFn,
    n_clients: int,
    iterations: int,
    nodes: int,
    pool_size: int | None = None,
    keep_going: Callable[[], bool] = lambda: True,
    tolerate: Callable[[Exception], bool] = lambda e: False,
    acked: set[tuple[int, int]] | None = None,
    auth: Any = None,
) -> list[int]:
    """Each client does bounded writes, at most `iterations`, stopping early once keep_going() is False (after at
    least one). Any error or hang fails, except errors accepted by tolerate() (counted as not-ok). Acked (c,k) pairs
    are added to `acked` if given. Returns the successful call count per client."""
    done: list[int] = [0] * n_clients
    errors: list[str] = []
    pool = {} if pool_size is None else {"max_connection_pool_size": pool_size}

    def client(i: int) -> None:
        try:
            with GraphDatabase.driver(URI, auth=auth, max_transaction_retry_time=0, **pool) as d:
                for k in range(iterations):
                    if k > 0 and not keep_going():
                        break
                    try:
                        created = run_bounded(lambda: write(d, i, k, nodes))
                    except Exception as e:  # noqa: BLE001
                        if tolerate(e):
                            continue
                        raise
                    assert created == nodes, f"summary reported {created} created nodes (client {i}, iteration {k})"
                    done[i] += 1
                    if acked is not None:
                        acked.add((i, k))
        except Exception as e:  # noqa: BLE001
            errors.append(repr(e))

    threads = [threading.Thread(target=client, args=(i,), daemon=True) for i in range(n_clients)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(CALL_TIMEOUT_S * iterations)
    assert not any(t.is_alive() for t in threads), "client thread hung"
    assert not errors, f"client errors: {errors[:5]}"
    return done


def assert_writes_landed_once(pairs: set[tuple[int, int]], ok: int, nodes: int) -> None:
    with GraphDatabase.driver(URI, auth=None) as d, d.session() as s:
        rows = s.run("MATCH (n:N) RETURN n.c AS c, n.k AS k, count(*) AS m").data()
    assert {(r["c"], r["k"]) for r in rows} == pairs
    assert len(rows) == ok
    assert all(r["m"] == nodes for r in rows), f"per-(c,k) multiplicity != {nodes}: {rows[:5]}"
