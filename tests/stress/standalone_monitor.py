"""
Lightweight monitor for standalone Memgraph instances.

Provides background and one-shot storage info monitoring.
Similar to ClusterMonitor but for single-instance deployments.
"""

import threading
import time
from typing import Any

from neo4j import GraphDatabase


class StandaloneMonitor:
    """Monitor for a standalone Memgraph instance.

    Usage::

        monitor = StandaloneMonitor(
            host="127.0.0.1",
            port=7687,
            storage_info=["vertex_count", "edge_count", "memory_res", "memory_limit"],
            interval=10,
        )
        with monitor:
            # ... run workload (background thread logs periodically) ...

        # Or one-shot:
        info = monitor.get_storage_info()
        print(info["memory_res"])
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 7687,
        storage_info: list[str] | None = None,
        metrics_info: list[str] | None = None,
        interval: float = 5.0,
    ):
        self._host = host
        self._port = port
        self._storage_fields = storage_info or []
        self._metrics_fields = metrics_info or []
        self._interval = interval
        self._stop_event = threading.Event()
        self._threads: list[threading.Thread] = []
        self._driver = GraphDatabase.driver(f"bolt://{host}:{port}", auth=("", ""))

    def get_storage_info(self) -> dict[str, Any]:
        """One-shot: query both global and per-DB SHOW STORAGE INFO and return the merged dict.

        Per-DB fields (e.g. vertex_count, edge_count) are scoped to the default
        database. Per-DB fields take precedence over global ones on key collision.
        """
        with self._driver.session() as session:
            merged: dict[str, Any] = {row["storage info"]: row["value"] for row in session.run("SHOW STORAGE INFO;")}
            for row in session.run("SHOW STORAGE INFO ON CURRENT DATABASE;"):
                merged[row["storage info"]] = row["value"]
            return merged

    def log_storage_info(self, label: str = "") -> dict[str, Any]:
        """Fetch and print storage info. Returns the info dict."""
        info = self.get_storage_info()
        prefix = f"[{label}] " if label else ""
        if self._storage_fields:
            parts = [f"{f}={info.get(f, '?')}" for f in self._storage_fields]
            print(f"{prefix}[STORAGE INFO @ {time.strftime('%H:%M:%S')}] {' '.join(parts)}")
        else:
            print(f"{prefix}[STORAGE INFO @ {time.strftime('%H:%M:%S')}] {info}")
        return info

    def get_metrics(self) -> dict[str, Any]:
        """One-shot: query SHOW METRICS and return a {name: value} dict."""
        with self._driver.session() as session:
            return {row["name"]: row["value"] for row in session.run("SHOW METRICS;")}

    def log_metrics(self, label: str = "") -> dict[str, Any]:
        """Fetch and print metrics. Returns the metrics dict."""
        metrics = self.get_metrics()
        prefix = f"[{label}] " if label else ""
        if self._metrics_fields:
            parts = [f"{f}={metrics.get(f, '?')}" for f in self._metrics_fields]
            print(f"{prefix}[METRICS @ {time.strftime('%H:%M:%S')}] {' '.join(parts)}")
        else:
            print(f"{prefix}[METRICS @ {time.strftime('%H:%M:%S')}] {metrics}")
        return metrics

    def _storage_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.log_storage_info()
            except Exception as e:
                print(f"[STORAGE INFO ERROR] {e}")
            self._stop_event.wait(self._interval)

    def _metrics_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.log_metrics()
            except Exception as e:
                # SHOW METRICS is enterprise-only; without a license it will
                # never succeed, so warn once and stop polling instead of
                # spamming the same error every interval.
                if "requires an enterprise" in str(e):
                    print(f"[METRICS] disabled: {e}")
                    return
                print(f"[METRICS ERROR] {e}")
            self._stop_event.wait(self._interval)

    def start(self) -> None:
        """Start background monitoring threads."""
        if self._storage_fields:
            t = threading.Thread(target=self._storage_loop, daemon=True)
            t.start()
            self._threads.append(t)
            print(
                f"[StandaloneMonitor] STORAGE INFO worker started (fields: {self._storage_fields}, interval: {self._interval}s)"
            )
        if self._metrics_fields:
            t = threading.Thread(target=self._metrics_loop, daemon=True)
            t.start()
            self._threads.append(t)
            print(
                f"[StandaloneMonitor] METRICS worker started (fields: {self._metrics_fields}, interval: {self._interval}s)"
            )

    def stop(self) -> None:
        """Stop background monitoring and close driver."""
        self._stop_event.set()
        for t in self._threads:
            t.join(timeout=5)
        self._threads.clear()
        self._driver.close()
        print("[StandaloneMonitor] Stopped")

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *exc):
        self.stop()
