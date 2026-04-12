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
            storage_info=["vertex_count", "edge_count", "memory_res"],
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
        interval: float = 5.0,
    ):
        self._host = host
        self._port = port
        self._storage_fields = storage_info or []
        self._interval = interval
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._driver = GraphDatabase.driver(f"bolt://{host}:{port}", auth=("", ""))

    def get_storage_info(self) -> dict[str, Any]:
        """One-shot: query SHOW STORAGE INFO and return as dict."""
        with self._driver.session() as session:
            result = session.run("SHOW STORAGE INFO;")
            return {row["storage info"]: row["value"] for row in result}

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

    def _monitor_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.log_storage_info()
            except Exception as e:
                print(f"[STORAGE INFO ERROR] {e}")
            self._stop_event.wait(self._interval)

    def start(self) -> None:
        """Start background monitoring thread."""
        if self._storage_fields:
            self._thread = threading.Thread(target=self._monitor_loop, daemon=True)
            self._thread.start()
            print(f"[StandaloneMonitor] Started (fields: {self._storage_fields}, interval: {self._interval}s)")

    def stop(self) -> None:
        """Stop background monitoring and close driver."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None
        self._driver.close()
        print("[StandaloneMonitor] Stopped")

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *exc):
        self.stop()
