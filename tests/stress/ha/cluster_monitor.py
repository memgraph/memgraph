"""
Reusable background cluster monitor for HA stress-test workloads.

Spawns daemon threads that periodically query coordinators and print
diagnostic output. Aborts the process (os._exit) on fatal conditions
(instance down).
"""
import os
import threading
import time
from typing import Any

from ha_common import Protocol, QueryType, execute_and_fetch


class ClusterMonitor:
    """Background monitor for HA cluster health during workloads.

    Accepts a list of coordinators and falls back to the next one if a
    query fails, so monitoring survives coordinator restarts/failovers.

    Usage::

        monitor = ClusterMonitor(
            coordinators=["coord_1", "coord_2", "coord_3"],
            show_replicas=True,
            show_instances=True,
            verify_up=True,
            storage_info=["vertex_count", "edge_count", "memory_res"],
            interval=5,
        )
        with monitor:
            # ... run workload ...

        if not monitor.verify_all_ready():
            sys.exit(1)
    """

    def __init__(
        self,
        coordinators: list[str] | str = "coord_1",
        show_replicas: bool = False,
        show_instances: bool = False,
        verify_up: bool = False,
        storage_info: list[str] | None = None,
        interval: float = 5.0,
    ):
        if isinstance(coordinators, str):
            coordinators = [coordinators]
        self._coordinators = coordinators
        self._show_replicas = show_replicas
        self._show_instances = show_instances
        self._verify_up = verify_up
        self._storage_fields = storage_info or []
        self._interval = interval
        self._stop_event = threading.Event()
        self._threads: list[threading.Thread] = []

    def _query(self, query: str, query_type: QueryType = QueryType.READ) -> tuple[str, list[dict[str, Any]]]:
        """Try each coordinator in order; return (coordinator_name, results) from the first that responds."""
        last_err = None
        for coord in self._coordinators:
            try:
                return coord, execute_and_fetch(coord, query, protocol=Protocol.BOLT_ROUTING, query_type=query_type)
            except Exception as e:
                last_err = e
        raise last_err  # type: ignore[misc]

    def start(self) -> None:
        if self._show_replicas:
            t = threading.Thread(target=self._replicas_loop, daemon=True)
            t.start()
            self._threads.append(t)
            print(f"[ClusterMonitor] SHOW REPLICAS worker started (interval: {self._interval}s)")

        if self._show_instances:
            t = threading.Thread(target=self._instances_loop, daemon=True)
            t.start()
            self._threads.append(t)
            print(f"[ClusterMonitor] SHOW INSTANCES worker started (interval: {self._interval}s)")

        if self._verify_up:
            t = threading.Thread(target=self._verify_up_loop, daemon=True)
            t.start()
            self._threads.append(t)
            print(f"[ClusterMonitor] Instance health check worker started (interval: {self._interval}s)")

        if self._storage_fields:
            t = threading.Thread(target=self._storage_info_loop, daemon=True)
            t.start()
            self._threads.append(t)
            print(
                f"[ClusterMonitor] STORAGE INFO worker started (fields: {self._storage_fields}, interval: {self._interval}s)"
            )

        print(f"[ClusterMonitor] Using coordinators: {self._coordinators}")

    def stop(self) -> None:
        self._stop_event.set()
        for t in self._threads:
            t.join(timeout=5)
        self._threads.clear()
        print("[ClusterMonitor] Stopped")

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *exc):
        self.stop()

    # -- Background loops -----------------------------------------------------

    def _replicas_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                coord, rows = self._query("SHOW REPLICAS;")
                if rows:
                    headers = list(rows[0].keys())
                    print(f"\n[SHOW REPLICAS @ {time.strftime('%H:%M:%S')} via {coord}]")
                    print("  " + ",".join(headers))
                    for row in rows:
                        print("  " + ",".join(str(row[h]) for h in headers))
            except Exception as e:
                print(f"\n[SHOW REPLICAS ERROR] {e}")
            self._stop_event.wait(self._interval)

    def _instances_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                coord, rows = self._query("SHOW INSTANCES;")
                if rows:
                    headers = list(rows[0].keys())
                    print(f"\n[SHOW INSTANCES @ {time.strftime('%H:%M:%S')} via {coord}]")
                    print("  " + ",".join(headers))
                    for row in rows:
                        print("  " + ",".join(str(row[h]) for h in headers))
            except Exception as e:
                print(f"\n[SHOW INSTANCES ERROR] {e}")
            self._stop_event.wait(self._interval)

    def _verify_up_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                coord, rows = self._query("SHOW INSTANCES;")
                for row in rows:
                    name = row.get("name", "unknown")
                    alive = row.get("alive", row.get("is_alive", None))
                    if alive is False:
                        print(f"\n  FATAL: Instance '{name}' is DOWN! (via {coord})")
                        os._exit(1)
            except Exception as e:
                print(f"\n[HEALTH CHECK ERROR] {e}")
            self._stop_event.wait(self._interval)

    def _storage_info_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                coord, rows = self._query("SHOW STORAGE INFO;")
                info = {row["storage info"]: row["value"] for row in rows if "storage info" in row}
                parts = [f"{f}={info.get(f, '?')}" for f in self._storage_fields]
                print(f"\n[STORAGE INFO @ {time.strftime('%H:%M:%S')} via {coord}] {' '.join(parts)}")
            except Exception as e:
                print(f"\n[STORAGE INFO ERROR] {e}")
            self._stop_event.wait(self._interval)

    # -- One-shot verification (call after workload) --------------------------

    def show_replicas(self) -> None:
        """Print SHOW REPLICAS once."""
        coord, rows = self._query("SHOW REPLICAS;")
        print(f"[via {coord}]")
        if rows:
            headers = list(rows[0].keys())
            print(",".join(headers))
            for row in rows:
                print(",".join(str(row[h]) for h in headers))

    def verify_all_ready(self) -> bool:
        """Check that all replicas have status 'ready'. Returns True if healthy."""
        coord, rows = self._query("SHOW REPLICAS;")
        print(f"[verify_all_ready via {coord}]")
        all_ready = True
        for row in rows:
            name = row.get("name", "unknown")
            data_info = row.get("data_info", {})
            for db_name, db_status in data_info.items():
                status = db_status.get("status", "unknown")
                if status != "ready":
                    print(f"WARN: Replica {name}, database {db_name} status is '{status}', expected 'ready'")
                    all_ready = False
        if all_ready:
            print("All replicas are in sync!")
        return all_ready

    def verify_instances_up(self) -> bool:
        """Check that all instances are alive. Returns True if all up."""
        coord, rows = self._query("SHOW INSTANCES;")
        print(f"[verify_instances_up via {coord}]")
        all_up = True
        for row in rows:
            name = row.get("name", "unknown")
            alive = row.get("alive", row.get("is_alive", None))
            if alive is False:
                print(f"ERROR: Instance '{name}' is DOWN!")
                all_up = False
        if all_up:
            print("All instances are up!")
        return all_up
