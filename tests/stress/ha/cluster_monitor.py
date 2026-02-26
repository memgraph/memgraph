"""
Reusable background cluster monitor for HA stress-test workloads.

Spawns daemon threads that periodically query the coordinator and print
diagnostic output. Aborts the process (os._exit) on fatal conditions
(replica invalid, instance down).
"""
import os
import threading
import time

from ha_common import Protocol, QueryType, execute_and_fetch


class ClusterMonitor:
    """Background monitor for HA cluster health during workloads.

    Usage::

        monitor = ClusterMonitor(
            coordinator="coord_1",
            show_replicas=True,
            show_instances=True,
            verify_up=True,
            storage_info=["vertex_count", "edge_count", "memory_res"],
            interval=5,
        )
        monitor.start()
        try:
            # ... run workload ...
        finally:
            monitor.stop()

        # After workload: optional final verification
        if not monitor.verify_all_ready():
            sys.exit(1)
    """

    def __init__(
        self,
        coordinator: str = "coord_1",
        show_replicas: bool = False,
        show_instances: bool = False,
        verify_up: bool = False,
        storage_info: list[str] | None = None,
        interval: float = 5.0,
    ):
        self._coordinator = coordinator
        self._show_replicas = show_replicas
        self._show_instances = show_instances
        self._verify_up = verify_up
        self._storage_fields = storage_info or []
        self._interval = interval
        self._stop_event = threading.Event()
        self._threads: list[threading.Thread] = []

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
                rows = execute_and_fetch(self._coordinator, "SHOW REPLICAS;", protocol=Protocol.BOLT_ROUTING)
                if rows:
                    headers = list(rows[0].keys())
                    print(f"\n[SHOW REPLICAS @ {time.strftime('%H:%M:%S')}]")
                    print("  " + ",".join(headers))
                    for row in rows:
                        print("  " + ",".join(str(row[h]) for h in headers))
            except Exception as e:
                print(f"\n[SHOW REPLICAS ERROR] {e}")
            self._stop_event.wait(self._interval)

    def _instances_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                rows = execute_and_fetch(self._coordinator, "SHOW INSTANCES;", protocol=Protocol.BOLT_ROUTING)
                if rows:
                    headers = list(rows[0].keys())
                    print(f"\n[SHOW INSTANCES @ {time.strftime('%H:%M:%S')}]")
                    print("  " + ",".join(headers))
                    for row in rows:
                        print("  " + ",".join(str(row[h]) for h in headers))
            except Exception as e:
                print(f"\n[SHOW INSTANCES ERROR] {e}")
            self._stop_event.wait(self._interval)

    def _verify_up_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                rows = execute_and_fetch(self._coordinator, "SHOW INSTANCES;", protocol=Protocol.BOLT_ROUTING)
                for row in rows:
                    name = row.get("name", "unknown")
                    alive = row.get("alive", row.get("is_alive", None))
                    if alive is False:
                        print(f"\n  FATAL: Instance '{name}' is DOWN!")
                        os._exit(1)
            except Exception as e:
                print(f"\n[HEALTH CHECK ERROR] {e}")
            self._stop_event.wait(self._interval)

    def _storage_info_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                rows = execute_and_fetch(
                    self._coordinator,
                    "SHOW STORAGE INFO;",
                    protocol=Protocol.BOLT_ROUTING,
                    query_type=QueryType.READ,
                )
                info = {row["storage info"]: row["value"] for row in rows if "storage info" in row}
                parts = [f"{f}={info.get(f, '?')}" for f in self._storage_fields]
                print(f"\n[STORAGE INFO @ {time.strftime('%H:%M:%S')}] {' '.join(parts)}")
            except Exception as e:
                print(f"\n[STORAGE INFO ERROR] {e}")
            self._stop_event.wait(self._interval)

    # -- One-shot verification (call after workload) --------------------------

    def show_replicas(self) -> None:
        """Print SHOW REPLICAS once."""
        rows = execute_and_fetch(self._coordinator, "SHOW REPLICAS;", protocol=Protocol.BOLT_ROUTING)
        if rows:
            headers = list(rows[0].keys())
            print(",".join(headers))
            for row in rows:
                print(",".join(str(row[h]) for h in headers))

    def verify_all_ready(self) -> bool:
        """Check that all replicas have status 'ready'. Returns True if healthy."""
        rows = execute_and_fetch(self._coordinator, "SHOW REPLICAS;", protocol=Protocol.BOLT_ROUTING)
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
        rows = execute_and_fetch(self._coordinator, "SHOW INSTANCES;", protocol=Protocol.BOLT_ROUTING)
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
