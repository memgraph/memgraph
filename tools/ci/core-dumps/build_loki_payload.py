#!/usr/bin/env python3
"""Build the VictoriaLogs/Loki push payload for a Memgraph CI core-dump ping.

All fields are read from the environment (so labels/message are escaped safely
by json.dumps rather than by the shell). The JSON payload is printed to stdout.
Invoked by ping_monitoring.sh.
"""
import json
import os
import sys


def build_payload():
    stream = {
        "app": "memgraph",
        "job": "memgraph",
        "role": "ci",
        "namespace": "ci",
        "level": "fatal",
        "cluster_id": os.environ.get("CLUSTER_ID", ""),
        "service_name": os.environ.get("SERVICE_NAME", "memgraph"),
        "cluster_env": os.environ.get("CLUSTER_ENV", "ci"),
        "stack_trace_url": os.environ.get("STACK_TRACE_URL", ""),
    }
    # Only attach crash-signal labels when we actually know the signal.
    if os.environ.get("SIGNAL"):
        stream["signal"] = os.environ["SIGNAL"]
        stream["signal_name"] = os.environ.get("SIGNAL_NAME", "")
        stream["exit_status"] = os.environ.get("EXIT_STATUS", "")
    # Core dump / build-artifacts URLs are present only when --upload-core ran.
    if os.environ.get("CORE_URL"):
        stream["core_url"] = os.environ["CORE_URL"]
    if os.environ.get("BINARIES_URL"):
        stream["binaries_url"] = os.environ["BINARIES_URL"]
    # TS_NS and MSG are required (the log entry is meaningless without them).
    # Fail with a clear message rather than an opaque KeyError if run standalone.
    ts_ns = os.environ.get("TS_NS")
    msg = os.environ.get("MSG")
    if not ts_ns or msg is None:
        sys.exit("build_loki_payload.py: TS_NS and MSG environment variables are required")
    return {
        "streams": [
            {
                "stream": stream,
                "values": [[ts_ns, msg]],
            }
        ]
    }


if __name__ == "__main__":
    print(json.dumps(build_payload()))
