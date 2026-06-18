#!/usr/bin/env python3
"""Build the VictoriaLogs/Loki push payload for a Memgraph CI core-dump ping.

All fields are read from the environment (so labels/message are escaped safely
by json.dumps rather than by the shell). The JSON payload is printed to stdout.
Invoked by ping_monitoring.sh.
"""
import json
import os


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
    return {
        "streams": [
            {
                "stream": stream,
                "values": [[os.environ["TS_NS"], os.environ["MSG"]]],
            }
        ]
    }


if __name__ == "__main__":
    print(json.dumps(build_payload()))
