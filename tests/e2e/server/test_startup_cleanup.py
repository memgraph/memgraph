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

import subprocess
import sys
import time
from types import SimpleNamespace

import memgraph
import pytest


@pytest.fixture
def slow_instance(tmp_path, monkeypatch):
    binary = tmp_path / "slow_server.py"
    binary.write_text(f"#!{sys.executable}\nimport time\ntime.sleep(600)\n")
    binary.chmod(0o755)
    data_directory = tmp_path / "data"
    data_directory.mkdir()
    marker = data_directory / "recovery_input"
    marker.write_text("keep")
    instance = memgraph.MemgraphInstanceRunner(str(binary), False, str(data_directory))
    cleanup = instance.kill
    monkeypatch.setattr(instance, "_print_diagnostics", lambda: None)
    monkeypatch.setattr(memgraph, "time", SimpleNamespace(sleep=lambda _: time.sleep(0.001)))
    yield instance, marker
    cleanup(keep_directories=True)


@pytest.mark.parametrize("failure", ["readiness", "setup"])
def test_failed_start_reaps_process(slow_instance, monkeypatch, failure):
    instance, marker = slow_instance
    monkeypatch.setattr(memgraph, "connectable_port", lambda _: failure == "setup")

    def fail_setup(_):
        raise RuntimeError("setup failed")

    monkeypatch.setattr(instance, "execute_setup_queries", fail_setup)
    expected_error = AssertionError if failure == "readiness" else RuntimeError
    expected_message = "failed to listen" if failure == "readiness" else "setup failed"
    with pytest.raises(expected_error, match=expected_message):
        instance.start(args=["--bolt-port", "7687"], setup_queries=["RETURN 1"])
    assert instance.proc_mg.poll() is not None, "Failed startup left a live process behind"
    assert marker.read_text() == "keep", "Failed startup deleted its recovery input"


def test_failed_start_preserves_error_when_reaping_times_out(slow_instance, monkeypatch):
    instance, marker = slow_instance
    monkeypatch.setattr(memgraph, "connectable_port", lambda _: False)

    def fail_cleanup(**kwargs):
        raise subprocess.TimeoutExpired("server", 15)

    monkeypatch.setattr(instance, "kill", fail_cleanup)
    with pytest.raises(AssertionError, match="failed to listen") as error:
        instance.start(args=["--bolt-port", "7687"])
    assert any("Failed to reap server process" in note for note in error.value.__notes__)
    assert marker.read_text() == "keep"


def test_kill_handles_concurrent_exit(slow_instance, monkeypatch):
    instance, _ = slow_instance
    instance.proc_mg = subprocess.Popen(
        [sys.executable, "-c", "import sys; sys.stdin.buffer.read()"], stdin=subprocess.PIPE
    )
    kill = instance.proc_mg.kill

    def exit_before_kill():
        instance.proc_mg.stdin.close()
        instance.proc_mg.wait(timeout=5)
        kill()

    monkeypatch.setattr(instance.proc_mg, "kill", exit_before_kill)
    instance.kill(keep_directories=True)
    assert instance.proc_mg.returncode == 0


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
