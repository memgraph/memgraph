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

"""What the durability harness reports when a server does not come up.

Every case here recovers a recorded snapshot or write-ahead log and compares the
dump against a recorded one, so every case first waits for a server to open its
port. A machine slow enough to miss that wait has not lost data, and the report
has to say which of the two happened.

Run with: pytest tests/integration/durability/
"""

import logging
import os
import stat
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pytest
from memgraph_server_context import memgraph_server, wait_for_server

LOGGER = logging.getLogger(__name__)

# Alive, never opens a port, and deaf to the signal the harness shuts down with:
# a server still recovering behaves this way for as long as recovery takes.
STILL_STARTING = """\
import signal, time
signal.signal(signal.SIGINT, signal.SIG_IGN)
time.sleep(300)
"""


def a_server_that(script: str, directory: Path) -> str:
    """An executable standing in for memgraph, doing only what the case needs.

    Written as a file rather than passed to `python -c`, because the harness
    puts the server's own flags between the binary and anything a caller adds.
    """
    path = directory / "stands_in_for_memgraph"
    path.write_text(f"#!{sys.executable}\nimport sys\n{script}")
    path.chmod(path.stat().st_mode | stat.S_IEXEC)
    return str(path)


def test_a_server_that_exits_is_reported_before_the_deadline():
    """A server that dies is known at once, whatever the deadline is.

    This is what lets the deadline be generous: it is reached only by a server
    that is alive and still starting, never by one that has already failed.
    """
    proc = subprocess.Popen([sys.executable, "-c", "raise SystemExit(1)"], stdout=subprocess.PIPE)
    started = time.time()

    with pytest.raises(RuntimeError, match="exited"):
        wait_for_server(proc, port=1, timeout=30.0)

    assert time.time() - started < 10.0, "a dead server waited out the deadline"


def test_a_server_that_never_starts_is_not_reported_as_a_durability_fault():
    """A start that never finished leaves no dump to judge, so the report says so.

    Shutting the server down raises refusals of its own, and one of them reads
    as though the recovered data were wrong. Reaching that from a failed start
    names the wrong fault, and sends whoever reads it looking for a durability
    bug that is not there.
    """
    with tempfile.TemporaryDirectory() as directory:
        binary = a_server_that(STILL_STARTING, Path(directory))
        data_dir = Path(directory) / "data"
        data_dir.mkdir()

        with pytest.raises(BaseException) as refusal:
            with memgraph_server(
                memgraph=binary,
                data_dir=data_dir,
                port=1,
                logger=LOGGER,
                timeout=1,
                start_timeout=1.0,
            ):
                pytest.fail("the body ran even though the server never started")

    assert "durability maybe incorrect" not in str(
        refusal.value
    ), f"a failed start was reported as a durability fault: {refusal.value}"
    assert "did not start" in str(refusal.value), f"the report does not name the failed start: {refusal.value}"
