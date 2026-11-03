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

import os
import signal
import time

import interactive_mg_runner
import pytest

CORE_DUMP_WAIT_SEC = 60


def _abort_running_instances():
    """
    Aborts every instance still running so each one leaves a core dump with all thread stacks. A hung coordinator
    or data instance only shows up as a timed-out assertion; the core shows where its threads were blocked.
    """
    aborted = []
    for name, instance in interactive_mg_runner.MEMGRAPH_INSTANCES.items():
        if not instance.is_running():
            continue
        print(f"\nAborting still running instance {name} (pid {instance.proc_mg.pid}) to capture a core dump.")
        instance._print_diagnostics()
        try:
            os.kill(instance.proc_mg.pid, signal.SIGABRT)
            aborted.append(instance)
        except ProcessLookupError:
            pass

    deadline = time.monotonic() + CORE_DUMP_WAIT_SEC
    for instance in aborted:
        try:
            instance.proc_mg.wait(timeout=max(0.0, deadline - time.monotonic()))
        except Exception:
            print(f"Instance with pid {instance.proc_mg.pid} did not exit after SIGABRT.")


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    # Runs before fixture teardown, so the instances of the failed test are still alive.
    if report.when == "call" and report.failed:
        _abort_running_instances()
