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

# The metrics endpoints are an enterprise feature: they serve nothing without a valid licence.

import os
import sys
import urllib.error
import urllib.request
from unittest.mock import patch

import interactive_mg_runner
import mgclient
import pytest

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

LICENSE_KEY = os.environ.get("MEMGRAPH_ENTERPRISE_LICENSE", "")
ORGANIZATION_NAME = os.environ.get("MEMGRAPH_ORGANIZATION_NAME", "")

pytestmark = pytest.mark.skipif(
    not (LICENSE_KEY and ORGANIZATION_NAME),
    reason="needs an enterprise license (MEMGRAPH_ENTERPRISE_LICENSE + MEMGRAPH_ORGANIZATION_NAME)",
)


@pytest.fixture(autouse=True)
def cleanup_after_test():
    yield
    interactive_mg_runner.kill_all(keep_directories=False)


def scrape():
    try:
        with urllib.request.urlopen("http://localhost:9091/metrics", timeout=5) as response:
            return response.status, response.read().decode()
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode()


@pytest.mark.parametrize("metrics_format", ["OpenMetrics", "JSON"])
def test_metrics_need_licence(request, metrics_format):
    description = {
        "main": {
            "args": [
                "--bolt-port=7687",
                "--log-level=TRACE",
                "--metrics-port=9091",
                f"--metrics-format={metrics_format}",
            ],
            "log_file": f"show_metrics/metrics_licence/{request.node.name}/main.log",
            "data_directory": f"show_metrics/metrics_licence/{request.node.name}/main",
        }
    }
    with patch.dict(os.environ):
        del os.environ["MEMGRAPH_ENTERPRISE_LICENSE"]
        del os.environ["MEMGRAPH_ORGANIZATION_NAME"]
        interactive_mg_runner.start(description, "main")

    status, body = scrape()
    assert status == 400
    assert "Memgraph must have an Enterprise License for providing metrics." in body

    connection = mgclient.connect(host="localhost", port=7687)
    connection.autocommit = True
    cursor = connection.cursor()
    cursor.execute(f"SET DATABASE SETTING 'organization.name' TO '{ORGANIZATION_NAME}';")
    cursor.execute(f"SET DATABASE SETTING 'enterprise.license' TO '{LICENSE_KEY}';")

    assert scrape()[0] == 200


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
