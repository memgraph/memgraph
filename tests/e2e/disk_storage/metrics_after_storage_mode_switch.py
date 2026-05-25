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

import sys

import pytest
from common import connect, execute_and_fetch_all

import requests

METRICS_URL = "http://localhost:9091/metrics"


def test_metrics_endpoint_alive_after_storage_mode_switch(connect):
    cursor = connect.cursor()
    execute_and_fetch_all(cursor, "STORAGE MODE ON_DISK_TRANSACTIONAL")

    response = requests.get(METRICS_URL, timeout=5)
    assert response.status_code == 200


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
