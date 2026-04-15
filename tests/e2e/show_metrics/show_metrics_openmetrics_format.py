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

import re
import sys
import urllib.request

import pytest

_VALID_LINE_PATTERNS = [
    re.compile(r"^$"),  # empty line
    re.compile(r"^# HELP \w+ .*$"),  # HELP comment
    re.compile(r"^# TYPE \w+ \w+$"),  # TYPE comment
    re.compile(r"^# EOF$"),  # OpenMetrics trailer
    re.compile(r"^[a-zA-Z_]\S*.* [-+]?[\d.]+(?:e[-+]?\d+)?$"),  # metric line
]


def test_metrics_openmetrics_format_returns_valid_openmetrics():
    # Doesn't validate in any depth, but just check that each line
    # matches what we'd expect from an OpenMetrics endpoint.
    req = urllib.request.Request(
        "http://localhost:9091/metrics",
        headers={"Accept": "application/openmetrics-text; version=1.0.0; charset=utf-8"},
    )
    with urllib.request.urlopen(req) as response:
        assert response.status == 200
        assert response.headers.get("Content-Type", "").startswith("application/openmetrics-text")
        body = response.read().decode("utf-8")
        assert len(body) > 0
        assert body.endswith("# EOF\n")
        for line in body.splitlines():
            assert any(p.match(line) for p in _VALID_LINE_PATTERNS), f"Unexpected line: {line}"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
