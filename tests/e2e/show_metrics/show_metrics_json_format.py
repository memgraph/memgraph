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

import json
import sys
import urllib.request

import pytest


def test_metrics_json_format_returns_valid_json():
    with urllib.request.urlopen("http://localhost:9091/metrics") as response:
        assert response.status == 200
        body = response.read()
        try:
            json.loads(body)
        except json.JSONDecodeError:
            pytest.fail("Response is not valid JSON")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
