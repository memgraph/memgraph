# Copyright 2023 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

from abc import ABC

from benchmark_context import BenchmarkContext
from runners import BaseClient


class BaseImporter(ABC):
    def __init__(self, benchmark_context: BenchmarkContext, client: BaseClient) -> None:
        self._benchmark_context = benchmark_context
        self._client = client

    def execute_import(self) -> bool:
        return False
