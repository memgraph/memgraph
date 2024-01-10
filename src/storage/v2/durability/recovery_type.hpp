// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <utility>
#include <vector>
#include "storage/v2/id_types.hpp"

namespace memgraph::storage::durability {
struct ParallelizedSchemaCreationInfo {
  std::vector<std::pair<Gid, uint64_t>> vertex_recovery_info;
  uint64_t thread_count;
};
}  // namespace memgraph::storage::durability
