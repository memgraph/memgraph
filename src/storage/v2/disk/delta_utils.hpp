// Copyright 2026 Memgraph Ltd.
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

#include <cstdint>
#include <optional>
#include <string_view>

namespace memgraph::storage {
struct Delta;
}  // namespace memgraph::storage

namespace memgraph::storage::disk {

// Walk a delta chain to find the oldest delta and extract the on-disk key
// if the oldest action is DELETE_DESERIALIZED_OBJECT; otherwise nullopt.
auto GetOldDiskKeyOrNull(Delta *head) -> std::optional<std::string_view>;

// Walk a delta chain to find the oldest delta and return its commit timestamp
// (0 for a nullptr head).
auto GetEarliestTimestamp(Delta *head) -> uint64_t;

}  // namespace memgraph::storage::disk
