// Copyright 2025 Memgraph Ltd.
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

#include "storage/v2/id_types.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::storage {
struct Delta;
}

namespace memgraph::utils {

auto GetOldDiskKeyOrNull(storage::Delta *head) -> std::optional<std::string_view>;

template <typename TSkipListAccessor>
bool ObjectExistsInCache(TSkipListAccessor &accessor, storage::Gid gid) {
  return accessor.contains(gid);
}

auto GetEarliestTimestamp(storage::Delta *head) -> uint64_t;

}  // namespace memgraph::utils
