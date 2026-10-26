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

#include <map>
#include <tuple>
#include <utility>
#include <vector>

#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"

// Helpers for the property writes of a fork vertex or edge against the overlay store.
namespace memgraph::versioning {

using StubPropertyMap = std::map<storage::PropertyId, storage::PropertyValue>;

// (pid, old, new) per entry of `properties`; `existing` supplies the old values.
inline std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>> UpdateChanges(
    const StubPropertyMap &existing, const StubPropertyMap &properties) {
  std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>> changes;
  changes.reserve(properties.size());
  for (const auto &[pid, new_value] : properties) {
    auto it = existing.find(pid);
    changes.emplace_back(pid, it != existing.end() ? it->second : storage::PropertyValue{}, new_value);
  }
  return changes;
}

// Storage's UpdateProperties leaves the untouched old properties merged into the caller's map; mirror that.
inline void MergeOldBack(StubPropertyMap &existing, StubPropertyMap &properties) {
  for (auto &[pid, old_value] : existing) properties.emplace(pid, std::move(old_value));
}

}  // namespace memgraph::versioning
