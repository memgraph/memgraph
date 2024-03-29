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

#include <cstdint>
#include <optional>
#include <string_view>

namespace memgraph::storage {

enum class IsolationLevel : std::uint8_t { SNAPSHOT_ISOLATION, READ_COMMITTED, READ_UNCOMMITTED };

static inline std::string_view IsolationLevelToString(IsolationLevel isolation_level) {
  switch (isolation_level) {
    case IsolationLevel::READ_COMMITTED:
      return "READ_COMMITTED";
    case IsolationLevel::READ_UNCOMMITTED:
      return "READ_UNCOMMITTED";
    case IsolationLevel::SNAPSHOT_ISOLATION:
      return "SNAPSHOT_ISOLATION";
  }
}

static inline std::string_view IsolationLevelToString(std::optional<IsolationLevel> isolation_level) {
  if (isolation_level) {
    return IsolationLevelToString(*isolation_level);
  }
  return "";
}

}  // namespace memgraph::storage
