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
#include <string_view>

namespace memgraph::query {

/// What a procedure declares it does to the graph it is handed.
///
/// A procedure reaches the graph only through that argument, so a procedure that never touches it can
/// neither read nor write, and the three states are exhaustive. `None` is what lets a call run with no
/// storage transaction open. Declaring it falsely is caught rather than fatal: the graph handed over
/// has no accessor behind it, and reaching through it reports a logic error.
enum class GraphAccess : uint8_t { None, Read, Write };

constexpr std::string_view ToString(GraphAccess access) {
  switch (access) {
    case GraphAccess::None:
      return "graph-free";
    case GraphAccess::Read:
      return "read";
    case GraphAccess::Write:
      return "write";
  }
  return "unknown";
}

}  // namespace memgraph::query
