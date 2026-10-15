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

#include "storage/v2/snapshot_progress.hpp"

namespace memgraph::storage {

std::string_view SnapshotProgress::PhaseToString(Phase phase) {
  using namespace std::string_view_literals;
  switch (phase) {
    using enum Phase;
    case IDLE:
      return "idle"sv;
    case EDGES:
      return "edges"sv;
    case VERTICES:
      return "vertices"sv;
    case INDICES:
      return "indices"sv;
    case CONSTRAINTS:
      return "constraints"sv;
    case FINALIZING:
      return "finalizing"sv;
  }
  return "unknown"sv;
}

}  // namespace memgraph::storage
