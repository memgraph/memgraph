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

const char *SnapshotProgress::PhaseToString(Phase phase) {
  switch (phase) {
    using enum Phase;
    case IDLE:
      return "idle";
    case EDGES:
      return "edges";
    case VERTICES:
      return "vertices";
    case INDICES:
      return "indices";
    case CONSTRAINTS:
      return "constraints";
    case FINALIZING:
      return "finalizing";
  }
  return "unknown";
}

}  // namespace memgraph::storage
