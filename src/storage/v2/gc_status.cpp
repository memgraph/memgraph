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

#include "storage/v2/gc_status.hpp"

namespace memgraph::storage {

const char *GcProgress::PhaseToString(GcPhase phase) {
  switch (phase) {
    using enum GcPhase;
    case IDLE:
      return "idle";
    case UNLINK:
      return "unlink";
    case INDEX_CLEANUP:
      return "index_cleanup";
    case DELETE:
      return "delete";
  }
  return "unknown";
}

}  // namespace memgraph::storage
