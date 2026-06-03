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

std::string_view GcProgress::PhaseToString(GcPhase phase) {
  using namespace std::string_view_literals;
  switch (phase) {
    using enum GcPhase;
    case IDLE:
      return "idle"sv;
    case UNLINK:
      return "unlink"sv;
    case INDEX_CLEANUP:
      return "index_cleanup"sv;
    case DELETE:
      return "delete"sv;
  }
  return "unknown"sv;
}

}  // namespace memgraph::storage
