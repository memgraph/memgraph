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
#include <string>
#include <vector>

namespace memgraph::query {

// Describes one derive() projection in a query result, so a projection-aware client can style the
// overlay nodes it produced. The overlay nodes carry `ref` (a plain Int property); this entry,
// delivered once in the result header, tells the client which of those nodes' property keys are
// computed overlays and what edge type connects them. Built statically from the plan at prepare
// time, so it is available before the first record streams.
struct ProjectionSchema {
  // The schema reference shared by every overlay node from this derive(): the output symbol's plan
  // position, matching the value carried in the node's reserved tag property.
  int64_t ref;
  // Property keys bound to the overlay (computed, not read through from the origin).
  std::vector<std::string> overlay;
  // The virtual edge type connecting the projection's nodes.
  std::string edge_type;
};

}  // namespace memgraph::query
