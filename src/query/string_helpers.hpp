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

#include "query/cypher_numeric_literal.hpp"
#include "storage/v2/point.hpp"

namespace memgraph::query {
[[nodiscard]] inline auto CypherConstructionFor(storage::Point2d const &value) -> std::string {
  return fmt::format("POINT({{ x:{}, y:{}, srid: {} }})",
                     CypherCoordinateLiteral(value.x()),
                     CypherCoordinateLiteral(value.y()),
                     CrsToSrid(value.crs()).value_of());
}

[[nodiscard]] inline auto CypherConstructionFor(storage::Point3d const &value) -> std::string {
  return fmt::format("POINT({{ x:{}, y:{}, z:{}, srid: {} }})",
                     CypherCoordinateLiteral(value.x()),
                     CypherCoordinateLiteral(value.y()),
                     CypherCoordinateLiteral(value.z()),
                     CrsToSrid(value.crs()).value_of());
}
}  // namespace memgraph::query
