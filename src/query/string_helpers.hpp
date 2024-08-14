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

#include "storage/v2/point.hpp"

namespace memgraph::query {
inline auto CypherConstructionFor(storage::Point2d const &value) -> std::string {
  return fmt::format("POINT({{ x:{}, y:{}, srid: {} }})", value.x(), value.y(), CrsToSrid(value.crs()).value_of());
}

inline auto CypherConstructionFor(storage::Point3d const &value) -> std::string {
  return fmt::format("POINT({{ x:{}, y:{}, z:{}, srid: {} }})", value.x(), value.y(), value.z(),
                     CrsToSrid(value.crs()).value_of());
}
}  // namespace memgraph::query
