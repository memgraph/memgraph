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

#include "storage/v2/point.hpp"

namespace memgraph::query {
namespace detail {
// Historical display spelling: shortest round-tripping form, no extra precision.
struct DefaultCoordinateFormatter {
  auto operator()(double value) const -> std::string { return fmt::format("{}", value); }
};
}  // namespace detail

// The dumper injects its own coordinate spelling; every other caller keeps the byte-identical default form.
template <typename CoordinateFormatter>
[[nodiscard]] inline auto CypherConstructionFor(storage::Point2d const &value, CoordinateFormatter &&format_coordinate)
    -> std::string {
  return fmt::format("POINT({{ x:{}, y:{}, srid: {} }})",
                     format_coordinate(value.x()),
                     format_coordinate(value.y()),
                     CrsToSrid(value.crs()).value_of());
}

template <typename CoordinateFormatter>
[[nodiscard]] inline auto CypherConstructionFor(storage::Point3d const &value, CoordinateFormatter &&format_coordinate)
    -> std::string {
  return fmt::format("POINT({{ x:{}, y:{}, z:{}, srid: {} }})",
                     format_coordinate(value.x()),
                     format_coordinate(value.y()),
                     format_coordinate(value.z()),
                     CrsToSrid(value.crs()).value_of());
}

[[nodiscard]] inline auto CypherConstructionFor(storage::Point2d const &value) -> std::string {
  return CypherConstructionFor(value, detail::DefaultCoordinateFormatter{});
}

[[nodiscard]] inline auto CypherConstructionFor(storage::Point3d const &value) -> std::string {
  return CypherConstructionFor(value, detail::DefaultCoordinateFormatter{});
}
}  // namespace memgraph::query
