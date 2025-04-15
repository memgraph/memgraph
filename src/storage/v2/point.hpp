// Copyright 2025 Memgraph Ltd.
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

#include "storage/v2/durability/version.hpp"
#include "utils/logging.hpp"
#include "utils/string.hpp"

#include <compare>
#include <cstdint>
#include <string_view>

#include "boost/functional/hash.hpp"
#include "strong_type/strong_type.hpp"

namespace memgraph::storage {

using Srid = strong::type<uint16_t, struct Srid_, strong::regular, strong::partially_ordered>;

// This type is also used for durability, please keep the values as are (or provide migration step)
enum class CoordinateReferenceSystem : uint8_t {
  WGS84_2d = 0,
  WGS84_3d = 1,
  Cartesian_2d = 2,
  Cartesian_3d = 3,
};

// srid's are spatial reference system identifiers
constexpr auto kSrid_WGS84_2D = 4326;  // GEOGRAPHIC_2D_CRS
constexpr auto kSrid_WGS84_3D = 4979;  // GEOGRAPHIC_3D_CRS
constexpr auto kSrid_Cartesian_2D = 7203;
constexpr auto kSrid_Cartesian_3D = 9157;

inline auto CrsToSrid(CoordinateReferenceSystem val) -> Srid {
  switch (val) {
    using enum CoordinateReferenceSystem;
    case WGS84_2d:
      return Srid{kSrid_WGS84_2D};
    case WGS84_3d:
      return Srid{kSrid_WGS84_3D};
    case Cartesian_2d:
      return Srid{kSrid_Cartesian_2D};
    case Cartesian_3d:
      return Srid{kSrid_Cartesian_3D};
  }
};

inline auto StringToCrs(std::string_view crs) -> std::optional<CoordinateReferenceSystem> {
  using enum CoordinateReferenceSystem;

  auto lookup_string = memgraph::utils::ToUpperCase(crs);

  if (lookup_string == "WGS-84") {
    return WGS84_2d;
  }
  if (lookup_string == "WGS-84-3D") {
    return WGS84_3d;
  }
  if (lookup_string == "CARTESIAN") {
    return Cartesian_2d;
  }
  if (lookup_string == "CARTESIAN-3D") {
    return Cartesian_3d;
  }
  return std::nullopt;
}

inline auto CrsToString(CoordinateReferenceSystem crs) -> std::string {
  switch (crs) {
    using enum CoordinateReferenceSystem;
    case WGS84_2d:
    case WGS84_3d:
      return "wgs-84";
    case Cartesian_2d:
    case Cartesian_3d:
      return "cartesian";
  }
}

inline auto SridToCrs(Srid val) -> std::optional<CoordinateReferenceSystem> {
  switch (val.value_of()) {
    using enum CoordinateReferenceSystem;
    case kSrid_WGS84_2D:
      return WGS84_2d;
    case kSrid_WGS84_3D:
      return WGS84_3d;
    case kSrid_Cartesian_2D:
      return Cartesian_2d;
    case kSrid_Cartesian_3D:
      return Cartesian_3d;
    case 9757:
      // This hack can be removed when we no longer support loading durability versions that old
      static_assert(durability::kOldestSupportedVersion < durability::kSridCartesian3DCorrected);
      return Cartesian_3d;
  }
  return std::nullopt;
}

inline bool valid2d(CoordinateReferenceSystem val) {
  using enum CoordinateReferenceSystem;
  return val == WGS84_2d || val == Cartesian_2d;
}

inline bool valid3d(CoordinateReferenceSystem val) {
  using enum CoordinateReferenceSystem;
  return val == WGS84_3d || val == Cartesian_3d;
}

inline bool IsWGS(CoordinateReferenceSystem val) {
  using enum CoordinateReferenceSystem;
  return val == WGS84_2d || val == WGS84_3d;
}

inline bool IsCartesian(CoordinateReferenceSystem val) {
  using enum CoordinateReferenceSystem;
  return val == Cartesian_2d || val == Cartesian_3d;
}

struct Point2d {
  Point2d() = default;  // needed for slk

  Point2d(CoordinateReferenceSystem crs, double x, double y) : crs_{crs}, x_longitude_{x}, y_latitude_{y} {
    DMG_ASSERT(valid2d(crs), "Not a valid 2d Coordinate Reference System");
  }

  [[nodiscard]] auto crs() const -> CoordinateReferenceSystem { return crs_; }
  [[nodiscard]] auto x() const -> double { return x_longitude_; }
  [[nodiscard]] auto y() const -> double { return y_latitude_; }
  [[nodiscard]] auto longitude() const -> double { return x_longitude_; }
  [[nodiscard]] auto latitude() const -> double { return y_latitude_; }

  friend bool operator==(Point2d const &, Point2d const &) = default;

  friend auto operator<=>(Point2d const &lhs, Point2d const &rhs) -> std::partial_ordering {
    return std::tie(lhs.crs_, lhs.x_longitude_, lhs.y_latitude_) <=>
           std::tie(rhs.crs_, rhs.x_longitude_, rhs.y_latitude_);
  }

 private:
  CoordinateReferenceSystem crs_{CoordinateReferenceSystem::Cartesian_2d};
  double x_longitude_;
  double y_latitude_;
};

struct Point3d {
  Point3d() = default;  // needed for slk

  Point3d(CoordinateReferenceSystem crs, double x, double y, double z)
      : crs_{crs}, x_longitude_{x}, y_latitude_{y}, z_height_{z} {
    DMG_ASSERT(valid3d(crs), "Not a valid 3d Coordinate Reference System");
  }

  [[nodiscard]] auto crs() const -> CoordinateReferenceSystem { return crs_; }
  [[nodiscard]] auto x() const -> double { return x_longitude_; }
  [[nodiscard]] auto y() const -> double { return y_latitude_; }
  [[nodiscard]] auto z() const -> double { return z_height_; }
  [[nodiscard]] auto longitude() const -> double { return x_longitude_; }
  [[nodiscard]] auto latitude() const -> double { return y_latitude_; }
  [[nodiscard]] auto height() const -> double { return z_height_; }

  friend bool operator==(Point3d const &A, Point3d const &B) = default;

  friend auto operator<=>(Point3d const &lhs, Point3d const &rhs) -> std::partial_ordering {
    return std::tie(lhs.crs_, lhs.x_longitude_, lhs.y_latitude_, lhs.z_height_) <=>
           std::tie(rhs.crs_, rhs.x_longitude_, rhs.y_latitude_, rhs.z_height_);
  }

 private:
  CoordinateReferenceSystem crs_{CoordinateReferenceSystem::Cartesian_3d};
  double x_longitude_;
  double y_latitude_;
  double z_height_;
};

static_assert(std::is_trivially_destructible_v<Point2d>);
static_assert(std::is_trivially_destructible_v<Point3d>);
}  // namespace memgraph::storage

namespace std {
template <>
struct hash<memgraph::storage::Point2d> {
  std::size_t operator()(const memgraph::storage::Point2d &point) const noexcept {
    size_t seed = 0;
    boost::hash_combine(seed, point.crs());
    boost::hash_combine(seed, point.x());
    boost::hash_combine(seed, point.y());
    return seed;
  }
};

template <>
struct hash<memgraph::storage::Point3d> {
  std::size_t operator()(const memgraph::storage::Point3d &point) const noexcept {
    size_t seed = 0;
    boost::hash_combine(seed, point.crs());
    boost::hash_combine(seed, point.x());
    boost::hash_combine(seed, point.y());
    boost::hash_combine(seed, point.z());
    return seed;
  }
};

}  // namespace std
