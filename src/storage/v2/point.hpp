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

#include "utils/logging.hpp"

#include <cmath>
#include <compare>
#include <cstdint>
#include <string_view>
#include <utility>

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

constexpr auto kSrid_WGS85_2D = 4326;
constexpr auto kSrid_WGS85_3D = 4979;
constexpr auto kSrid_Cartesian_2D = 7203;
constexpr auto kSrid_Cartesian_3D = 9757;

inline auto CrsToSrid(CoordinateReferenceSystem val) -> Srid {
  switch (val) {
    using enum CoordinateReferenceSystem;
    case WGS84_2d:
      return Srid{kSrid_WGS85_2D};
    case WGS84_3d:
      return Srid{kSrid_WGS85_3D};
    case Cartesian_2d:
      return Srid{kSrid_Cartesian_2D};
    case Cartesian_3d:
      return Srid{kSrid_Cartesian_3D};
  }
};

inline auto StringToCrs(std::string_view crs) -> std::optional<CoordinateReferenceSystem> {
  using enum CoordinateReferenceSystem;

  if (crs == "WGS-84") {
    return WGS84_2d;
  }
  if (crs == "WGS-84-3D") {
    return WGS84_3d;
  }
  if (crs == "CARTESIAN") {
    return Cartesian_2d;
  }
  if (crs == "CARTESIAN-3D") {
    return Cartesian_3d;
  }
  return std::nullopt;
}

// TODO Ivan: not consistent with neo4j, I think this is fine
inline auto CrsToString(CoordinateReferenceSystem crs) -> std::string {
  switch (crs) {
    using enum CoordinateReferenceSystem;
    case WGS84_2d:
      return "wgs-84";
    case WGS84_3d:
      return "wgs-84";
    case Cartesian_2d:
      return "cartesian";
    case Cartesian_3d:
      return "cartesian";
  }
}

inline auto SridToCrs(Srid val) -> std::optional<CoordinateReferenceSystem> {
  switch (val.value_of()) {
    using enum CoordinateReferenceSystem;
    case kSrid_WGS85_2D:
      return WGS84_2d;
    case kSrid_WGS85_3D:
      return WGS84_3d;
    case kSrid_Cartesian_2D:
      return Cartesian_2d;
    case kSrid_Cartesian_3D:
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

struct Point2d {
  Point2d() = default;  // needed for slk

  Point2d(CoordinateReferenceSystem crs, double x, double y) : crs_{crs}, x_{x}, y_{y} {
    DMG_ASSERT(valid2d(crs), "Not a valid 2d Coordinate Reference System");
  }

  auto crs() const -> CoordinateReferenceSystem { return crs_; }
  auto x() const -> double { return x_; }
  auto y() const -> double { return y_; }

  friend bool operator==(Point2d const &, Point2d const &) = default;

  friend auto operator<=>(Point2d const &lhs, Point2d const &rhs) -> std::partial_ordering {
    if (lhs.crs_ != rhs.crs_) return std::partial_ordering::unordered;

    const auto cmp_x = lhs.x_ <=> rhs.x_;
    const auto cmp_y = lhs.y_ <=> rhs.y_;

    return (cmp_x == cmp_y) ? cmp_x : std::partial_ordering::unordered;
  }

 private:
  CoordinateReferenceSystem crs_;
  double x_;
  double y_;
};

struct Point3d {
  Point3d() = default;  // needed for slk

  Point3d(CoordinateReferenceSystem crs, double x, double y, double z) : crs_{crs}, x_{x}, y_{y}, z_{z} {
    DMG_ASSERT(valid3d(crs), "Not a valid 3d Coordinate Reference System");
  }

  auto crs() const -> CoordinateReferenceSystem { return crs_; }
  auto x() const -> double { return x_; }
  auto y() const -> double { return y_; }
  auto z() const -> double { return z_; }

  friend bool operator==(Point3d const &A, Point3d const &B) = default;

  friend auto operator<=>(Point3d const &lhs, Point3d const &rhs) -> std::partial_ordering {
    if (lhs.crs_ != rhs.crs_) return std::partial_ordering::unordered;

    const auto cmp_x = lhs.x_ <=> rhs.x_;
    const auto cmp_y = lhs.y_ <=> rhs.y_;
    const auto cmp_z = lhs.z_ <=> rhs.z_;

    return (cmp_x == cmp_y && cmp_x == cmp_z) ? cmp_x : std::partial_ordering::unordered;
  }

 private:
  CoordinateReferenceSystem crs_;
  double x_;
  double y_;
  double z_;
};

inline double Haversine(const Point2d &point1, const Point2d &point2) {
  constexpr double R = 6371e3;
  const double PI_RADIANS = std::atan(1) * 4 / 180.00;

  auto phi_1 = point1.y() * PI_RADIANS;
  auto phi_2 = point2.y() * PI_RADIANS;
  auto delta_phi = (point2.y() - point1.y()) * PI_RADIANS;
  auto delta_lambda = (point2.x() - point1.x()) * PI_RADIANS;

  auto sin_delta_phi = std::sin(delta_phi / 2.0);
  auto sin_delta_lambda = std::sin(delta_lambda / 2.0);

  auto a = sin_delta_phi * sin_delta_phi + std::cos(phi_1) * std::cos(phi_2) * (sin_delta_lambda * sin_delta_lambda);
  auto c = 2 * std::atan2(std::sqrt(a), std::sqrt(1 - a));

  return R * c;
}

inline double Haversine(const Point3d &point1, const Point3d &point2) {
  constexpr double R = 6371e3;
  const double PI_RADIANS = std::atan(1) * 4 / 180.0;
  auto phi_1 = point1.y() * PI_RADIANS;
  auto phi_2 = point2.y() * PI_RADIANS;
  auto delta_phi = (point2.y() - point1.y()) * PI_RADIANS;
  auto delta_lambda = (point2.x() - point1.x()) * PI_RADIANS;

  auto sin_delta_phi = std::sin(delta_phi / 2.0);
  auto sin_delta_lambda = std::sin(delta_lambda / 2.0);

  auto a = sin_delta_phi * sin_delta_phi + std::cos(phi_1) * std::cos(phi_2) * (sin_delta_lambda * sin_delta_lambda);
  auto c = 2 * std::atan2(std::sqrt(a), std::sqrt(1 - a));

  auto horizontal_distance = R * c;
  auto delta_height = point2.z() - point1.z();

  return std::sqrt(horizontal_distance * horizontal_distance + delta_height * delta_height);
}

inline double Distance(const Point2d &point1, const Point2d &point2) {
  MG_ASSERT(point1.crs() == point2.crs());
  if (point1.crs() == CoordinateReferenceSystem::Cartesian_2d) {
    auto dx = point1.x() - point2.x();
    auto dy = point1.y() - point2.y();
    return std::sqrt(dx * dx + dy * dy);
  }
  return Haversine(point1, point2);
}

inline double Distance(const Point3d &point1, const Point3d &point2) {
  MG_ASSERT(point1.crs() == point2.crs());
  if (point1.crs() == CoordinateReferenceSystem::Cartesian_3d) {
    auto dx = point1.x() - point2.x();
    auto dy = point1.y() - point2.y();
    auto dz = point1.z() - point2.z();
    return std::sqrt(dx * dx + dy * dy + dz * dz);
  }
  return Haversine(point1, point2);
}

inline bool WithinBBox(const Point2d &point, const Point2d &lower_bound, const Point2d &upper_bound) {
  using enum CoordinateReferenceSystem;
  auto lb_x = lower_bound.x();
  auto ub_x = upper_bound.x();

  auto lb_y = lower_bound.y();
  auto ub_y = upper_bound.y();

  auto p_x = point.x();
  auto p_y = point.y();
  auto crs = point.crs();

  if (lb_x <= ub_x || crs == Cartesian_2d) [[likely]] {
    return lb_x <= p_x && p_x <= ub_x && lb_y <= p_y && p_y <= ub_y;
  } else {
    return ub_x <= p_x && p_x <= lb_x + 360.0 && lb_y <= p_y && p_y <= ub_y;
  }
}

inline bool WithinBBox(const Point3d &point, const Point3d &lower_bound, const Point3d &upper_bound) {
  using enum CoordinateReferenceSystem;
  auto lb_x = lower_bound.x();
  auto ub_x = upper_bound.x();

  auto lb_y = lower_bound.y();
  auto ub_y = upper_bound.y();

  auto lb_z = lower_bound.z();
  auto ub_z = upper_bound.z();

  auto p_x = point.x();
  auto p_y = point.y();
  auto p_z = point.z();
  auto crs = point.crs();

  if (lb_x <= ub_x || crs == Cartesian_3d) [[likely]] {
    return lb_x <= p_x && p_x <= ub_x && lb_y <= p_y && p_y <= ub_y && lb_z <= p_z && p_z <= ub_z;
  } else {
    return ub_x <= p_x && p_x <= lb_x + 360.0 && lb_y <= p_y && p_y <= ub_y && lb_z <= p_z && p_z <= ub_z;
  }
}

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
