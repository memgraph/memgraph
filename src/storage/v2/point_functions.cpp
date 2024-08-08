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

#include "storage/v2/point_functions.hpp"

namespace memgraph::storage {

namespace {

constexpr double MEAN_EARTH_RADIUS = 6'371'009;
constexpr auto PI_RADIANS = std::numbers::pi_v<double> / 180.0;

/// Input in radians
constexpr double GeneralHaversine(double phi_1, double phi_2, double lambda_2, double lambda_1, double r) {
  auto delta_phi = phi_2 - phi_1;
  auto delta_lambda = lambda_2 - lambda_1;

  auto sin_delta_phi = sin(delta_phi / 2.0);
  auto sin_delta_lambda = sin(delta_lambda / 2.0);

  auto a = sin_delta_phi * sin_delta_phi + cos(phi_1) * cos(phi_2) * (sin_delta_lambda * sin_delta_lambda);
  auto c = 2 * atan2(sqrt(a), sqrt(1 - a));

  return r * c;
}

}  // namespace

double Haversine(Point2d const &point1, Point2d const &point2) {
  auto phi_1 = point1.latitude() * PI_RADIANS;
  auto phi_2 = point2.latitude() * PI_RADIANS;
  auto lambda_2 = point2.longitude() * PI_RADIANS;
  auto lambda_1 = point1.longitude() * PI_RADIANS;

  return GeneralHaversine(phi_1, phi_2, lambda_2, lambda_1, MEAN_EARTH_RADIUS);
}

double Haversine(Point3d const &point1, Point3d const &point2) {
  auto phi_1 = point1.latitude() * PI_RADIANS;
  auto phi_2 = point2.latitude() * PI_RADIANS;
  auto lambda_2 = point2.longitude() * PI_RADIANS;
  auto lambda_1 = point1.longitude() * PI_RADIANS;

  auto horizontal_distance = GeneralHaversine(phi_1, phi_2, lambda_2, lambda_1, MEAN_EARTH_RADIUS);
  auto delta_height = point2.z() - point1.z();

  return std::sqrt(horizontal_distance * horizontal_distance + delta_height * delta_height);
}

double Distance(Point2d const &point1, Point2d const &point2) {
  MG_ASSERT(point1.crs() == point2.crs());
  if (point1.crs() == CoordinateReferenceSystem::Cartesian_2d) {
    auto dx = point1.x() - point2.x();
    auto dy = point1.y() - point2.y();
    return std::sqrt(dx * dx + dy * dy);
  }
  return Haversine(point1, point2);
}

double Distance(Point3d const &point1, Point3d const &point2) {
  MG_ASSERT(point1.crs() == point2.crs());
  if (point1.crs() == CoordinateReferenceSystem::Cartesian_3d) {
    auto dx = point1.x() - point2.x();
    auto dy = point1.y() - point2.y();
    auto dz = point1.z() - point2.z();
    return std::sqrt(dx * dx + dy * dy + dz * dz);
  }
  return Haversine(point1, point2);
}

bool WithinBBox(Point2d const &point, Point2d const &lower_bound, Point2d const &upper_bound) {
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
    return (lb_x <= p_x || p_x <= ub_x) && lb_y <= p_y && p_y <= ub_y;
  }
}

bool WithinBBox(Point3d const &point, Point3d const &lower_bound, Point3d const &upper_bound) {
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
    return (lb_x <= p_x || p_x <= ub_x) && lb_y <= p_y && p_y <= ub_y && lb_z <= p_z && p_z <= ub_z;
  }
}
}  // namespace memgraph::storage
