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

#include "storage/v2/indices/point_index_expensive_header.hpp"

namespace memgraph::storage {

double Haversine(Point2d const &point1, Point2d const &point2) {
  using pt = IndexPointWGS2d::point_type;
  auto boost_p1 = pt{point1.x(), point1.y()};
  auto boost_p2 = pt{point2.x(), point2.y()};
  return bg::distance(boost_p1, boost_p2);
}

double Haversine(Point3d const &point1, Point3d const &point2) {
  using pt = IndexPointWGS3d::point_type;

  // SPECIAL CASE: WGS-84 3D

  // We could reply on boost implementation that ignores the height for distance, but for possible future
  // boost changes, going to hand code operations to the 2d equivilant.

  // distance using average height of the two points
  auto h1 = point1.z();
  auto h2 = point2.z();
  auto middle = std::midpoint(h1, h2);
  auto boost_p1 = pt{point1.x(), point1.y(), middle};
  auto boost_p2 = pt{point2.x(), point2.y(), middle};
  auto distance_spherical = bg::distance(boost_p1, boost_p2);

  // use Pythagoras' theorem, combining height difference
  auto height_diff = h1 - h2;
  return std::sqrt(height_diff * height_diff + distance_spherical * distance_spherical);
}

double Distance(Point2d const &point1, Point2d const &point2) {
  MG_ASSERT(point1.crs() == point2.crs());
  if (point1.crs() == CoordinateReferenceSystem::Cartesian_2d) {
    using pt = IndexPointCartesian2d::point_type;
    auto boost_p1 = pt{point1.x(), point1.y()};
    auto boost_p2 = pt{point2.x(), point2.y()};
    return bg::distance(boost_p1, boost_p2);
  }
  return Haversine(point1, point2);
}

double Distance(Point3d const &point1, Point3d const &point2) {
  MG_ASSERT(point1.crs() == point2.crs());
  if (point1.crs() == CoordinateReferenceSystem::Cartesian_3d) {
    using pt = IndexPointCartesian3d::point_type;
    auto boost_p1 = pt{point1.x(), point1.y(), point1.z()};
    auto boost_p2 = pt{point2.x(), point2.y(), point2.z()};
    return bg::distance(boost_p1, boost_p2);
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
