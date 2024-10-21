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

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point.hpp>
#include <boost/geometry/index/rtree.hpp>

namespace bg = boost::geometry;
namespace bgi = boost::geometry::index;

namespace memgraph::storage {

struct Vertex;

struct IndexPointWGS2d {
  explicit IndexPointWGS2d(Point2d point) : rep{point.x(), point.y()} { DMG_ASSERT(IsWGS(point.crs())); }
  using point_type = bg::model::point<double, 2, bg::cs::geographic<bg::degree>>;
  point_type rep;
};
struct IndexPointWGS3d {
  explicit IndexPointWGS3d(Point3d point) : rep{point.x(), point.y(), point.z()} { DMG_ASSERT(IsWGS(point.crs())); }
  using point_type = bg::model::point<double, 3, bg::cs::geographic<bg::degree>>;
  point_type rep;
};
struct IndexPointCartesian2d {
  explicit IndexPointCartesian2d(Point2d point) : rep{point.x(), point.y()} { DMG_ASSERT(IsCartesian(point.crs())); }
  using point_type = bg::model::point<double, 2, bg::cs::cartesian>;
  point_type rep;
};
struct IndexPointCartesian3d {
  explicit IndexPointCartesian3d(Point3d point) : rep{point.x(), point.y(), point.z()} {
    DMG_ASSERT(IsCartesian(point.crs()));
  }
  using point_type = bg::model::point<double, 3, bg::cs::cartesian>;
  point_type rep;
};

template <typename Point>
struct Entry {
  using point_type = typename Point::point_type;
  Entry(Point p, Vertex const *vertex) : p_(p), vertex_(vertex) {}

  friend bool operator==(Entry const &lhs, Entry const &rhs) {
    if (lhs.vertex_ != rhs.vertex_) return false;
    if (!boost::geometry::equals(lhs.p_, rhs.p_)) return false;
    return true;
  };

  auto point() const -> point_type const & { return p_.rep; }
  auto vertex() const -> storage::Vertex const * { return vertex_; }

 private:
  Point p_;
  storage::Vertex const *vertex_;
};
};  // namespace memgraph::storage

template <typename IndexPoint>
struct bg::index::indexable<memgraph::storage::Entry<IndexPoint>> {
  using result_type = typename IndexPoint::point_type;
  auto operator()(memgraph::storage::Entry<IndexPoint> const &val) const -> result_type const & { return val.point(); }
};

namespace memgraph::storage {
template <typename IndexPoint>
using index_t = bgi::rtree<Entry<IndexPoint>, bgi::quadratic<64>>;  // TODO: tune this

}
