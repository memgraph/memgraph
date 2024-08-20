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
#include "storage/v2/id_types.hpp"
#include "storage/v2/point.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/skip_list.hpp"
#include "utils/synchronized.hpp"

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point.hpp>
#include <boost/geometry/index/rtree.hpp>

namespace bg = boost::geometry;
namespace bgi = boost::geometry::index;

namespace memgraph::storage {

struct IndexPointWGS2d {
  explicit IndexPointWGS2d(Point2d point) : rep{point.x(), point.y()} { DMG_ASSERT(IsWGS(point.crs())); }
  using point_type = bg::model::point<double, 2, bg::cs::spherical_equatorial<bg::degree>>;
  point_type rep;
};
struct IndexPointWGS3d {
  explicit IndexPointWGS3d(Point3d point) : rep{point.x(), point.y(), point.z()} { DMG_ASSERT(IsWGS(point.crs())); }
  using point_type = bg::model::point<double, 3, bg::cs::spherical_equatorial<bg::degree>>;
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
  Entry(Point p, Vertex const *vertex) : p(p), vertex(vertex) {}

  friend bool operator==(Entry const &lhs, Entry const &rhs) {
    if (lhs.vertex != rhs.vertex) return false;
    if (!boost::geometry::equals(lhs.p, rhs.p)) return false;
    return true;
  };

  auto point() const -> point_type const & { return p.rep; }

 private:
  Point p;
  storage::Vertex const *vertex;
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

struct PointIndex {
  PointIndex(std::span<Entry<IndexPointWGS2d>> points2dWGS, std::span<Entry<IndexPointCartesian2d>> points2dCartesian,
             std::span<Entry<IndexPointWGS3d>> points3dWGS, std::span<Entry<IndexPointCartesian3d>> points3dCartesian)
      : wgs_2d_index_{points2dWGS.begin(), points2dWGS.end()},
        wgs_3d_index_{points3dWGS.begin(), points3dWGS.end()},
        cartesian_2d_index_{points2dCartesian.begin(), points2dCartesian.end()},
        cartesian_3d_index_{points3dCartesian.begin(), points3dCartesian.end()} {}

 private:
  index_t<IndexPointWGS2d> wgs_2d_index_;
  index_t<IndexPointWGS3d> wgs_3d_index_;
  index_t<IndexPointCartesian2d> cartesian_2d_index_;
  index_t<IndexPointCartesian3d> cartesian_3d_index_;
};

struct PointIndexStorage {
  using index_container_t = std::map<LabelPropKey, std::shared_ptr<PointIndex const>>;

  bool CreatePointIndex(LabelId label, PropertyId property, utils::SkipList<Vertex>::Accessor vertices);

  bool DropPointIndex(LabelId label, PropertyId property);

 private:
  utils::Synchronized<index_container_t> indexes_;
};

}  // namespace memgraph::storage
