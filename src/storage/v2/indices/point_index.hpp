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
#include "storage/v2/indices/point_index_change_collector.hpp"
#include "storage/v2/point.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/view.hpp"
#include "utils/skip_list.hpp"
#include "utils/synchronized.hpp"

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point.hpp>
#include <boost/geometry/index/rtree.hpp>

#include <ranges>

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

struct PointIndex {
  PointIndex() = default;

  PointIndex(std::span<Entry<IndexPointWGS2d>> points2dWGS, std::span<Entry<IndexPointCartesian2d>> points2dCartesian,
             std::span<Entry<IndexPointWGS3d>> points3dWGS, std::span<Entry<IndexPointCartesian3d>> points3dCartesian)
      : wgs_2d_index_{std::make_shared<index_t<IndexPointWGS2d>>(points2dWGS.begin(), points2dWGS.end())},
        wgs_3d_index_{std::make_shared<index_t<IndexPointWGS3d>>(points3dWGS.begin(), points3dWGS.end())},
        cartesian_2d_index_{
            std::make_shared<index_t<IndexPointCartesian2d>>(points2dCartesian.begin(), points2dCartesian.end())},
        cartesian_3d_index_{
            std::make_shared<index_t<IndexPointCartesian3d>>(points3dCartesian.begin(), points3dCartesian.end())} {}

  auto CreateNewPointIndex(LabelPropKey labelPropKey, std::unordered_set<Vertex const *> const &changed_vertices) const
      -> PointIndex;

 private:
  PointIndex(std::shared_ptr<index_t<IndexPointWGS2d>> points2dWGS,
             std::shared_ptr<index_t<IndexPointWGS3d>> points3dWGS,
             std::shared_ptr<index_t<IndexPointCartesian2d>> points2dCartesian,
             std::shared_ptr<index_t<IndexPointCartesian3d>> points3dCartesian)
      : wgs_2d_index_{std::move(points2dWGS)},
        wgs_3d_index_{std::move(points3dWGS)},
        cartesian_2d_index_{std::move(points2dCartesian)},
        cartesian_3d_index_{std::move(points3dCartesian)} {}

  std::shared_ptr<index_t<IndexPointWGS2d>> wgs_2d_index_ = std::make_shared<index_t<IndexPointWGS2d>>();
  std::shared_ptr<index_t<IndexPointWGS3d>> wgs_3d_index_ = std::make_shared<index_t<IndexPointWGS3d>>();
  std::shared_ptr<index_t<IndexPointCartesian2d>> cartesian_2d_index_ =
      std::make_shared<index_t<IndexPointCartesian2d>>();
  std::shared_ptr<index_t<IndexPointCartesian3d>> cartesian_3d_index_ =
      std::make_shared<index_t<IndexPointCartesian3d>>();
};

using index_container_t = std::map<LabelPropKey, std::shared_ptr<PointIndex const>>;

struct PointIndexContext {
  auto IndexKeys() const { return *orig_indexes_ | std::views::keys; }

  bool UsingLocalIndex() const { return orig_indexes_ != current_indexes_; }

  void AdvanceCommand(PointIndexChangeCollector &collector) { update_current(collector); }

 private:
  // Only PointIndexStorage can make these
  friend struct PointIndexStorage;
  explicit PointIndexContext(std::shared_ptr<index_container_t> indexes_)
      : orig_indexes_{std::move(indexes_)}, current_indexes_{orig_indexes_} {}

  void update_current(PointIndexChangeCollector &collector);

  void rebuild_current(std::shared_ptr<index_container_t> latest_index, PointIndexChangeCollector &collector);

  std::shared_ptr<index_container_t> orig_indexes_;
  std::shared_ptr<index_container_t> current_indexes_;
};

struct PointIndexStorage {
  // TODO: consider passkey idiom

  // Query (modify index set)
  bool CreatePointIndex(LabelId label, PropertyId property, utils::SkipList<Vertex>::Accessor vertices);
  bool DropPointIndex(LabelId label, PropertyId property);

  // Transaction (estabilish what to collect + able to build next index)
  auto CreatePointIndexContext() const -> PointIndexContext { return PointIndexContext{indexes_}; }

  // Commit
  void InstallNewPointIndex(PointIndexChangeCollector &collector, PointIndexContext &context);

 private:
  std::shared_ptr<index_container_t> indexes_ = std::make_shared<index_container_t>();
};

}  // namespace memgraph::storage
