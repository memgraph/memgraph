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
      -> PointIndex {
    DMG_ASSERT(!changed_vertices.empty(), "Expect at least one change");

    // These are at this stage uncommitted changes + must have come from this txn, no need to check MVCC history
    auto changed_wgs_2d = std::unordered_map<Vertex const *, IndexPointWGS2d>{};
    auto changed_wgs_3d = std::unordered_map<Vertex const *, IndexPointWGS3d>{};
    auto changed_cartesian_2d = std::unordered_map<Vertex const *, IndexPointCartesian2d>{};
    auto changed_cartesian_3d = std::unordered_map<Vertex const *, IndexPointCartesian3d>{};

    // Single pass over all changes to cache current values
    for (auto v : changed_vertices) {
      auto guard = std::shared_lock{v->lock};
      auto isDeleted = [](Vertex const *v) { return v->deleted; };
      auto isWithoutLabel = [label = labelPropKey.label()](Vertex const *v) {
        return !utils::Contains(v->labels, label);
      };
      if (isDeleted(v) || isWithoutLabel(v)) {
        continue;
      }
      constexpr auto all_point_types = std::array{PropertyStoreType::POINT_2D, PropertyStoreType::POINT_3D};
      auto prop = v->properties.GetPropertyOfTypes(labelPropKey.property(), all_point_types);
      if (!prop) continue;
      if (prop->IsPoint2d()) {
        auto const &val = prop->ValuePoint2d();
        if (IsWGS(val.crs())) {
          changed_wgs_2d.emplace(v, val);
        } else {
          changed_cartesian_2d.emplace(v, val);
        }

      } else {
        auto const &val = prop->ValuePoint3d();
        if (IsWGS(val.crs())) {
          changed_wgs_3d.emplace(v, val);
        } else {
          changed_cartesian_3d.emplace(v, val);
        }
      }
    }

    auto helper = [&]<typename PointType>(std::shared_ptr<index_t<PointType>> const &index,
                                          std::unordered_map<Vertex const *, PointType> const &changed_values

                                          ) -> std::shared_ptr<index_t<PointType>> {
      auto modified = [&](Entry<PointType> const &entry) { return changed_vertices.contains(entry.vertex()); };
      if (changed_values.empty() && !std::ranges::any_of(*index, modified)) {
        // was unmodified, no need to rebuild
        return index;
      }

      auto copy_old = *index | std::views::filter(std::not_fn(modified));
      auto as_entry = [](std::pair<Vertex const *, PointType> const &p) { return Entry<PointType>{p.second, p.first}; };
      auto insert_modified = changed_values | std::views::transform(as_entry);

      auto new_index = std::make_shared<index_t<PointType>>(copy_old.begin(), copy_old.end());
      new_index->insert(insert_modified.begin(), insert_modified.end());
      return new_index;
    };

    return PointIndex{std::invoke(helper, wgs_2d_index_, changed_wgs_2d),
                      std::invoke(helper, wgs_3d_index_, changed_wgs_3d),
                      std::invoke(helper, cartesian_2d_index_, changed_cartesian_2d),
                      std::invoke(helper, cartesian_3d_index_, changed_cartesian_3d)};
  }

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
  auto IndexKeys() const { return *point_indexes_ | std::views::keys; }

  static auto update_internal(index_container_t const &src,
                              std::unordered_map<LabelPropKey, std::unordered_set<Vertex const *>> &tracked_changes)
      -> std::optional<index_container_t> {
    // All previous txns will use older index, this new built index will not concurrently be seen by older txns

    // TODO: not true, need to check that all tracked changes have nothing changed
    if (tracked_changes.empty()) return std::nullopt;

    auto new_point_index = index_container_t{};

    for (auto const &key : src | std::views::keys) {
      auto tracked_changes_it = tracked_changes.find(key);

      auto const &key_src_index = std::invoke([&]() -> PointIndex const & {
        auto it = src.find(key);
        if (it != src.end()) return *it->second;
        static auto const empty_index = PointIndex{};
        return empty_index;
      });

      DMG_ASSERT(tracked_changes_it != tracked_changes.end(),
                 "We expect the set of label+properties we are tracking to not have changed");

      auto changed_vertices = tracked_changes_it->second;
      if (changed_vertices.empty()) {
        // unmodified, no changes, just copy as is
        auto it = src.find(key);
        assert(it != src.end());
        new_point_index.emplace(key, it->second);
      } else /* if (has_changes)*/ {
        new_point_index.emplace(key,
                                std::make_shared<PointIndex>(key_src_index.CreateNewPointIndex(key, changed_vertices)));
      }
    }

    return new_point_index;
  }

 private:
  // Only PointIndexStorage can make these
  friend struct PointIndexStorage;
  explicit PointIndexContext(std::shared_ptr<index_container_t> indexes_) : point_indexes_{std::move(indexes_)} {}

  std::shared_ptr<index_container_t> point_indexes_;
};

struct PointIndexStorage {
  // TODO: consider passkey idiom

  // Query (modify index set)
  bool CreatePointIndex(LabelId label, PropertyId property, utils::SkipList<Vertex>::Accessor vertices);
  bool DropPointIndex(LabelId label, PropertyId property);

  // Transaction (estabilish what to collect + able to build next index)
  auto CreatePointIndexContext() const -> PointIndexContext { return PointIndexContext{indexes_}; }

 private:
  std::shared_ptr<index_container_t> indexes_ = std::make_shared<index_container_t>();
};

}  // namespace memgraph::storage
