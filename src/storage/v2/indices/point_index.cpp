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

#include "storage/v2/indices/point_index.hpp"

#include <boost/geometry.hpp>
#include <boost/geometry/index/predicates.hpp>

#include <cmath>
#include <utility>
#include "storage/v2/indices/point_index_change_collector.hpp"
#include "storage/v2/indices/point_index_expensive_header.hpp"
#include "storage/v2/indices/point_iterator.hpp"
#include "storage/v2/point_functions.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/exceptions.hpp"
#include "utils/logging.hpp"

namespace memgraph::storage {

struct PointIndex {
  PointIndex() = default;

  PointIndex(std::span<Entry<IndexPointWGS2d>> points2dWGS, std::span<Entry<IndexPointCartesian2d>> points2dCartesian,
             std::span<Entry<IndexPointWGS3d>> points3dWGS, std::span<Entry<IndexPointCartesian3d>> points3dCartesian);

  auto CreateNewPointIndex(LabelPropKey labelPropKey, absl::flat_hash_set<Vertex const *> const &changed_vertices) const
      -> PointIndex;

  auto EntryCount() const -> std::size_t {
    return wgs_2d_index_->size() + wgs_3d_index_->size() + cartesian_2d_index_->size() + cartesian_3d_index_->size();
  }

  auto GetWgs2dIndex() const -> std::shared_ptr<index_t<IndexPointWGS2d>> { return wgs_2d_index_; }
  auto GetWgs3dIndex() const -> std::shared_ptr<index_t<IndexPointWGS3d>> { return wgs_3d_index_; }
  auto GetCartesian2dIndex() const -> std::shared_ptr<index_t<IndexPointCartesian2d>> { return cartesian_2d_index_; }
  auto GetCartesian3dIndex() const -> std::shared_ptr<index_t<IndexPointCartesian3d>> { return cartesian_3d_index_; }

 private:
  PointIndex(std::shared_ptr<index_t<IndexPointWGS2d>> points2dWGS,
             std::shared_ptr<index_t<IndexPointWGS3d>> points3dWGS,
             std::shared_ptr<index_t<IndexPointCartesian2d>> points2dCartesian,
             std::shared_ptr<index_t<IndexPointCartesian3d>> points3dCartesian);

  std::shared_ptr<index_t<IndexPointWGS2d>> wgs_2d_index_ = std::make_shared<index_t<IndexPointWGS2d>>();
  std::shared_ptr<index_t<IndexPointWGS3d>> wgs_3d_index_ = std::make_shared<index_t<IndexPointWGS3d>>();
  std::shared_ptr<index_t<IndexPointCartesian2d>> cartesian_2d_index_ =
      std::make_shared<index_t<IndexPointCartesian2d>>();
  std::shared_ptr<index_t<IndexPointCartesian3d>> cartesian_3d_index_ =
      std::make_shared<index_t<IndexPointCartesian3d>>();
};

namespace {
auto update_internal(index_container_t const &src, TrackedChanges const &tracked_changes)
    -> std::optional<index_container_t> {
  // All previous txns will use older index, this new built index will not concurrently be seen by older txns
  if (!tracked_changes.AnyChanges()) return std::nullopt;

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
      DMG_ASSERT(it != src.end());
      new_point_index.emplace(key, it->second);
    } else {
      // this key has changes, need to rebuild PointIndex for it
      new_point_index.emplace(key,
                              std::make_shared<PointIndex>(key_src_index.CreateNewPointIndex(key, changed_vertices)));
    }
  }

  return new_point_index;
}
}  // namespace

bool PointIndexStorage::CreatePointIndex(LabelId label, PropertyId property,
                                         memgraph::utils::SkipList<Vertex>::Accessor vertices) {
  // indexes_ protected by unique storage access
  auto &indexes = *indexes_;
  auto key = LabelPropKey{label, property};
  if (indexes.contains(key)) return false;

  auto points_2d_WGS = std::vector<Entry<IndexPointWGS2d>>{};
  auto points_2d_Crt = std::vector<Entry<IndexPointCartesian2d>>{};
  auto points_3d_WGS = std::vector<Entry<IndexPointWGS3d>>{};
  auto points_3d_Crt = std::vector<Entry<IndexPointCartesian3d>>{};

  for (auto const &v : vertices) {
    if (v.deleted) continue;
    if (!utils::Contains(v.labels, label)) continue;

    static constexpr auto point_types = std::array{PropertyStoreType::POINT};
    auto maybe_value = v.properties.GetPropertyOfTypes(property, point_types);
    if (!maybe_value) continue;

    auto value = *maybe_value;
    switch (value.type()) {
      case PropertyValueType::Point2d: {
        auto val = value.ValuePoint2d();
        if (IsWGS(val.crs())) {
          points_2d_WGS.emplace_back(IndexPointWGS2d{val}, &v);
        } else {
          points_2d_Crt.emplace_back(IndexPointCartesian2d{val}, &v);
        }
        break;
      }
      case PropertyValueType::Point3d: {
        auto val = value.ValuePoint3d();
        if (IsWGS(val.crs())) {
          points_3d_WGS.emplace_back(IndexPointWGS3d{val}, &v);
        } else {
          points_3d_Crt.emplace_back(IndexPointCartesian3d{val}, &v);
        }
        break;
      }
      default:
        continue;
    }
  }
  auto new_index = std::make_shared<PointIndex>(points_2d_WGS, points_2d_Crt, points_3d_WGS, points_3d_Crt);
  auto [_, inserted] = indexes.try_emplace(key, std::move(new_index));
  return inserted;
}

bool PointIndexStorage::DropPointIndex(LabelId label, PropertyId property) {
  // indexes_ protected by unique storage access
  auto &indexes = *indexes_;
  auto it = indexes.find(LabelPropKey{label, property});
  if (it == indexes.end()) return false;
  indexes.erase(it);
  return true;
}

void PointIndexStorage::InstallNewPointIndex(PointIndexChangeCollector &collector, PointIndexContext &context) {
  if (!context.UsingLocalIndex() && !collector.CurrentChanges().AnyChanges()) {
    // Hence TXN didn't do AdvanceCommand that required new private local index
    // no modification during the last command to require new index now
    return;
  }

  auto noOtherIndexUpdate = indexes_ == context.orig_indexes_;
  if (noOtherIndexUpdate) {
    // TODO: make a special case for inplace modification
    //    if (!context.UsingLocalIndex() && context.orig_indexes_.use_count() == 3) { /* ??? */}
    //    3 becasue indexes_ + orig_indexes_ + current_indexes_ should be the only references
    context.update_current(collector);
    indexes_ = context.current_indexes_;
  } else {
    // Another txn made a commit, we need to build from indexes_ + all collected changes (even from AdvanceCommand)
    // TODO: make a special case for inplace modification
    //    if (indexes_.use_count() == 1) { /* ??? */ }
    context.rebuild_current(indexes_, collector);
    indexes_ = context.current_indexes_;
  };
}
void PointIndexStorage::Clear() { indexes_->clear(); }

std::vector<std::pair<LabelId, PropertyId>> PointIndexStorage::ListIndices() {
  auto indexes = indexes_;  // local copy of shared_ptr, for safety
  auto keys = *indexes | std::views::keys | std::views::transform([](LabelPropKey key) {
    return std::pair{key.label(), key.property()};
  });
  return {keys.begin(), keys.end()};
}

std::optional<uint64_t> PointIndexStorage::ApproximatePointCount(LabelId labelId, PropertyId propertyId) {
  auto indexes = indexes_;  // local copy of shared_ptr, for safety
  auto it = indexes->find(LabelPropKey{labelId, propertyId});
  if (it == indexes->end()) return std::nullopt;
  return it->second->EntryCount();
}

bool PointIndexStorage::PointIndexExists(LabelId labelId, PropertyId propertyId) {
  auto indexes = indexes_;  // local copy of shared_ptr, for safety
  return indexes->contains(LabelPropKey{labelId, propertyId});
}

auto PointIndex::CreateNewPointIndex(LabelPropKey labelPropKey,
                                     absl::flat_hash_set<Vertex const *> const &changed_vertices) const -> PointIndex {
  DMG_ASSERT(!changed_vertices.empty(), "Expect at least one change");

  // These are at this stage uncommitted changes + must have come from this txn, no need to check MVCC history
  auto changed_wgs_2d = std::unordered_map<Vertex const *, IndexPointWGS2d>{};
  auto changed_wgs_3d = std::unordered_map<Vertex const *, IndexPointWGS3d>{};
  auto changed_cartesian_2d = std::unordered_map<Vertex const *, IndexPointCartesian2d>{};
  auto changed_cartesian_3d = std::unordered_map<Vertex const *, IndexPointCartesian3d>{};

  // Single pass over all changes to cache current values
  for (auto const *v : changed_vertices) {
    auto guard = std::shared_lock{v->lock};
    auto isDeleted = [](Vertex const *v) { return v->deleted; };
    auto isWithoutLabel = [label = labelPropKey.label()](Vertex const *v) {
      return !utils::Contains(v->labels, label);
    };
    if (isDeleted(v) || isWithoutLabel(v)) {
      continue;
    }
    constexpr auto all_point_types = std::array{PropertyStoreType::POINT};
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

  return PointIndex{helper(wgs_2d_index_, changed_wgs_2d), helper(wgs_3d_index_, changed_wgs_3d),
                    helper(cartesian_2d_index_, changed_cartesian_2d),
                    helper(cartesian_3d_index_, changed_cartesian_3d)};
}

void PointIndexContext::rebuild_current(std::shared_ptr<index_container_t> latest_index,
                                        PointIndexChangeCollector &collector) {
  orig_indexes_ = std::move(latest_index);
  current_indexes_ = orig_indexes_;

  auto result = update_internal(*current_indexes_, collector.PreviousChanges());
  if (result) current_indexes_ = std::make_unique<index_container_t>(*std::move(result));

  update_current(collector);
}

void PointIndexContext::update_current(PointIndexChangeCollector &collector) {
  auto result = update_internal(*current_indexes_, collector.CurrentChanges());
  if (result) current_indexes_ = std::make_unique<index_container_t>(*std::move(result));
  collector.ArchiveCurrentChanges();
}

auto PointIndexContext::PointVertices(LabelId label, PropertyId property, CoordinateReferenceSystem crs,
                                      Storage *storage, Transaction *transaction) -> PointIterable {
  auto const &indexes = *current_indexes_;
  auto it = indexes.find(LabelPropKey{label, property});
  if (it == indexes.cend()) {
    spdlog::warn("Failure why trying to locate a point index that should exist");
    return {};
  }

  auto const &point_index = *it->second;
  return {storage, transaction, point_index, crs};
}

auto PointIndexContext::PointVertices(LabelId label, PropertyId property, CoordinateReferenceSystem crs,
                                      Storage *storage, Transaction *transaction, PropertyValue point_value,
                                      PropertyValue boundary_value, PointDistanceCondition condition) -> PointIterable {
  auto const &indexes = *current_indexes_;
  auto it = indexes.find(LabelPropKey{label, property});
  if (it == indexes.cend()) {
    spdlog::warn("Failure why trying to locate a point index that should exist");
    return {};
  }

  auto const &point_index = *it->second;
  return {storage, transaction, point_index, crs, point_value, boundary_value, condition};
}

PointIndex::PointIndex(std::span<Entry<IndexPointWGS2d>> points2dWGS,
                       std::span<Entry<IndexPointCartesian2d>> points2dCartesian,
                       std::span<Entry<IndexPointWGS3d>> points3dWGS,
                       std::span<Entry<IndexPointCartesian3d>> points3dCartesian)
    : wgs_2d_index_{std::make_shared<index_t<IndexPointWGS2d>>(points2dWGS.begin(), points2dWGS.end())},
      wgs_3d_index_{std::make_shared<index_t<IndexPointWGS3d>>(points3dWGS.begin(), points3dWGS.end())},
      cartesian_2d_index_{
          std::make_shared<index_t<IndexPointCartesian2d>>(points2dCartesian.begin(), points2dCartesian.end())},
      cartesian_3d_index_{
          std::make_shared<index_t<IndexPointCartesian3d>>(points3dCartesian.begin(), points3dCartesian.end())} {}

PointIndex::PointIndex(std::shared_ptr<index_t<IndexPointWGS2d>> points2dWGS,
                       std::shared_ptr<index_t<IndexPointWGS3d>> points3dWGS,
                       std::shared_ptr<index_t<IndexPointCartesian2d>> points2dCartesian,
                       std::shared_ptr<index_t<IndexPointCartesian3d>> points3dCartesian)
    : wgs_2d_index_{std::move(points2dWGS)},
      wgs_3d_index_{std::move(points3dWGS)},
      cartesian_2d_index_{std::move(points2dCartesian)},
      cartesian_3d_index_{std::move(points3dCartesian)} {}

struct PointIterable::impl {
  explicit impl(Storage *storage, Transaction *transaction, std::shared_ptr<index_t<IndexPointWGS2d>> index)
      : storage_{storage},
        transaction_{transaction},
        crs_{CoordinateReferenceSystem::WGS84_2d},
        wgs84_2d_{std::move(index)} {}

  explicit impl(Storage *storage, Transaction *transaction, std::shared_ptr<index_t<IndexPointWGS3d>> index)
      : storage_{storage},
        transaction_{transaction},
        crs_{CoordinateReferenceSystem::WGS84_3d},
        wgs84_3d_{std::move(index)} {}

  explicit impl(Storage *storage, Transaction *transaction, std::shared_ptr<index_t<IndexPointCartesian2d>> index)
      : storage_{storage},
        transaction_{transaction},
        crs_{CoordinateReferenceSystem::Cartesian_2d},
        cartesian_2d_{std::move(index)} {}

  explicit impl(Storage *storage, Transaction *transaction, std::shared_ptr<index_t<IndexPointCartesian3d>> index)
      : storage_{storage},
        transaction_{transaction},
        crs_{CoordinateReferenceSystem::Cartesian_3d},
        cartesian_3d_{std::move(index)} {}

  // TODO: can this be made smaller?
  explicit impl(Storage *storage, Transaction *transaction, std::shared_ptr<index_t<IndexPointWGS2d>> index,
                PropertyValue point_value, PropertyValue boundary_value, PointDistanceCondition condition)
      : storage_{storage},
        transaction_{transaction},
        crs_{CoordinateReferenceSystem::WGS84_2d},
        point_value_{std::move(point_value)},
        boundary_value_{std::move(boundary_value)},
        using_distance_(true),
        wgs84_2d_{std::move(index)},
        distance_condition_{condition} {}

  explicit impl(Storage *storage, Transaction *transaction, std::shared_ptr<index_t<IndexPointWGS3d>> index,
                PropertyValue point_value, PropertyValue boundary_value, PointDistanceCondition condition)
      : storage_{storage},
        transaction_{transaction},
        crs_{CoordinateReferenceSystem::WGS84_3d},
        point_value_{std::move(point_value)},
        boundary_value_{std::move(boundary_value)},
        using_distance_(true),
        wgs84_3d_{std::move(index)},
        distance_condition_{condition} {}

  explicit impl(Storage *storage, Transaction *transaction, std::shared_ptr<index_t<IndexPointCartesian2d>> index,
                PropertyValue point_value, PropertyValue boundary_value, PointDistanceCondition condition)
      : storage_{storage},
        transaction_{transaction},
        crs_{CoordinateReferenceSystem::Cartesian_2d},
        point_value_{std::move(point_value)},
        boundary_value_{std::move(boundary_value)},
        using_distance_(true),
        cartesian_2d_{std::move(index)},
        distance_condition_{condition} {}

  explicit impl(Storage *storage, Transaction *transaction, std::shared_ptr<index_t<IndexPointCartesian3d>> index,
                PropertyValue point_value, PropertyValue boundary_value, PointDistanceCondition condition)
      : storage_{storage},
        transaction_{transaction},
        crs_{CoordinateReferenceSystem::Cartesian_3d},
        point_value_{std::move(point_value)},
        boundary_value_{std::move(boundary_value)},
        using_distance_(true),
        cartesian_3d_{std::move(index)},
        distance_condition_{condition} {}

  friend struct PointIterable;

  impl(impl const &) = delete;
  impl(impl &&) = delete;
  impl &operator=(impl const &) = delete;
  impl &operator=(impl &&) = delete;

  // don't need destroy_at for shared ptrs
  ~impl() {
    switch (crs_) {
      case CoordinateReferenceSystem::WGS84_2d:
        std::destroy_at(&wgs84_2d_);
        break;
      case CoordinateReferenceSystem::WGS84_3d:
        std::destroy_at(&wgs84_3d_);
        break;
      case CoordinateReferenceSystem::Cartesian_2d:
        std::destroy_at(&cartesian_2d_);
        break;
      case CoordinateReferenceSystem::Cartesian_3d:
        std::destroy_at(&cartesian_3d_);
        break;
    }
  }

 private:
  Storage *storage_;
  Transaction *transaction_;
  CoordinateReferenceSystem crs_;
  PropertyValue point_value_;
  PropertyValue boundary_value_;
  bool using_distance_;
  union {
    std::shared_ptr<index_t<IndexPointWGS2d>> wgs84_2d_;
    std::shared_ptr<index_t<IndexPointWGS3d>> wgs84_3d_;
    std::shared_ptr<index_t<IndexPointCartesian2d>> cartesian_2d_;
    std::shared_ptr<index_t<IndexPointCartesian3d>> cartesian_3d_;
  };

  union {
    PointDistanceCondition distance_condition_;
    WithinBBoxCondition withinbbox_condition_;
  };
};

PointIterable::PointIterable() : pimpl{nullptr} {};
PointIterable::~PointIterable() = default;
PointIterable::PointIterable(PointIterable &&) noexcept = default;
PointIterable &PointIterable::operator=(PointIterable &&) = default;
PointIterable::PointIterable(Storage *storage, Transaction *transaction, PointIndex const &index,
                             storage::CoordinateReferenceSystem crs) {
  switch (crs) {
    case CoordinateReferenceSystem::WGS84_2d: {
      pimpl = std::make_unique<impl>(storage, transaction, index.GetWgs2dIndex());
      return;
    }
    case CoordinateReferenceSystem::WGS84_3d: {
      pimpl = std::make_unique<impl>(storage, transaction, index.GetWgs3dIndex());
      return;
    }
    case CoordinateReferenceSystem::Cartesian_2d: {
      pimpl = std::make_unique<impl>(storage, transaction, index.GetCartesian2dIndex());
      return;
    }
    case CoordinateReferenceSystem::Cartesian_3d: {
      pimpl = std::make_unique<impl>(storage, transaction, index.GetCartesian3dIndex());
      return;
    }
  }
}

PointIterable::PointIterable(Storage *storage, Transaction *transaction, PointIndex const &index,
                             storage::CoordinateReferenceSystem crs, PropertyValue point_value,
                             PropertyValue boundary_value, PointDistanceCondition condition) {
  switch (crs) {
    case CoordinateReferenceSystem::WGS84_2d: {
      pimpl =
          std::make_unique<impl>(storage, transaction, index.GetWgs2dIndex(), point_value, boundary_value, condition);
      return;
    }
    case CoordinateReferenceSystem::WGS84_3d: {
      pimpl =
          std::make_unique<impl>(storage, transaction, index.GetWgs3dIndex(), point_value, boundary_value, condition);
      return;
    }
    case CoordinateReferenceSystem::Cartesian_2d: {
      pimpl = std::make_unique<impl>(storage, transaction, index.GetCartesian2dIndex(), point_value, boundary_value,
                                     condition);
      return;
    }
    case CoordinateReferenceSystem::Cartesian_3d: {
      pimpl = std::make_unique<impl>(storage, transaction, index.GetCartesian3dIndex(), point_value, boundary_value,
                                     condition);
      return;
    }
  }
}

namespace {

double toRadians(double degrees) { return degrees * M_PI / 180.0; }

double toDegrees(double radians) { return radians * 180.0 / M_PI; }

template <typename point_type>
requires std::is_same<typename bg::traits::coordinate_system<point_type>::type, bg::cs::cartesian>::value auto
create_bounding_box(const point_type &center_point, double boundary) -> bg::model::box<point_type> {
  constexpr auto n_dimensions = bg::traits::dimension<point_type>::value;
  return [&]<auto... I>(std::index_sequence<I...>) {
    auto const min_corner = point_type{(bg::get<I>(center_point) - boundary)...};
    auto const max_corner = point_type{(bg::get<I>(center_point) + boundary)...};
    return bg::model::box{min_corner, max_corner};
  }
  (std::make_index_sequence<n_dimensions>{});
}

template <typename point_type>
requires std::is_same<typename bg::traits::coordinate_system<point_type>::type,
                      bg::cs::geographic<bg::degree>>::value auto
create_bounding_box(const point_type &center_point, double boundary) -> bg::model::box<point_type> {
  constexpr auto n_dimensions = bg::traits::dimension<point_type>::value;
  double radDist = boundary / MEAN_EARTH_RADIUS;

  auto radLon = toRadians(bg::get<0>(center_point));
  auto radLat = toRadians(bg::get<1>(center_point));

  constexpr auto MIN_LAT = -M_PI;
  constexpr auto MAX_LAT = M_PI;

  constexpr auto MIN_LON = -2.0 * M_PI;
  constexpr auto MAX_LON = 2.0 * M_PI;

  double minLat = radLat - radDist;
  double maxLat = radLat + radDist;

  double minLon, maxLon;
  if (minLat > MIN_LAT && maxLat < MAX_LAT) {
    double deltaLon = std::asin(std::sin(radDist) / std::cos(radLat));
    minLon = radLon - deltaLon;
    if (minLon < MIN_LON) minLon += 2.0 * M_PI;
    maxLon = radLon + deltaLon;
    if (maxLon > MAX_LON) maxLon -= 2.0 * M_PI;
  } else {
    minLat = std::max(minLat, MIN_LAT);
    maxLat = std::min(maxLat, MAX_LAT);
    minLon = MIN_LON;
    maxLon = MAX_LON;
  }
  if constexpr (n_dimensions == 2) {
    auto min_corner = point_type{toDegrees(minLat), toDegrees(minLon)};
    auto max_corner = point_type{toDegrees(maxLat), toDegrees(maxLon)};
    return bg::model::box<point_type>{min_corner, max_corner};
  } else {
    auto min_corner = point_type{toDegrees(minLat), toDegrees(minLon), 0.0};
    auto max_corner = point_type{toDegrees(maxLat), toDegrees(maxLon), std::numeric_limits<double>::infinity()};
    return bg::model::box<point_type>{min_corner, max_corner};
  }
}

template <typename Index>
auto get_index_iterator_distance(Index &index, PropertyValue const &point_value, PropertyValue const &boundary_value,
                                 PointDistanceCondition condition) -> Index::const_query_iterator {
  double boundary =
      boundary_value.IsInt() ? static_cast<double>(boundary_value.ValueInt()) : boundary_value.ValueDouble();

  using enum PointDistanceCondition;
  if (boundary < 0.0) {
    // point.distance() will always be positive, a negative boundary needs special handling
    if ((condition == INSIDE || condition == INSIDE_AND_BOUNDARY)) {
      //  < or <= will be always false
      return index.qend();
    }
    // > or >= will be always true
    return index.qbegin(bgi::satisfies([](auto const &) { return true; }));
  }

  using point_type = Index::value_type::point_type;
  using CoordinateSystem = typename bg::traits::coordinate_system<point_type>::type;
  auto constexpr dimensions = bg::traits::dimension<point_type>::value;
  auto constexpr is_cartesian = std::is_same<CoordinateSystem, bg::cs::cartesian>::value;

  auto center_point = std::invoke([&]() {
    if constexpr (dimensions == 3) {
      auto tmp_point = point_value.ValuePoint3d();
      return point_type(tmp_point.x(), tmp_point.y(), tmp_point.z());
    } else {
      auto tmp_point = point_value.ValuePoint2d();
      return point_type(tmp_point.x(), tmp_point.y());
    }
  });

  auto center_point2 = std::invoke([&]() {
    if constexpr (dimensions == 3) {
      return point_value.ValuePoint3d();
    } else {
      return point_value.ValuePoint2d();
    }
  });

  auto inner_exclusion_box = [&] {
    auto get_inner_box_boundary = [](double radius) {
      auto offset = radius / std::sqrt(dimensions);
      // Need to ensure this inner box will not intersect with actual boundary,
      // because `bgi::covered_by` includes edges we are using `!bgi::covered_by` for our OUTSIDE
      // conditions.
      return std::max(0.0, std::nexttoward(offset, 0.0));
    };

    if constexpr (is_cartesian) {
      return create_bounding_box(center_point, get_inner_box_boundary(boundary));
    } else if constexpr (dimensions == 3) {
      // TODO
      auto min_corner = point_type{0, 0, 0};
      auto max_corner = point_type{0, 0, 0};
      return bg::model::box<point_type>{min_corner, max_corner};
    } else {
      // TODO
      auto min_corner = point_type{0, 0};
      auto max_corner = point_type{0, 0};
      return bg::model::box<point_type>{min_corner, max_corner};
    }
  };

  auto outer_inclusion_box = [&] { return create_bounding_box(center_point, boundary); };

  switch (condition) {
    case PointDistanceCondition::OUTSIDE: {
      // BOOST 1.81.0 covered_by geometry must be box
      return index.qbegin(!bgi::covered_by(inner_exclusion_box()) &&
                          bgi::satisfies([center_point, boundary](const auto &value) {
                            return bg::distance(value.point(), center_point) > boundary;
                          }));
    }
    case PointDistanceCondition::INSIDE: {
      return index.qbegin(bgi::covered_by(outer_inclusion_box()) &&
                          bgi::satisfies([center_point, boundary](const auto &value) {
                            return bg::distance(value.point(), center_point) < boundary;
                          }));
    }
    case PointDistanceCondition::INSIDE_AND_BOUNDARY: {
      return index.qbegin(bgi::covered_by(outer_inclusion_box()) &&
                          bgi::satisfies([center_point, boundary](const auto &value) {
                            return bg::distance(value.point(), center_point) <= boundary;
                          }));
    }
    case PointDistanceCondition::OUTSIDE_AND_BOUNDARY: {
      return index.qbegin(!bgi::covered_by(inner_exclusion_box()) &&
                          bgi::satisfies([center_point, boundary](const auto &value) {
                            return bg::distance(value.point(), center_point) >= boundary;
                          }));
    }
  }
}

}  // namespace

auto PointIterable::begin() const -> PointIterator {
  if (pimpl->using_distance_) {
    switch (pimpl->crs_) {
      case CoordinateReferenceSystem::WGS84_2d:
        return PointIterator{pimpl->storage_, pimpl->transaction_, pimpl->crs_,
                             get_index_iterator_distance(*pimpl->wgs84_2d_, pimpl->point_value_, pimpl->boundary_value_,
                                                         pimpl->distance_condition_)};
      case CoordinateReferenceSystem::WGS84_3d:
        return PointIterator{pimpl->storage_, pimpl->transaction_, pimpl->crs_,
                             get_index_iterator_distance(*pimpl->wgs84_3d_, pimpl->point_value_, pimpl->boundary_value_,
                                                         pimpl->distance_condition_)};
      case CoordinateReferenceSystem::Cartesian_2d:
        return PointIterator{pimpl->storage_, pimpl->transaction_, pimpl->crs_,
                             get_index_iterator_distance(*pimpl->cartesian_2d_, pimpl->point_value_,
                                                         pimpl->boundary_value_, pimpl->distance_condition_)};
      case CoordinateReferenceSystem::Cartesian_3d:
        return PointIterator{pimpl->storage_, pimpl->transaction_, pimpl->crs_,
                             get_index_iterator_distance(*pimpl->cartesian_3d_, pimpl->point_value_,
                                                         pimpl->boundary_value_, pimpl->distance_condition_)};
    }
  } else {
    throw utils::NotYetImplemented("Crash");
  }
}
auto PointIterable::end() const -> PointIterator {
  if (pimpl->using_distance_) {
    switch (pimpl->crs_) {
      case CoordinateReferenceSystem::WGS84_2d:
        return PointIterator{pimpl->storage_, pimpl->transaction_, pimpl->crs_, pimpl->wgs84_2d_->qend()};
      case CoordinateReferenceSystem::WGS84_3d:
        return PointIterator{pimpl->storage_, pimpl->transaction_, pimpl->crs_, pimpl->wgs84_3d_->qend()};
      case CoordinateReferenceSystem::Cartesian_2d:
        return PointIterator{pimpl->storage_, pimpl->transaction_, pimpl->crs_, pimpl->cartesian_2d_->qend()};
      case CoordinateReferenceSystem::Cartesian_3d:
        return PointIterator{pimpl->storage_, pimpl->transaction_, pimpl->crs_, pimpl->cartesian_3d_->qend()};
    }
  } else {
    throw utils::NotYetImplemented("Crash");
  }
}

}  // namespace memgraph::storage
