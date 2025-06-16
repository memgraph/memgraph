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

#include "storage/v2/indices/point_index.hpp"

#include <boost/geometry.hpp>
#include <boost/geometry/index/predicates.hpp>

#include <cmath>
#include <utility>
#include "storage/v2/indices/point_index_change_collector.hpp"
#include "storage/v2/indices/point_index_expensive_header.hpp"
#include "storage/v2/indices/point_iterator.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/vertex.hpp"
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

bool PointIndexStorage::CreatePointIndex(LabelId label, PropertyId property, utils::SkipList<Vertex>::Accessor vertices,
                                         std::optional<SnapshotObserverInfo> const &snapshot_info) {
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

    if (snapshot_info) {
      snapshot_info->Update(UpdateType::POINT_IDX);
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
    //    3 because indexes_ + orig_indexes_ + current_indexes_ should be the only references
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
                                      Storage *storage, Transaction *transaction, PropertyValue const &point_value,
                                      PropertyValue const &boundary_value, PointDistanceCondition condition)
    -> PointIterable {
  auto const &indexes = *current_indexes_;
  auto it = indexes.find(LabelPropKey{label, property});
  if (it == indexes.cend()) {
    spdlog::warn("Failure: trying to locate a point index that should exist");
    return {};
  }

  auto const &point_index = *it->second;
  return {storage, transaction, point_index, crs, point_value, boundary_value, condition};
}

auto PointIndexContext::PointVertices(LabelId label, PropertyId property, CoordinateReferenceSystem crs,
                                      Storage *storage, Transaction *transaction, PropertyValue const &bottom_left,
                                      PropertyValue const &top_right, WithinBBoxCondition condition) -> PointIterable {
  auto const &indexes = *current_indexes_;
  auto it = indexes.find(LabelPropKey{label, property});
  if (it == indexes.cend()) {
    spdlog::warn("Failure: trying to locate a point index that should exist");
    return {};
  }

  auto const &point_index = *it->second;
  return {storage, transaction, point_index, crs, bottom_left, top_right, condition};
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
  explicit impl(Storage *storage, Transaction *transaction, std::shared_ptr<index_t<IndexPointWGS2d>> index,
                PropertyValue point_value, PropertyValue boundary_value, PointDistanceCondition condition)
      : storage_{storage},
        transaction_{transaction},
        crs_{CoordinateReferenceSystem::WGS84_2d},
        using_distance_(true),
        point_value_{std::move(point_value)},
        boundary_value_{std::move(boundary_value)},
        distance_condition_{condition},
        wgs84_2d_{std::move(index)} {}

  explicit impl(Storage *storage, Transaction *transaction, std::shared_ptr<index_t<IndexPointWGS3d>> index,
                PropertyValue point_value, PropertyValue boundary_value, PointDistanceCondition condition)
      : storage_{storage},
        transaction_{transaction},
        crs_{CoordinateReferenceSystem::WGS84_3d},
        using_distance_(true),
        point_value_{std::move(point_value)},
        boundary_value_{std::move(boundary_value)},
        distance_condition_{condition},
        wgs84_3d_{std::move(index)} {}

  explicit impl(Storage *storage, Transaction *transaction, std::shared_ptr<index_t<IndexPointCartesian2d>> index,
                PropertyValue point_value, PropertyValue boundary_value, PointDistanceCondition condition)
      : storage_{storage},
        transaction_{transaction},
        crs_{CoordinateReferenceSystem::Cartesian_2d},
        using_distance_(true),
        point_value_{std::move(point_value)},
        boundary_value_{std::move(boundary_value)},
        distance_condition_{condition},
        cartesian_2d_{std::move(index)} {}

  explicit impl(Storage *storage, Transaction *transaction, std::shared_ptr<index_t<IndexPointCartesian3d>> index,
                PropertyValue point_value, PropertyValue boundary_value, PointDistanceCondition condition)
      : storage_{storage},
        transaction_{transaction},
        crs_{CoordinateReferenceSystem::Cartesian_3d},
        using_distance_(true),
        point_value_{std::move(point_value)},
        boundary_value_{std::move(boundary_value)},
        distance_condition_{condition},
        cartesian_3d_{std::move(index)} {}

  explicit impl(Storage *storage, Transaction *transaction, std::shared_ptr<index_t<IndexPointWGS2d>> index,
                PropertyValue bottom_left, PropertyValue top_right, WithinBBoxCondition condition)
      : storage_{storage},
        transaction_{transaction},
        crs_{CoordinateReferenceSystem::WGS84_2d},
        using_distance_(false),
        bottom_left_{std::move(bottom_left)},
        top_right_{std::move(top_right)},
        withinbbox_condition_{condition},
        wgs84_2d_{std::move(index)} {}

  explicit impl(Storage *storage, Transaction *transaction, std::shared_ptr<index_t<IndexPointWGS3d>> index,
                PropertyValue bottom_left, PropertyValue top_right, WithinBBoxCondition condition)
      : storage_{storage},
        transaction_{transaction},
        crs_{CoordinateReferenceSystem::WGS84_3d},
        using_distance_(false),
        bottom_left_{std::move(bottom_left)},
        top_right_{std::move(top_right)},
        withinbbox_condition_{condition},
        wgs84_3d_{std::move(index)} {}

  explicit impl(Storage *storage, Transaction *transaction, std::shared_ptr<index_t<IndexPointCartesian2d>> index,
                PropertyValue bottom_left, PropertyValue top_right, WithinBBoxCondition condition)
      : storage_{storage},
        transaction_{transaction},
        crs_{CoordinateReferenceSystem::Cartesian_2d},
        using_distance_(false),
        bottom_left_{std::move(bottom_left)},
        top_right_{std::move(top_right)},
        withinbbox_condition_{condition},
        cartesian_2d_{std::move(index)} {}

  explicit impl(Storage *storage, Transaction *transaction, std::shared_ptr<index_t<IndexPointCartesian3d>> index,
                PropertyValue bottom_left, PropertyValue top_right, WithinBBoxCondition condition)
      : storage_{storage},
        transaction_{transaction},
        crs_{CoordinateReferenceSystem::Cartesian_3d},
        using_distance_(false),
        bottom_left_{std::move(bottom_left)},
        top_right_{std::move(top_right)},
        withinbbox_condition_{condition},
        cartesian_3d_{std::move(index)} {}

  friend struct PointIterable;

  impl(impl const &) = delete;
  impl(impl &&) = delete;
  impl &operator=(impl const &) = delete;
  impl &operator=(impl &&) = delete;

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
    if (using_distance_) {
      std::destroy_at(&point_value_);
    } else {
      std::destroy_at(&bottom_left_);
      std::destroy_at(&top_right_);
    }
  }

 private:
  Storage *storage_;
  Transaction *transaction_;
  CoordinateReferenceSystem crs_;
  bool using_distance_;

  union {
    struct {
      PropertyValue point_value_;
      PropertyValue boundary_value_;
      PointDistanceCondition distance_condition_;
    };

    struct {
      PropertyValue bottom_left_;
      PropertyValue top_right_;
      WithinBBoxCondition withinbbox_condition_;
    };
  };

  union {
    std::shared_ptr<index_t<IndexPointWGS2d>> wgs84_2d_;
    std::shared_ptr<index_t<IndexPointWGS3d>> wgs84_3d_;
    std::shared_ptr<index_t<IndexPointCartesian2d>> cartesian_2d_;
    std::shared_ptr<index_t<IndexPointCartesian3d>> cartesian_3d_;
  };
};

PointIterable::PointIterable() : pimpl{nullptr} {};
PointIterable::~PointIterable() = default;
PointIterable::PointIterable(PointIterable &&) noexcept = default;
PointIterable &PointIterable::operator=(PointIterable &&) = default;

namespace {
auto make_pimpl(PointIndex const &index, storage::CoordinateReferenceSystem crs, auto &&func) {
  switch (crs) {
    case CoordinateReferenceSystem::WGS84_2d: {
      return func(index.GetWgs2dIndex());
    }
    case CoordinateReferenceSystem::WGS84_3d: {
      return func(index.GetWgs3dIndex());
    }
    case CoordinateReferenceSystem::Cartesian_2d: {
      return func(index.GetCartesian2dIndex());
    }
    case CoordinateReferenceSystem::Cartesian_3d: {
      return func(index.GetCartesian3dIndex());
    }
  }
};
}  // namespace

PointIterable::PointIterable(Storage *storage, Transaction *transaction, PointIndex const &index,
                             storage::CoordinateReferenceSystem crs, PropertyValue const &point_value,
                             PropertyValue const &boundary_value, PointDistanceCondition condition)
    : pimpl{make_pimpl(index, crs, [&](auto specific_index) {
        return std::make_unique<impl>(storage, transaction, std::move(specific_index), point_value, boundary_value,
                                      condition);
      })} {}

PointIterable::PointIterable(Storage *storage, Transaction *transaction, PointIndex const &index,
                             storage::CoordinateReferenceSystem crs, PropertyValue const &bottom_left,
                             PropertyValue const &top_right, WithinBBoxCondition condition)
    : pimpl{make_pimpl(index, crs, [&](auto specific_index) {
        return std::make_unique<impl>(storage, transaction, std::move(specific_index), bottom_left, top_right,
                                      condition);
      })} {}

namespace {

double toRadians(double degrees) { return degrees * M_PI / 180.0; }

double toDegrees(double radians) { return radians * 180.0 / M_PI; }

template <typename point_type>
requires std::is_same_v<typename bg::traits::coordinate_system<point_type>::type, bg::cs::cartesian>
auto create_bounding_box(const point_type &center_point, double boundary) -> bg::model::box<point_type> {
  constexpr auto n_dimensions = bg::traits::dimension<point_type>::value;
  return [&]<auto... I>(std::index_sequence<I...>) {
    auto const min_corner = point_type{(bg::get<I>(center_point) - boundary)...};
    auto const max_corner = point_type{(bg::get<I>(center_point) + boundary)...};
    return bg::model::box{min_corner, max_corner};
  }
  (std::make_index_sequence<n_dimensions>{});
}

template <typename point_type>
requires std::is_same_v<typename bg::traits::coordinate_system<point_type>::type, bg::cs::geographic<bg::degree>>
auto create_bounding_box(const point_type &center_point, double boundary) -> bg::model::box<point_type> {
  // Our approximation for earth radius
  constexpr double MEAN_EARTH_RADIUS = 6'371'009;
  double const radDist = boundary / MEAN_EARTH_RADIUS;

  auto const radLon = toRadians(bg::get<0>(center_point));
  auto const radLat = toRadians(bg::get<1>(center_point));

  constexpr auto MIN_LAT = -M_PI_2;
  constexpr auto MAX_LAT = M_PI_2;

  constexpr auto MIN_LON = -M_PI;
  constexpr auto MAX_LON = M_PI;

  double minLat = radLat - radDist;
  double maxLat = radLat + radDist;

  double minLon;  // NOLINT(cppcoreguidelines-init-variables)
  double maxLon;  // NOLINT(cppcoreguidelines-init-variables)
  // check if latitude needs to truncate at the poles
  if (minLat > MIN_LAT && maxLat < MAX_LAT) {
    double const deltaLon = std::asin(std::sin(radDist) / std::cos(radLat));
    minLon = radLon - deltaLon;
    if (minLon < MIN_LON) minLon += 2.0 * M_PI;
    maxLon = radLon + deltaLon;
    if (maxLon > MAX_LON) maxLon -= 2.0 * M_PI;

    // for rtree `covered_by` needs the box the have lb <= ub
    // it internally will deal with these non-normalised degrees
    if (maxLon < minLon) maxLon += 2.0 * M_PI;

  } else {
    minLat = std::max(minLat, MIN_LAT);
    maxLat = std::min(maxLat, MAX_LAT);
    minLon = MIN_LON;
    maxLon = MAX_LON;
  }

  constexpr auto n_dimensions = bg::traits::dimension<point_type>::value;
  if constexpr (n_dimensions == 2) {
    auto min_corner = point_type{toDegrees(minLon), toDegrees(minLat)};
    auto max_corner = point_type{toDegrees(maxLon), toDegrees(maxLat)};
    return bg::model::box<point_type>{min_corner, max_corner};
  } else {
    auto height_center = bg::get<2>(center_point);
    auto min_corner = point_type{toDegrees(minLon), toDegrees(minLat), height_center - boundary};
    auto max_corner = point_type{toDegrees(maxLon), toDegrees(maxLat), height_center + boundary};
    return bg::model::box<point_type>{min_corner, max_corner};
  }
}

template <typename Index>
auto get_index_iterator_distance(Index const &index, PropertyValue const &point_value,
                                 PropertyValue const &boundary_value, PointDistanceCondition condition)
    -> Index::const_query_iterator {
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
  auto constexpr dimensions = bg::traits::dimension<point_type>::value;
  using CoordinateSystem = typename bg::traits::coordinate_system<point_type>::type;
  auto constexpr is_cartesian = std::is_same_v<CoordinateSystem, bg::cs::cartesian>;

  auto center_point = std::invoke([&]() -> point_type {
    if constexpr (dimensions == 2) {
      auto tmp_point = point_value.ValuePoint2d();
      return {tmp_point.x(), tmp_point.y()};
    } else {
      auto tmp_point = point_value.ValuePoint3d();
      return {tmp_point.x(), tmp_point.y(), tmp_point.z()};
    }
  });

  auto inner_exclusion_box = [&] {
    // dimensional scaling
    auto offset = boundary / std::sqrt(dimensions);
    // Need to ensure this inner box will not intersect with actual boundary,
    // because `bgi::covered_by` includes edges we are using `!bgi::covered_by` for our OUTSIDE
    // conditions.
    auto slightly_smaller = std::max(0.0, std::nexttoward(offset, 0.0));

    return create_bounding_box(center_point, slightly_smaller);
  };

  auto outer_inclusion_box = [&] {
    auto slightly_larger = std::nexttoward(boundary, std::numeric_limits<long double>::infinity());
    return create_bounding_box(center_point, slightly_larger);
  };

  auto true_distance = [](auto a, auto b) {
    if constexpr (is_cartesian || dimensions == 2) {
      return bg::distance(a, b);
    } else {
      // SPECIAL CASE: WGS-84 3D

      // We could reply on boost implementation that ignores the height for distance, but for possible future
      // boost changes, going to hand code operations to the 2d equivilant.

      // distance using average height of the two points
      auto h1 = bg::get<2>(a);
      auto h2 = bg::get<2>(b);
      auto middle = std::midpoint(h1, h2);
      auto avg_a = point_type{bg::get<0>(a), bg::get<1>(a), middle};
      auto avg_b = point_type{bg::get<0>(b), bg::get<1>(b), middle};
      auto distance_spherical = bg::distance(avg_a, avg_b);

      // use Pythagoras' theorem, combining height difference
      auto height_diff = h1 - h2;
      return std::sqrt(height_diff * height_diff + distance_spherical * distance_spherical);
    }
  };

  switch (condition) {
    case PointDistanceCondition::OUTSIDE: {
      // BOOST 1.81.0 covered_by geometry must be box
      return index.qbegin(!bgi::covered_by(inner_exclusion_box()) && bgi::satisfies([=](const auto &value) {
        return true_distance(value.point(), center_point) > boundary;
      }));
    }
    case PointDistanceCondition::INSIDE: {
      return index.qbegin(bgi::covered_by(outer_inclusion_box()) && bgi::satisfies([=](const auto &value) {
                            return true_distance(value.point(), center_point) < boundary;
                          }));
    }
    case PointDistanceCondition::INSIDE_AND_BOUNDARY: {
      return index.qbegin(bgi::covered_by(outer_inclusion_box()) && bgi::satisfies([=](const auto &value) {
                            return true_distance(value.point(), center_point) <= boundary;
                          }));
    }
    case PointDistanceCondition::OUTSIDE_AND_BOUNDARY: {
      return index.qbegin(!bgi::covered_by(inner_exclusion_box()) && bgi::satisfies([=](const auto &value) {
        return true_distance(value.point(), center_point) >= boundary;
      }));
    }
  }
}

template <typename Index>
auto get_index_iterator_withinbbox(Index &index, PropertyValue const &bottom_left, PropertyValue const &top_right,
                                   WithinBBoxCondition condition) -> Index::const_query_iterator {
  using point_type = Index::value_type::point_type;
  auto constexpr dimensions = bg::traits::dimension<point_type>::value;

  auto const lower_bound = std::invoke([&bottom_left]() -> point_type {
    if constexpr (dimensions == 2) {
      auto tmp_point = bottom_left.ValuePoint2d();
      return {tmp_point.x(), tmp_point.y()};
    } else {
      auto tmp_point = bottom_left.ValuePoint3d();
      return {tmp_point.x(), tmp_point.y(), tmp_point.z()};
    }
  });

  auto const upper_bound = std::invoke([&lower_bound, &top_right]() -> point_type {
    using CoordinateSystem = typename bg::traits::coordinate_system<point_type>::type;
    auto constexpr is_cartesian = std::is_same_v<CoordinateSystem, bg::cs::cartesian>;
    if constexpr (dimensions == 2) {
      auto const tmp_point = top_right.ValuePoint2d();
      if constexpr (is_cartesian) return {tmp_point.x(), tmp_point.y()};

      auto const longitude = tmp_point.x();
      return {bg::get<0>(lower_bound) <= longitude ? longitude : longitude + 360.0, tmp_point.y()};
    } else {
      auto const tmp_point = top_right.ValuePoint3d();
      if constexpr (is_cartesian) return {tmp_point.x(), tmp_point.y(), tmp_point.z()};

      auto const longitude = tmp_point.x();
      return {bg::get<0>(lower_bound) <= longitude ? longitude : longitude + 360.0, tmp_point.y(), tmp_point.z()};
    }
  });

  auto const bounding_box = bg::model::box(lower_bound, upper_bound);

  switch (condition) {
    case WithinBBoxCondition::OUTSIDE: {
      return index.qbegin(!bgi::covered_by(bounding_box));
    }
    case WithinBBoxCondition::INSIDE: {
      return index.qbegin(bgi::covered_by(bounding_box));
    }
  }
}

}  // namespace

auto PointIterable::begin() const -> PointIterator {
  auto apply_index = [&](CoordinateReferenceSystem crs, auto &&func) {
    switch (crs) {
      case CoordinateReferenceSystem::WGS84_2d:
        return func(*pimpl->wgs84_2d_);
      case CoordinateReferenceSystem::WGS84_3d:
        return func(*pimpl->wgs84_3d_);
      case CoordinateReferenceSystem::Cartesian_2d:
        return func(*pimpl->cartesian_2d_);
      case CoordinateReferenceSystem::Cartesian_3d:
        return func(*pimpl->cartesian_3d_);
    }
  };

  if (pimpl->using_distance_) {
    auto make_distance_iter = [&](auto const &index) {
      return PointIterator{
          pimpl->storage_, pimpl->transaction_, pimpl->crs_,
          get_index_iterator_distance(index, pimpl->point_value_, pimpl->boundary_value_, pimpl->distance_condition_)};
    };
    return apply_index(pimpl->crs_, make_distance_iter);
  }

  auto make_withinbbox_iter = [&](auto const &index) {
    return PointIterator{
        pimpl->storage_, pimpl->transaction_, pimpl->crs_,
        get_index_iterator_withinbbox(index, pimpl->bottom_left_, pimpl->top_right_, pimpl->withinbbox_condition_)};
  };
  return apply_index(pimpl->crs_, make_withinbbox_iter);
}
auto PointIterable::end() const -> PointIterator {
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
}

}  // namespace memgraph::storage
