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
#include "storage/v2/indices/point_index_change_collector.hpp"
#include "storage/v2/indices/point_index_expensive_header.hpp"
#include "storage/v2/indices/point_iterator.hpp"
#include "storage/v2/vertex.hpp"

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

  friend struct PointIterable;

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
  union {
    std::shared_ptr<index_t<IndexPointWGS2d>> wgs84_2d_;
    std::shared_ptr<index_t<IndexPointWGS3d>> wgs84_3d_;
    std::shared_ptr<index_t<IndexPointCartesian2d>> cartesian_2d_;
    std::shared_ptr<index_t<IndexPointCartesian3d>> cartesian_3d_;
  };
};

PointIterable::PointIterable() : pimpl{nullptr} {};
PointIterable::~PointIterable() = default;
PointIterable::PointIterable(PointIterable &&) = default;
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

auto PointIterable::begin() const -> PointIterator {
  switch (pimpl->crs_) {
    case CoordinateReferenceSystem::WGS84_2d:
      return PointIterator{pimpl->storage_, pimpl->transaction_, pimpl->crs_, pimpl->wgs84_2d_->begin()};
    case CoordinateReferenceSystem::WGS84_3d:
      return PointIterator{pimpl->storage_, pimpl->transaction_, pimpl->crs_, pimpl->wgs84_3d_->begin()};
    case CoordinateReferenceSystem::Cartesian_2d:
      return PointIterator{pimpl->storage_, pimpl->transaction_, pimpl->crs_, pimpl->cartesian_2d_->begin()};
    case CoordinateReferenceSystem::Cartesian_3d:
      return PointIterator{pimpl->storage_, pimpl->transaction_, pimpl->crs_, pimpl->cartesian_3d_->begin()};
  }
}
auto PointIterable::end() const -> PointIterator {
  switch (pimpl->crs_) {
    case CoordinateReferenceSystem::WGS84_2d:
      return PointIterator{pimpl->storage_, pimpl->transaction_, pimpl->crs_, pimpl->wgs84_2d_->end()};
    case CoordinateReferenceSystem::WGS84_3d:
      return PointIterator{pimpl->storage_, pimpl->transaction_, pimpl->crs_, pimpl->wgs84_3d_->end()};
    case CoordinateReferenceSystem::Cartesian_2d:
      return PointIterator{pimpl->storage_, pimpl->transaction_, pimpl->crs_, pimpl->cartesian_2d_->end()};
    case CoordinateReferenceSystem::Cartesian_3d:
      return PointIterator{pimpl->storage_, pimpl->transaction_, pimpl->crs_, pimpl->cartesian_3d_->end()};
  }
}

}  // namespace memgraph::storage
