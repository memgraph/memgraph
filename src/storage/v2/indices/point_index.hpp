// Copyright 2026 Memgraph Ltd.
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
#include <cstdint>
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/point_index_change_collector.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/snapshot_observer_info.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::storage {

enum class PointDistanceCondition : uint8_t { OUTSIDE, INSIDE, INSIDE_AND_BOUNDARY, OUTSIDE_AND_BOUNDARY };
enum class WithinBBoxCondition : uint8_t { OUTSIDE, INSIDE };

struct PointIndex;

using index_container_t = std::map<LabelPropKey, std::shared_ptr<PointIndex const>>;

struct PointIterator;

// TODO move?
struct PointIterable {
  using iterator = PointIterator;

  PointIterable(Storage *storage, Transaction *transaction, PointIndex const &index,
                storage::CoordinateReferenceSystem crs, PropertyValue const &point_value,
                PropertyValue const &boundary_value, PointDistanceCondition condition);
  PointIterable(Storage *storage, Transaction *transaction, PointIndex const &index,
                storage::CoordinateReferenceSystem crs, PropertyValue const &bottom_left,
                PropertyValue const &top_right, WithinBBoxCondition condition);
  ~PointIterable();

  PointIterable();
  PointIterable(PointIterable &&) noexcept;
  PointIterable &operator=(PointIterable &&);

  auto begin() const -> iterator;
  auto end() const -> iterator;

  friend bool operator==(PointIterable const &, PointIterable const &) = default;

 private:
  struct impl;
  std::unique_ptr<impl> pimpl;
};

struct PointIndexContext {
  auto IndexKeys() const { return *orig_indexes_ | std::views::keys; }

  bool UsingLocalIndex() const { return orig_indexes_ != current_indexes_; }

  void AdvanceCommand(PointIndexChangeCollector &collector) { update_current(collector); }

  auto PointVertices(LabelId label, PropertyId property, CoordinateReferenceSystem crs, Storage *storage,
                     Transaction *transaction, PropertyValue const &point_value, PropertyValue const &boundary_value,
                     PointDistanceCondition condition) -> PointIterable;

  auto PointVertices(LabelId label, PropertyId property, CoordinateReferenceSystem crs, Storage *storage,
                     Transaction *transaction, PropertyValue const &bottom_left, PropertyValue const &top_right,
                     WithinBBoxCondition condition) -> PointIterable;

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

struct ActiveIndicesUpdater;

/// Abstract interface for point index metadata queries accessed through ActiveIndices snapshots.
struct PointIndexActiveIndices {
  virtual ~PointIndexActiveIndices() = default;
  virtual bool PointIndexExists(LabelId labelId, PropertyId propertyId) const = 0;
  virtual std::optional<uint64_t> ApproximatePointCount(LabelId labelId, PropertyId propertyId) const = 0;
  virtual std::vector<std::pair<LabelId, PropertyId>> ListIndices() const = 0;
};

struct PointIndexStorage {
  /// Concrete ActiveIndices implementation holding an immutable snapshot of the index container.
  struct ActiveIndices : PointIndexActiveIndices {
    explicit ActiveIndices(std::shared_ptr<index_container_t> indexes = std::make_shared<index_container_t>())
        : indexes_(std::move(indexes)) {}

    bool PointIndexExists(LabelId labelId, PropertyId propertyId) const override;
    std::optional<uint64_t> ApproximatePointCount(LabelId labelId, PropertyId propertyId) const override;
    std::vector<std::pair<LabelId, PropertyId>> ListIndices() const override;

   private:
    std::shared_ptr<index_container_t> indexes_;
  };

  // Query (modify index set). Caller is responsible for publishing the
  // resulting ActiveIndices snapshot via PublishActiveIndices — for user DDL
  // this is typically deferred through Transaction::commit_callbacks_.
  bool CreatePointIndex(LabelId label, PropertyId property, utils::SkipList<Vertex>::Accessor vertices,
                        std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt);
  bool DropPointIndex(LabelId label, PropertyId property);

  /// Retrieves the live PointIndex for (label, property) without mutating the
  /// owner. Used by abort rollback of DROP to capture the entry before the
  /// drop so it can be re-inserted if the txn aborts.
  std::shared_ptr<PointIndex const> GetPointIndex(LabelId label, PropertyId property) const;

  /// Re-installs an entry previously captured via GetPointIndex. Used by abort
  /// rollback of DROP POINT INDEX. No-op if a re-created entry with the same
  /// key already exists.
  void RestorePointIndex(LabelId label, PropertyId property, std::shared_ptr<PointIndex const> index);

  // Transaction (establish what to collect + able to build next index)
  auto CreatePointIndexContext() const -> PointIndexContext { return PointIndexContext{indexes_}; }

  /// Returns the current active indices snapshot for use in transactions.
  auto GetActiveIndices() -> std::shared_ptr<PointIndexActiveIndices const> {
    return std::make_shared<ActiveIndices const>(indexes_);
  }

  // Commit
  void InstallNewPointIndex(PointIndexChangeCollector &collector, PointIndexContext &context,
                            ActiveIndicesUpdater const &updater);

  void Clear();

  /// ListIndices on the owning class (for snapshot creation, outside transaction context).
  std::vector<std::pair<LabelId, PropertyId>> ListIndices();

  bool PointIndexExists(LabelId labelId, PropertyId propertyId);

  /// Publishes the current index container as the new ActiveIndices snapshot.
  void PublishActiveIndices(ActiveIndicesUpdater const &updater);

 private:
  // Invariant: `indexes_` is only mutated under UNIQUE storage access (see the MG_ASSERTs in
  // InMemoryAccessor::CreatePointIndex / DropPointIndex and in DropGraphClearIndices). Reads from
  // other contexts (regular READ/WRITE accessors, DatabaseInfoQuery) MUST go through the published
  // snapshot in `ActiveIndicesStore` -- UNIQUE excludes READ/WRITE, which is what makes direct
  // access to `indexes_` from commit-time hot paths race-free.
  std::shared_ptr<index_container_t> indexes_ = std::make_shared<index_container_t>();
};

}  // namespace memgraph::storage
