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
#include <cstdint>
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/point_index_change_collector.hpp"
#include "storage/v2/property_value.hpp"
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

struct PointIndexStorage {
  // TODO: consider passkey idiom

  // Query (modify index set)
  bool CreatePointIndex(LabelId label, PropertyId property, utils::SkipList<Vertex>::Accessor vertices);
  bool DropPointIndex(LabelId label, PropertyId property);

  // Transaction (estabilish what to collect + able to build next index)
  auto CreatePointIndexContext() const -> PointIndexContext { return PointIndexContext{indexes_}; }

  // Commit
  void InstallNewPointIndex(PointIndexChangeCollector &collector, PointIndexContext &context);

  void Clear();

  std::vector<std::pair<LabelId, PropertyId>> ListIndices();

  std::optional<uint64_t> ApproximatePointCount(LabelId labelId, PropertyId propertyId);

  bool PointIndexExists(LabelId labelId, PropertyId propertyId);

 private:
  std::shared_ptr<index_container_t> indexes_ = std::make_shared<index_container_t>();
};

}  // namespace memgraph::storage
