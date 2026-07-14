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

#include "storage/v2/inmemory/vertex_property_index.hpp"
#include <range/v3/all.hpp>

#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/active_indices_updater.hpp"
#include "storage/v2/indices/indices_utils.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/counter.hpp"

namespace r = ranges;
namespace rv = r::views;

namespace memgraph::storage {

namespace {
inline void TryInsertVertexPropertyIndex(Vertex &vertex, PropertyId property, auto &&index_accessor,
                                         std::optional<SnapshotObserverInfo> const &snapshot_info) {
  if (vertex.deleted()) {
    return;
  }
  auto value = vertex.properties.GetProperty(property);
  if (value.IsNull()) {
    return;
  }
  index_accessor.insert({std::move(value), &vertex, 0});
  if (snapshot_info) {
    snapshot_info->Update(UpdateType::VERTICES);
  }
}

inline void TryInsertVertexPropertyIndex(Vertex &vertex, PropertyId property, auto &&index_accessor,
                                         std::optional<SnapshotObserverInfo> const &snapshot_info,
                                         Transaction const &tx) {
  bool exists = true;
  bool deleted = false;
  Delta *delta = nullptr;
  PropertyValue property_value;

  {
    auto guard = std::shared_lock{vertex.lock};
    deleted = vertex.deleted();
    delta = vertex.delta();
    property_value = vertex.properties.GetProperty(property);

    if (!vertex.has_uncommitted_non_sequential_deltas()) {
      guard.unlock();
    }

    if (delta) {
      ApplyDeltasForRead(&tx, delta, View::OLD, [&](Delta const &delta) {
        // clang-format off
        DeltaDispatch(delta, utils::ChainedOverloaded{
          Exists_ActionMethod(exists),
          Deleted_ActionMethod(deleted),
          PropertyValue_ActionMethod(property_value, property),
        });
        // clang-format on
      });
    }
  }

  if (!exists || deleted || property_value.IsNull()) {
    return;
  }

  index_accessor.insert({std::move(property_value), &vertex, tx.start_timestamp});
  if (snapshot_info) {
    snapshot_info->Update(UpdateType::VERTICES);
  }
}

void AdvanceUntilValid_(auto &index_iterator, auto end, Vertex *&current_vertex, VertexAccessor &current_accessor,
                        Storage *storage, Transaction *transaction, View view, PropertyId property,
                        std::optional<utils::Bound<PropertyValue>> const &lower_bound,
                        std::optional<utils::Bound<PropertyValue>> const &upper_bound, Gid max_gid) {
  for (; index_iterator != end; ++index_iterator) {
    if (index_iterator->vertex == current_vertex) {
      continue;
    }

    if (!IsValueIncludedByLowerBound(index_iterator->value, lower_bound)) {
      continue;
    }

    if (!IsValueIncludedByUpperBound(index_iterator->value, upper_bound)) {
      index_iterator = end;
      break;
    }

    if (index_iterator->vertex->gid >= max_gid) {
      continue;
    }

    if (!CanSeeEntityWithTimestamp(index_iterator->timestamp, transaction, view)) {
      continue;
    }

    if (!CurrentVersionHasProperty(*index_iterator->vertex, property, index_iterator->value, transaction, view)) {
      continue;
    }

    auto accessor = VertexAccessor{index_iterator->vertex, storage, transaction};
    if (!accessor.IsVisible(view)) {
      continue;
    }

    current_vertex = index_iterator->vertex;
    current_accessor = accessor;
    break;
  }
}
}  // namespace

InMemoryVertexPropertyIndex::IndividualIndex::~IndividualIndex() = default;

void InMemoryVertexPropertyIndex::IndividualIndex::Publish(uint64_t commit_timestamp, metrics::GaugeHandle gauge) {
  status_.Commit(commit_timestamp);
  gauge_ = metrics::ScopedGauge{gauge.gauge};
}

bool InMemoryVertexPropertyIndex::InstallIndividualIndex_(PropertyId property, std::shared_ptr<IndividualIndex> entry,
                                                          ActiveIndicesUpdater const &updater,
                                                          bool register_in_all_indices) {
  return index_.WithLock([&](std::shared_ptr<IndicesContainer const> &indices_container) {
    if (indices_container->indices_.find(property) != indices_container->indices_.cend()) return false;
    utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
    auto new_container = std::make_shared<IndicesContainer>(*indices_container);
    auto [new_it, _] = new_container->indices_.emplace(property, std::move(entry));

    if (register_in_all_indices) {
      // NOLINTNEXTLINE(clang-analyzer-core.CallAndMessage)
      all_indices_.WithLock([&](auto &all_indices) {
        auto new_all_indices = *all_indices;
        // NOLINTNEXTLINE(clang-analyzer-core.CallAndMessage)
        new_all_indices.emplace_back(property, new_it->second);
        all_indices = std::make_shared<std::vector<AllIndicesEntry>>(std::move(new_all_indices));
      });
    }
    indices_container = std::move(new_container);
    updater(std::make_shared<ActiveIndices>(indices_container));
    return true;
  });
}

bool InMemoryVertexPropertyIndex::RegisterIndex(PropertyId property, ActiveIndicesUpdater const &updater) {
  return InstallIndividualIndex_(property,
                                 std::make_shared<IndividualIndex>(),
                                 updater,
                                 /*register_in_all_indices=*/true);
}

bool InMemoryVertexPropertyIndex::PublishIndex(PropertyId property, uint64_t commit_timestamp) {
  auto index = GetIndividualIndex(property);
  if (!index) return false;
  index->Publish(commit_timestamp, gauge_);
  return true;
}

bool InMemoryVertexPropertyIndex::CreateIndexOnePass(PropertyId property, utils::SkipListDb<Vertex>::Accessor vertices,
                                                     ActiveIndicesUpdater const &updater,
                                                     std::optional<SnapshotObserverInfo> const &snapshot_info) {
  auto res = RegisterIndex(property, updater);
  if (!res) return false;
  auto res2 = PopulateIndex(property, std::move(vertices), updater, snapshot_info);
  if (!res2) {
    MG_ASSERT(false, "Index population can't fail, there was no cancellation callback.");
  }
  return PublishIndex(property, 0);
}

auto InMemoryVertexPropertyIndex::PopulateIndex(PropertyId property, utils::SkipListDb<Vertex>::Accessor vertices,
                                                ActiveIndicesUpdater const &updater,
                                                std::optional<SnapshotObserverInfo> const &snapshot_info,
                                                Transaction const *tx, CheckCancelFunction cancel_check)
    -> std::expected<void, IndexPopulateError> {
  auto index = GetIndividualIndex(property);
  if (!index) {
    MG_ASSERT(false, "It should not be possible to remove the index before populating it.");
  }

  try {
    auto const accessor_factory = [&] { return index->skip_list_.access(); };
    if (tx) {
      auto const insert_function = [&](Vertex &vertex, auto &index_accessor) {
        TryInsertVertexPropertyIndex(vertex, property, index_accessor, snapshot_info, *tx);
      };
      PopulateIndexDispatch(
          vertices, accessor_factory, insert_function, std::move(cancel_check), {} /*TODO: parallel*/);
    } else {
      auto const insert_function = [&](Vertex &vertex, auto &index_accessor) {
        TryInsertVertexPropertyIndex(vertex, property, index_accessor, snapshot_info);
      };
      PopulateIndexDispatch(
          vertices, accessor_factory, insert_function, std::move(cancel_check), {} /*TODO: parallel*/);
    }
  } catch (PopulateCancel const &) {
    (void)DropIndex(property, updater);
    return std::unexpected{IndexPopulateError::Cancellation};
  } catch (utils::OutOfMemoryException const &) {
    (void)DropIndex(property, updater);
    throw;
  }
  return {};
}

auto InMemoryVertexPropertyIndex::DropIndex(PropertyId property, ActiveIndicesUpdater const &updater)
    -> std::shared_ptr<IndividualIndex> {
  auto evicted = index_.WithLock(
      [&](std::shared_ptr<IndicesContainer const> &indices_container) -> std::shared_ptr<IndividualIndex> {
        auto const it = indices_container->indices_.find(property);
        if (it == indices_container->indices_.cend()) return {};
        auto evicted_entry = it->second;

        auto new_container = std::make_shared<IndicesContainer>();
        for (auto const &[existing_property, index] : indices_container->indices_) {
          if (existing_property != property) {
            new_container->indices_.emplace(existing_property, index);
          }
        }
        indices_container = new_container;
        updater(std::make_shared<ActiveIndices>(indices_container));
        return evicted_entry;
      });
  CleanupAllIndices();
  return evicted;
}

void InMemoryVertexPropertyIndex::RestoreIndex(PropertyId property, std::shared_ptr<IndividualIndex> evicted,
                                               ActiveIndicesUpdater const &updater) {
  if (!evicted) return;
  (void)InstallIndividualIndex_(property, std::move(evicted), updater, /*register_in_all_indices=*/false);
}

bool InMemoryVertexPropertyIndex::ActiveIndices::IndexExists(PropertyId property) const {
  return index_container_->indices_.contains(property);
}

bool InMemoryVertexPropertyIndex::ActiveIndices::IndexReady(PropertyId property) const {
  auto const &indices = index_container_->indices_;
  auto it = indices.find(property);
  if (it == indices.end()) return false;
  return it->second->status_.IsReady();
}

std::vector<PropertyId> InMemoryVertexPropertyIndex::ActiveIndices::ListIndices(uint64_t start_timestamp) const {
  auto ret = std::vector<PropertyId>{};
  ret.reserve(index_container_->indices_.size());
  for (auto const &[property, index] : index_container_->indices_) {
    if (index->status_.IsVisible(start_timestamp)) {
      ret.emplace_back(property);
    }
  }
  return ret;
}

void InMemoryVertexPropertyIndex::ActiveIndices::UpdateOnSetProperty(PropertyId property, PropertyValue value,
                                                                     Vertex *vertex, uint64_t timestamp) {
  if (value.IsNull()) {
    return;
  }

  auto it = index_container_->indices_.find(property);
  if (it == index_container_->indices_.end()) return;

  auto acc = it->second->skip_list_.access();
  acc.insert({std::move(value), vertex, timestamp});
}

uint64_t InMemoryVertexPropertyIndex::ActiveIndices::ApproximateVertexCount(PropertyId property) const {
  if (auto it = index_container_->indices_.find(property); it != index_container_->indices_.end()) {
    return it->second->skip_list_.size();
  }
  return 0U;
}

uint64_t InMemoryVertexPropertyIndex::ActiveIndices::ApproximateVertexCount(PropertyId property,
                                                                            PropertyValue const &value) const {
  auto it = index_container_->indices_.find(property);
  MG_ASSERT(it != index_container_->indices_.end(), "Index for vertex property {} doesn't exist", property.AsUint());
  auto acc = it->second->skip_list_.access();
  if (!value.IsNull()) {
    // NOLINTNEXTLINE(bugprone-narrowing-conversions,cppcoreguidelines-narrowing-conversions)
    return acc.estimate_count(value, utils::SkipListLayerForCountEstimation(acc.size()));
  }
  return acc.estimate_average_number_of_equals(
      [](auto const &first, auto const &second) { return first.value == second.value; },
      // NOLINTNEXTLINE(bugprone-narrowing-conversions,cppcoreguidelines-narrowing-conversions)
      utils::SkipListLayerForAverageEqualsEstimation(acc.size()));
}

uint64_t InMemoryVertexPropertyIndex::ActiveIndices::ApproximateVertexCount(
    PropertyId property, std::optional<utils::Bound<PropertyValue>> const &lower,
    std::optional<utils::Bound<PropertyValue>> const &upper) const {
  auto it = index_container_->indices_.find(property);
  MG_ASSERT(it != index_container_->indices_.end(), "Index for vertex property {} doesn't exist", property.AsUint());
  auto acc = it->second->skip_list_.access();
  // NOLINTNEXTLINE(bugprone-narrowing-conversions,cppcoreguidelines-narrowing-conversions)
  return acc.estimate_range_count(lower, upper, utils::SkipListLayerForCountEstimation(acc.size()));
}

void InMemoryVertexPropertyIndex::RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp, std::stop_token token) {
  auto maybe_stop = utils::ResettableCounter(2048);

  CleanupAllIndices();

  auto cpy = all_indices_.ReadCopy();

  for (auto &[property_id, index] : *cpy) {
    if (token.stop_requested()) return;

    auto acc = index->skip_list_.access();
    for (auto it = acc.begin(); it != acc.end();) {
      if (maybe_stop() && token.stop_requested()) return;

      auto next_it = it;
      ++next_it;

      if (it->timestamp >= oldest_active_start_timestamp) {
        it = next_it;
        continue;
      }

      bool const has_next = next_it != acc.end();

      bool const redundant_duplicate = has_next && it->value == next_it->value && it->vertex == next_it->vertex;
      if (redundant_duplicate ||
          !AnyVersionHasProperty(*it->vertex, property_id, it->value, oldest_active_start_timestamp)) {
        acc.remove(*it);
      }

      it = next_it;
    }
  }
}

void InMemoryVertexPropertyIndex::DropGraphClearIndices() {
  index_.WithLock([](std::shared_ptr<IndicesContainer const> &index) { index = std::make_shared<IndicesContainer>(); });
  all_indices_.WithLock([](std::shared_ptr<std::vector<AllIndicesEntry> const> &all_indices) {
    all_indices = std::make_shared<std::vector<AllIndicesEntry>>();
  });
}

void InMemoryVertexPropertyIndex::RunGC() {
  CleanupAllIndices();

  auto cpy = all_indices_.ReadCopy();
  for (auto &[_, index] : *cpy) {
    index->skip_list_.run_gc();
  }
}

InMemoryVertexPropertyIndex::Iterable::Iterable(
    utils::SkipListDb<InMemoryVertexPropertyIndex::Entry>::Accessor index_accessor,
    utils::SkipListDb<Vertex>::ConstAccessor vertex_accessor, PropertyId property,
    std::optional<utils::Bound<PropertyValue>> const &lower_bound,
    std::optional<utils::Bound<PropertyValue>> const &upper_bound, View view, Storage *storage,
    Transaction *transaction, Gid max_gid)
    : pin_accessor_(std::move(vertex_accessor)),
      index_accessor_(std::move(index_accessor)),
      property_(property),
      lower_bound_(lower_bound),
      upper_bound_(upper_bound),
      bounds_valid_(ValidateBounds(lower_bound_, upper_bound_)),
      view_(view),
      storage_(storage),
      transaction_(transaction),
      max_gid_(max_gid) {}

InMemoryVertexPropertyIndex::Iterable::Iterator::Iterator(
    Iterable *self, utils::SkipListDb<InMemoryVertexPropertyIndex::Entry>::Iterator index_iterator)
    : self_(self), index_iterator_(index_iterator), current_accessor_(nullptr, self_->storage_, nullptr) {
  AdvanceUntilValid();
}

InMemoryVertexPropertyIndex::Iterable::Iterator &InMemoryVertexPropertyIndex::Iterable::Iterator::operator++() {
  ++index_iterator_;
  AdvanceUntilValid();
  return *this;
}

void InMemoryVertexPropertyIndex::Iterable::Iterator::AdvanceUntilValid() {
  AdvanceUntilValid_(index_iterator_,
                     self_->index_accessor_.end(),
                     current_vertex_,
                     current_accessor_,
                     self_->storage_,
                     self_->transaction_,
                     self_->view_,
                     self_->property_,
                     self_->lower_bound_,
                     self_->upper_bound_,
                     self_->max_gid_);
}

InMemoryVertexPropertyIndex::Iterable InMemoryVertexPropertyIndex::ActiveIndices::Vertices(
    PropertyId property, utils::SkipListDb<Vertex>::ConstAccessor vertex_accessor,
    std::optional<utils::Bound<PropertyValue>> const &lower_bound,
    std::optional<utils::Bound<PropertyValue>> const &upper_bound, View view, Storage *storage,
    Transaction *transaction) {
  auto it = index_container_->indices_.find(property);
  MG_ASSERT(it != index_container_->indices_.end(), "Index for vertex property {} doesn't exist", property.AsUint());
  auto const max_gid = Gid::FromUint(storage->vertex_id_.load(std::memory_order_acquire));
  return {it->second->skip_list_.access(),
          std::move(vertex_accessor),
          property,
          lower_bound,
          upper_bound,
          view,
          storage,
          transaction,
          max_gid};
}

InMemoryVertexPropertyIndex::ChunkedIterable InMemoryVertexPropertyIndex::ActiveIndices::ChunkedVertices(
    PropertyId property, utils::SkipListDb<Vertex>::ConstAccessor vertex_accessor,
    std::optional<utils::Bound<PropertyValue>> const &lower_bound,
    std::optional<utils::Bound<PropertyValue>> const &upper_bound, View view, Storage *storage,
    Transaction *transaction, size_t num_chunks) {
  auto it = index_container_->indices_.find(property);
  MG_ASSERT(it != index_container_->indices_.end(), "Index for vertex property {} doesn't exist", property.AsUint());
  auto const max_gid = Gid::FromUint(storage->vertex_id_.load(std::memory_order_acquire));
  return {it->second->skip_list_.access(),
          std::move(vertex_accessor),
          property,
          lower_bound,
          upper_bound,
          view,
          storage,
          transaction,
          num_chunks,
          max_gid};
}

VertexPropertyIndex::AbortProcessor InMemoryVertexPropertyIndex::ActiveIndices::GetAbortProcessor() const {
  auto property_ids_filter = index_container_->indices_ | std::views::keys | ranges::to_vector;
  return AbortProcessor{property_ids_filter};
}

void InMemoryVertexPropertyIndex::ActiveIndices::AbortEntries(VertexPropertyIndex::AbortableInfo const &info,
                                                              uint64_t start_timestamp) {
  for (auto const &[property, vertices] : info) {
    auto const it = index_container_->indices_.find(property);
    DMG_ASSERT(it != index_container_->indices_.end());

    auto acc = it->second->skip_list_.access();
    for (auto const &[value, vertex] : vertices) {
      acc.remove(Entry{value, vertex, start_timestamp});
    }
  }
}

std::shared_ptr<VertexPropertyIndex::ActiveIndices> InMemoryVertexPropertyIndex::GetActiveIndices() const {
  return std::make_shared<ActiveIndices>(index_.ReadCopy());
}

auto InMemoryVertexPropertyIndex::GetIndividualIndex(PropertyId property) const -> std::shared_ptr<IndividualIndex> {
  return index_.WithReadLock(
      [&](std::shared_ptr<IndicesContainer const> const &index) -> std::shared_ptr<IndividualIndex> {
        auto it = index->indices_.find(property);
        if (it == index->indices_.cend()) [[unlikely]]
          return {};
        return it->second;
      });
}

void InMemoryVertexPropertyIndex::CleanupAllIndices() {
  all_indices_.WithLock([](std::shared_ptr<std::vector<AllIndicesEntry> const> &indices) {
    auto keep_condition = [](AllIndicesEntry const &entry) { return entry.second.use_count() != 1; };
    if (!r::all_of(*indices, keep_condition)) {
      indices = std::make_shared<std::vector<AllIndicesEntry>>(*indices | rv::filter(keep_condition) | r::to_vector);
    }
  });
}

InMemoryVertexPropertyIndex::ChunkedIterable::ChunkedIterable(
    utils::SkipListDb<InMemoryVertexPropertyIndex::Entry>::Accessor index_accessor,
    utils::SkipListDb<Vertex>::ConstAccessor vertex_accessor, PropertyId property,
    std::optional<utils::Bound<PropertyValue>> const &lower_bound,
    std::optional<utils::Bound<PropertyValue>> const &upper_bound, View view, Storage *storage,
    Transaction *transaction, size_t num_chunks, Gid max_gid)
    : pin_accessor_(std::move(vertex_accessor)),
      index_accessor_(std::move(index_accessor)),
      property_(property),
      lower_bound_(lower_bound),
      upper_bound_(upper_bound),
      bounds_valid_(ValidateBounds(lower_bound_, upper_bound_)),
      view_(view),
      storage_(storage),
      transaction_(transaction),
      max_gid_(max_gid) {
  if (!bounds_valid_) return;

  auto const lower_bound_pv = lower_bound_ ? std::optional<PropertyValue>{lower_bound_->value()} : std::nullopt;
  auto const upper_bound_pv = upper_bound_ ? std::optional<PropertyValue>{upper_bound_->value()} : std::nullopt;
  chunks_ = index_accessor_.create_chunks(num_chunks, lower_bound_pv, upper_bound_pv);

  RechunkIndex<utils::SkipListDb<Entry>>(
      chunks_, [](auto const &a, auto const &b) { return a.vertex == b.vertex && a.value == b.value; });
}

void InMemoryVertexPropertyIndex::ChunkedIterable::Iterator::AdvanceUntilValid() {
  AdvanceUntilValid_(index_iterator_,
                     utils::SkipListDb<InMemoryVertexPropertyIndex::Entry>::ChunkedIterator{},
                     current_vertex_,
                     current_accessor_,
                     self_->storage_,
                     self_->transaction_,
                     self_->view_,
                     self_->property_,
                     self_->lower_bound_,
                     self_->upper_bound_,
                     self_->max_gid_);
}

}  // namespace memgraph::storage
