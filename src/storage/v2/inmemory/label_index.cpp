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

#include "storage/v2/inmemory/label_index.hpp"

#include "storage/v2/indices/indices_utils.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "utils/counter.hpp"

namespace r = ranges;
namespace rv = r::views;
namespace memgraph::storage {

bool InMemoryLabelIndex::RegisterIndex(LabelId label) {
  return index_.WithLock([&](std::shared_ptr<const IndexContainer> &index) {
    auto const &indices = *index;
    {
      auto it = index->find(label);
      if (it != indices.end()) return false;  // already exists
    }

    auto new_index = std::make_shared<IndexContainer>(indices);
    auto [new_it, _] = new_index->try_emplace(label, std::make_shared<IndividualIndex>());

    utils::MemoryTracker::OutOfMemoryExceptionBlocker oom_blocker;

    all_indices_.WithLock([&](auto &all_indices) {
      auto new_all_indices = *all_indices;
      // NOLINTNEXTLINE(clang-analyzer-core.CallAndMessage)
      new_all_indices.emplace_back(new_it->second, label);
      all_indices = std::make_shared<std::vector<AllIndicesEntry>>(std::move(new_all_indices));
    });

    index = std::move(new_index);
    return true;
  });
}

auto InMemoryLabelIndex::GetIndividualIndex(LabelId label) const -> std::shared_ptr<IndividualIndex> {
  return index_.WithReadLock(
      [&](std::shared_ptr<IndexContainer const> const &index) -> std::shared_ptr<IndividualIndex> {
        auto it1 = index->find(label);
        if (it1 == index->cend()) [[unlikely]]
          return {};
        return it1->second;
      });
}

auto InMemoryLabelIndex::PublishIndex(LabelId label, uint64_t commit_timestamp) -> bool {
  auto index = GetIndividualIndex(label);
  if (!index) return false;
  index->Publish(commit_timestamp);
  return true;
}

void InMemoryLabelIndex::IndividualIndex::Publish(uint64_t commit_timestamp) {
  status.Commit(commit_timestamp);
  memgraph::metrics::IncrementCounter(memgraph::metrics::ActiveLabelIndices);
}

InMemoryLabelIndex::IndividualIndex::~IndividualIndex() {
  if (status.IsReady()) {
    memgraph::metrics::DecrementCounter(memgraph::metrics::ActiveLabelIndices);
  }
}

inline void TryInsertLabelPropertiesIndex(Vertex &vertex, LabelId label, auto &&index_accessor,
                                          std::optional<SnapshotObserverInfo> const &snapshot_info) {
  // observe regardless
  if (snapshot_info) {
    snapshot_info->Update(UpdateType::VERTICES);
  }

  if (vertex.deleted || !utils::Contains(vertex.labels, label)) {
    return;
  }

  // Using 0 as a timestamp is fine because the index is created at timestamp x
  // and any query using the index will be > x.
  index_accessor.insert({&vertex, 0});
}

inline void TryInsertLabelIndex(Vertex &vertex, LabelId label, auto &&index_accessor,
                                std::optional<SnapshotObserverInfo> const &snapshot_info, Transaction const &tx) {
  // observe regardless
  if (snapshot_info) {
    snapshot_info->Update(UpdateType::VERTICES);
  }

  bool exists = true;
  bool deleted = false;
  Delta *delta = nullptr;
  bool has_label = false;
  {
    auto guard = std::shared_lock{vertex.lock};
    deleted = vertex.deleted;
    delta = vertex.delta;
    has_label = utils::Contains(vertex.labels, label);
  }
  // Create and drop index will always use snapshot isolation
  if (delta) {
    ApplyDeltasForRead(&tx, delta, View::OLD, [&](const Delta &delta) {
      // clang-format off
      DeltaDispatch(delta, utils::ChainedOverloaded{
        Exists_ActionMethod(exists),
        Deleted_ActionMethod(deleted),
        HasLabel_ActionMethod(has_label, label),
      });
      // clang-format on
    });
  }
  if (!exists || deleted || !has_label) {
    return;
  }

  index_accessor.insert({&vertex, tx.start_timestamp});
}

auto InMemoryLabelIndex::PopulateIndex(
    LabelId label, utils::SkipList<Vertex>::Accessor vertices,
    const std::optional<durability::ParallelizedSchemaCreationInfo> &parallel_exec_info,
    std::optional<SnapshotObserverInfo> const &snapshot_info, Transaction const *tx, CheckCancelFunction cancel_check)
    -> utils::BasicResult<IndexPopulateError> {
  auto index = GetIndividualIndex(label);
  if (!index) {
    MG_ASSERT(false, "It should not be possible to remove the index before populating it.");
  }

  spdlog::trace("Vertices size when creating index: {}", vertices.size());

  try {
    auto const accessor_factory = [&] { return index->skiplist.access(); };

    if (tx) {
      // If we are in a transaction, we need to read the object with the correct MVCC snapshot isolation
      auto const try_insert_into_index = [&](Vertex &vertex, auto &index_accessor) {
        TryInsertLabelIndex(vertex, label, index_accessor, snapshot_info, *tx);
      };
      PopulateIndexDispatch(vertices, accessor_factory, try_insert_into_index, std::move(cancel_check),
                            parallel_exec_info);
    } else {
      // If we are not in a transaction, we need to read the object as it is. (post recovery)
      auto const try_insert_into_index = [&](Vertex &vertex, auto &index_accessor) {
        TryInsertLabelPropertiesIndex(vertex, label, index_accessor, snapshot_info);
      };
      PopulateIndexDispatch(vertices, accessor_factory, try_insert_into_index, std::move(cancel_check),
                            parallel_exec_info);
    }
  } catch (const PopulateCancel &) {
    DropIndex(label);
    return IndexPopulateError::Cancellation;
  } catch (const utils::OutOfMemoryException &) {
    DropIndex(label);
    throw;
  }

  return {};
}

auto InMemoryLabelIndex::GetActiveIndices() const -> std::unique_ptr<LabelIndex::ActiveIndices> {
  return std::make_unique<ActiveIndices>(index_.WithReadLock(std::identity{}));
}

bool InMemoryLabelIndex::CreateIndexOnePass(
    LabelId label, utils::SkipList<Vertex>::Accessor vertices,
    const std::optional<durability::ParallelizedSchemaCreationInfo> &parallel_exec_info,
    std::optional<SnapshotObserverInfo> const &snapshot_info) {
  auto res = RegisterIndex(label);
  if (!res) return false;
  auto res2 = PopulateIndex(label, std::move(vertices), parallel_exec_info, snapshot_info);
  if (res2.HasError()) {
    MG_ASSERT(false, "Index population can't fail, there was no cancellation callback.");
  }
  return PublishIndex(label, 0);
}

void InMemoryLabelIndex::ActiveIndices::UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update,
                                                         const Transaction &tx) {
  auto it = index_container_->find(added_label);
  if (it == index_container_->end()) return;
  auto acc = it->second->skiplist.access();
  acc.insert(Entry{vertex_after_update, tx.start_timestamp});
}

bool InMemoryLabelIndex::DropIndex(LabelId label) {
  auto result = index_.WithLock([&](std::shared_ptr<IndexContainer const> &index) -> bool {
    {
      auto it = index->find(label);
      if (it == index->end()) [[unlikely]] {
        return false;
      }
    }
    auto new_index = std::make_shared<IndexContainer>(*index);
    new_index->erase(label);
    index = std::move(new_index);
    return true;
  });
  CleanupAllIndices();
  return result;
}

bool InMemoryLabelIndex::ActiveIndices::IndexRegistered(LabelId label) const {
  return index_container_->find(label) != index_container_->end();
}

bool InMemoryLabelIndex::ActiveIndices::IndexReady(LabelId label) const {
  auto it = index_container_->find(label);
  if (it == index_container_->end()) [[unlikely]] {
    return false;
  }
  return it->second->status.IsReady();
}

std::vector<LabelId> InMemoryLabelIndex::ActiveIndices::ListIndices(uint64_t start_timestamp) const {
  std::vector<LabelId> ret;
  ret.reserve(index_container_->size());
  for (const auto &item : *index_container_) {
    if (item.second->status.IsVisible(start_timestamp)) ret.push_back(item.first);
  }
  return ret;
}

void InMemoryLabelIndex::RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp, std::stop_token token) {
  CleanupAllIndices();
  auto maybe_stop = utils::ResettableCounter(2048);
  auto index_container = all_indices_.WithReadLock(std::identity{});

  for (auto &[index, label] : *index_container) {
    // before starting index, check if stop_requested
    if (token.stop_requested()) return;

    auto vertices_acc = index->skiplist.access();
    for (auto it = vertices_acc.begin(); it != vertices_acc.end();) {
      // Hot loop, don't check stop_requested every time
      if (maybe_stop() && token.stop_requested()) return;

      auto next_it = it;
      ++next_it;

      if (it->timestamp >= oldest_active_start_timestamp) {
        it = next_it;
        continue;
      }

      if ((next_it != vertices_acc.end() && it->vertex == next_it->vertex) ||
          !AnyVersionHasLabel(*it->vertex, label, oldest_active_start_timestamp)) {
        vertices_acc.remove(*it);
      }

      it = next_it;
    }
  }
}

void InMemoryLabelIndex::ActiveIndices::AbortEntries(LabelIndex::AbortableInfo const &info,
                                                     uint64_t exact_start_timestamp) {
  for (auto const &[label, to_remove] : info) {
    auto it = index_container_->find(label);
    DMG_ASSERT(it != index_container_->end());
    auto acc = it->second->skiplist.access();
    for (auto vertex : to_remove) {
      acc.remove(Entry{vertex, exact_start_timestamp});
    }
  }
}

InMemoryLabelIndex::Iterable::Iterable(utils::SkipList<Entry>::Accessor index_accessor,
                                       utils::SkipList<Vertex>::ConstAccessor vertices_accessor, LabelId label,
                                       View view, Storage *storage, Transaction *transaction)
    : pin_accessor_(std::move(vertices_accessor)),
      index_accessor_(std::move(index_accessor)),
      label_(label),
      view_(view),
      storage_(storage),
      transaction_(transaction) {}

InMemoryLabelIndex::Iterable::Iterator::Iterator(Iterable *self, utils::SkipList<Entry>::Iterator index_iterator)
    : self_(self),
      index_iterator_(index_iterator),
      current_vertex_accessor_(nullptr, self_->storage_, nullptr),
      current_vertex_(nullptr) {
  AdvanceUntilValid();
}

InMemoryLabelIndex::Iterable::Iterator &InMemoryLabelIndex::Iterable::Iterator::operator++() {
  ++index_iterator_;
  AdvanceUntilValid();
  return *this;
}

void InMemoryLabelIndex::Iterable::Iterator::AdvanceUntilValid() {
  for (; index_iterator_ != self_->index_accessor_.end(); ++index_iterator_) {
    if (index_iterator_->vertex == current_vertex_) {
      continue;
    }

    if (!CanSeeEntityWithTimestamp(index_iterator_->timestamp, self_->transaction_, self_->view_)) {
      continue;
    }

    auto accessor = VertexAccessor{index_iterator_->vertex, self_->storage_, self_->transaction_};
    auto res = accessor.HasLabel(self_->label_, self_->view_);
    if (!res.HasError() and res.GetValue()) {
      current_vertex_ = accessor.vertex_;
      current_vertex_accessor_ = accessor;
      break;
    }
  }
}

uint64_t InMemoryLabelIndex::ActiveIndices::ApproximateVertexCount(LabelId label) const {
  auto it = index_container_->find(label);
  MG_ASSERT(it != index_container_->end(), "Index for label {} doesn't exist", label.AsUint());
  return it->second->skiplist.size();
}

void InMemoryLabelIndex::RunGC() {
  CleanupAllIndices();
  auto cpy = all_indices_.WithReadLock(std::identity{});
  for (auto &[index, _] : *cpy) {
    index->skiplist.run_gc();
  }
}

InMemoryLabelIndex::Iterable InMemoryLabelIndex::ActiveIndices::Vertices(LabelId label, View view, Storage *storage,
                                                                         Transaction *transaction) {
  DMG_ASSERT(storage->storage_mode_ == StorageMode::IN_MEMORY_TRANSACTIONAL ||
                 storage->storage_mode_ == StorageMode::IN_MEMORY_ANALYTICAL,
             "LabelIndex trying to access InMemory vertices from OnDisk!");
  auto vertices_acc = static_cast<InMemoryStorage const *>(storage)->vertices_.access();
  const auto it = index_container_->find(label);
  MG_ASSERT(it != index_container_->end(), "Index for label {} doesn't exist", label.AsUint());
  return {it->second->skiplist.access(), std::move(vertices_acc), label, view, storage, transaction};
}

InMemoryLabelIndex::Iterable InMemoryLabelIndex::ActiveIndices::Vertices(
    LabelId label, memgraph::utils::SkipList<memgraph::storage::Vertex>::ConstAccessor vertices_acc, View view,
    Storage *storage, Transaction *transaction) {
  const auto it = index_container_->find(label);
  MG_ASSERT(it != index_container_->end(), "Index for label {} doesn't exist", label.AsUint());
  return {it->second->skiplist.access(), std::move(vertices_acc), label, view, storage, transaction};
}

void InMemoryLabelIndex::SetIndexStats(const storage::LabelId &label, const storage::LabelIndexStats &stats) {
  auto locked_stats = stats_.Lock();
  locked_stats->insert_or_assign(label, stats);
}

std::optional<LabelIndexStats> InMemoryLabelIndex::GetIndexStats(const storage::LabelId &label) const {
  auto locked_stats = stats_.ReadLock();
  if (auto it = locked_stats->find(label); it != locked_stats->end()) {
    return it->second;
  }
  return {};
}

std::vector<LabelId> InMemoryLabelIndex::ClearIndexStats() {
  std::vector<LabelId> deleted_indexes;
  auto locked_stats = stats_.Lock();
  deleted_indexes.reserve(locked_stats->size());
  std::transform(locked_stats->begin(), locked_stats->end(), std::back_inserter(deleted_indexes),
                 [](const auto &elem) { return elem.first; });
  locked_stats->clear();
  return deleted_indexes;
}

// stats_ is a map with label as the key, so only one can exist at a time
bool InMemoryLabelIndex::DeleteIndexStats(const storage::LabelId &label) {
  auto locked_stats = stats_.Lock();
  for (auto it = locked_stats->cbegin(); it != locked_stats->cend(); ++it) {
    if (it->first == label) {
      locked_stats->erase(it);
      return true;
    }
  }
  return false;
}

LabelIndex::AbortProcessor InMemoryLabelIndex::ActiveIndices::GetAbortProcessor() const {
  std::vector<LabelId> res;
  res.reserve(index_container_->size());
  for (const auto &[label, _] : *index_container_) {
    res.emplace_back(label);
  }
  return LabelIndex::AbortProcessor{res};
}

void InMemoryLabelIndex::DropGraphClearIndices() {
  index_.WithLock([](auto &idx) { idx = std::make_shared<IndexContainer>(); });
  stats_->clear();
  all_indices_.WithLock([](std::shared_ptr<std::vector<AllIndicesEntry> const> &all_indices) {
    all_indices = std::make_unique<std::vector<AllIndicesEntry>>();
  });
}

void InMemoryLabelIndex::CleanupAllIndices() {
  // By cleanup, we mean just cleanup of the all_indexes_
  // If all_indexes_ is the only thing holding onto an IndividualIndex, we remove it
  all_indices_.WithLock([](std::shared_ptr<std::vector<AllIndicesEntry> const> &indices) {
    auto keep_condition = [](AllIndicesEntry const &entry) { return entry.index_.use_count() != 1; };
    if (!ranges::all_of(*indices, keep_condition)) {
      indices = std::make_shared<std::vector<AllIndicesEntry>>(*indices | rv::filter(keep_condition) | r::to_vector);
    }
  });
}

}  // namespace memgraph::storage
