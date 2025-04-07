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

#include <span>

#include "storage/v2/indices/indices_utils.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "utils/counter.hpp"
#include "utils/logging.hpp"

namespace memgraph::storage {

namespace {
template <typename I>
auto IndexOptReadLock(I &index) {
  // We only need to read the label to index map; index itself is threadsafe
  // NOTE: Index shouldn't be used while it's being populated
  std::optional<typename I::MutableSharedLockPtr> locked_index = index.MutableSharedLock();
  return std::move(locked_index);
}
}  // namespace

void InMemoryLabelIndex::UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update, const Transaction &tx) {
  auto locked_index = IndexOptReadLock(index_);  // Should be READ
  auto it = locked_index.value()->find(added_label);
  if (it == locked_index.value()->end()) return;
  DMG_ASSERT(it->second.state == READY, "Index should be ready when updating label");
  // Eagerly unlock; higher level logic protects us from index drops
  locked_index.reset();
  auto acc = it->second.index.access();
  acc.insert(Entry{vertex_after_update, tx.start_timestamp});
}

utils::BasicResult<StorageIndexDefinitionError, void> InMemoryLabelIndex::CreateIndex(
    LabelId label, utils::SkipList<Vertex>::Accessor vertices,
    const std::optional<durability::ParallelizedSchemaCreationInfo> &parallel_exec_info,
    std::optional<SnapshotObserverInfo> const &snapshot_info) {
  const auto create_index_seq = [this, &snapshot_info](LabelId label, utils::SkipList<Vertex>::Accessor &vertices,
                                                       std::map<LabelId, IndexElem<Entry>>::iterator it) {
    using IndexAccessor = decltype(it->second.index.access());

    CreateIndexOnSingleThread(
        vertices, it->second.index, index_, label,
        [](Vertex &vertex, LabelId label, IndexAccessor &index_accessor) {
          TryInsertLabelIndex(vertex, label, index_accessor);
        },
        snapshot_info);
  };

  const auto create_index_par = [this, &snapshot_info](
                                    LabelId label, utils::SkipList<Vertex>::Accessor &vertices,
                                    std::map<LabelId, IndexElem<Entry>>::iterator label_it,
                                    const durability::ParallelizedSchemaCreationInfo &parallel_exec_info) {
    using IndexAccessor = decltype(label_it->second.index.access());

    CreateIndexOnMultipleThreads(
        vertices, label_it->second.index, index_, label, parallel_exec_info,
        [](Vertex &vertex, LabelId label, IndexAccessor &index_accessor) {
          TryInsertLabelIndex(vertex, label, index_accessor);
        },
        snapshot_info);
  };

  // NOTE: No need to hold the lock after emplace; map has stable references and the individual index is thread safe
  auto [it, emplaced] =
      index_->emplace(std::piecewise_construct, std::forward_as_tuple(label), std::forward_as_tuple());
  if (!emplaced) {
    return it->second.state != READY ? StorageIndexDefinitionError{IndexIncompleteError{}}
                                     : StorageIndexDefinitionError{IndexDefinitionError{}};
  }

  if (parallel_exec_info) {
    create_index_par(label, vertices, it, *parallel_exec_info);
  } else {
    create_index_seq(label, vertices, it);
  }

  // Signal that the index is ready for use
  it->second.state = READY;
  return {};
}

bool InMemoryLabelIndex::DropIndex(LabelId label) {
  // NOTE: Drop index is still unique access
  return index_->erase(label) > 0;
}

// NOTE: Used to figure out which index to use; hide as long as index is not ready/populated
bool InMemoryLabelIndex::IndexExists(LabelId label) const {
  auto locked_index = index_.ReadLock();
  auto it = locked_index->find(label);
  return it != locked_index->end() && it->second.state == READY;
}

std::vector<LabelId> InMemoryLabelIndex::ListIndices() const {
  std::vector<LabelId> ret;
  auto locked_index = index_.ReadLock();
  ret.reserve(locked_index->size());
  for (const auto &item : *locked_index) {
    if (item.second.state != READY) continue;  // TODO Give user some indication the index is there but not ready
    ret.push_back(item.first);
  }
  return ret;
}

void InMemoryLabelIndex::RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp, std::stop_token token) {
  auto maybe_stop = utils::ResettableCounter<2048>();

  // TODO Another thread safe structure could allow us to loop and remove without blocking any insertions
  auto locked_index = index_.MutableSharedLock();
  for (auto &label_storage : *locked_index) {
    // before starting index, check if stop_requested
    if (token.stop_requested()) return;
    if (label_storage.second.state != READY) continue;

    auto vertices_acc = label_storage.second.index.access();
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
          !AnyVersionHasLabel(*it->vertex, label_storage.first, oldest_active_start_timestamp)) {
        vertices_acc.remove(*it);
      }

      it = next_it;
    }
  }
}

void InMemoryLabelIndex::AbortEntries(LabelId labelId, std::span<Vertex *const> vertices,
                                      uint64_t exact_start_timestamp) {
  auto locked_index = IndexOptReadLock(index_);
  auto const it = locked_index.value()->find(labelId);
  auto &label_storage = it->second;
  if (it == locked_index.value()->end()) return;
  DMG_ASSERT(it->second.state == READY, "Index should be ready before it's used");
  // Eagerly unlock; std::map has stable references
  locked_index.reset();

  auto vertices_acc = label_storage.index.access();
  for (auto *vertex : vertices) {
    vertices_acc.remove(Entry{vertex, exact_start_timestamp});
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

uint64_t InMemoryLabelIndex::ApproximateVertexCount(LabelId label) const {
  auto locked_index = index_.ReadLock();
  auto it = locked_index->find(label);
  MG_ASSERT(it != locked_index->end(), "Index for label {} doesn't exist", label.AsUint());
  DMG_ASSERT(it->second.state == READY, "Index should be ready before it's used");
  return it->second.index.size();
}

void InMemoryLabelIndex::RunGC() {
  // TODO Another thread safe structure could allow us to loop and remove without blocking any insertions
  auto locked_index = index_.MutableSharedLock();
  for (auto &index_entry : *locked_index) {
    index_entry.second.index.run_gc();
  }
}

InMemoryLabelIndex::Iterable InMemoryLabelIndex::Vertices(LabelId label, View view, Storage *storage,
                                                          Transaction *transaction) {
  DMG_ASSERT(storage->storage_mode_ == StorageMode::IN_MEMORY_TRANSACTIONAL ||
                 storage->storage_mode_ == StorageMode::IN_MEMORY_ANALYTICAL,
             "LabelIndex trying to access InMemory vertices from OnDisk!");
  auto vertices_acc = static_cast<InMemoryStorage const *>(storage)->vertices_.access();
  auto locked_index = IndexOptReadLock(index_);
  const auto it = locked_index.value()->find(label);
  MG_ASSERT(it != locked_index.value()->end(), "Index for label {} doesn't exist", label.AsUint());
  DMG_ASSERT(it->second.state == READY, "Index should be ready before it's used");
  locked_index.reset();
  return {it->second.index.access(), std::move(vertices_acc), label, view, storage, transaction};
}

InMemoryLabelIndex::Iterable InMemoryLabelIndex::Vertices(
    LabelId label, memgraph::utils::SkipList<memgraph::storage::Vertex>::ConstAccessor vertices_acc, View view,
    Storage *storage, Transaction *transaction) {
  auto locked_index = IndexOptReadLock(index_);
  const auto it = locked_index.value()->find(label);
  MG_ASSERT(it != locked_index.value()->end(), "Index for label {} doesn't exist", label.AsUint());
  DMG_ASSERT(it->second.state == READY, "Index should be ready before it's used");
  locked_index.reset();
  return {it->second.index.access(), std::move(vertices_acc), label, view, storage, transaction};
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

std::vector<LabelId> InMemoryLabelIndex::Analysis() const {
  std::vector<LabelId> res;
  auto locked_index = index_.ReadLock();
  res.reserve(locked_index->size());
  for (const auto &[label, index] : *locked_index) {
    if (index.state != READY) continue;
    res.emplace_back(label);
  }
  return res;
}

void InMemoryLabelIndex::DropGraphClearIndices() {
  // DROP GRAPH is unique access
  index_->clear();
  stats_->clear();
}

}  // namespace memgraph::storage
