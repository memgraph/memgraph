// Copyright 2023 Memgraph Ltd.
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
#include "storage/v2/constraints/constraints.hpp"
#include "storage/v2/indices/indices_utils.hpp"

namespace memgraph::storage {

InMemoryLabelIndex::InMemoryLabelIndex(Indices *indices, Config config) : LabelIndex(indices, config) {}

void InMemoryLabelIndex::UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update, const Transaction &tx) {
  auto locked_index = index_.Lock();
  auto it = locked_index->find(added_label);
  if (it == locked_index->end()) return;
  auto acc = it->second.access();
  acc.insert(Entry{vertex_after_update, tx.start_timestamp});
}

bool InMemoryLabelIndex::CreateIndex(LabelId label, utils::SkipList<Vertex>::Accessor vertices,
                                     const std::optional<ParallelizedIndexCreationInfo> &parallel_exec_info) {
  const auto create_index_seq = [](LabelId label, utils::SkipList<Vertex>::Accessor &vertices,
                                   std::map<LabelId, utils::SkipList<Entry>>::iterator it, auto &index) {
    using IndexAccessor = decltype(it->second.access());

    CreateIndexOnSingleThread(vertices, it, index, label,
                              [](Vertex &vertex, LabelId label, IndexAccessor &index_accessor) {
                                TryInsertLabelIndex(vertex, label, index_accessor);
                              });

    return true;
  };

  const auto create_index_par = [](LabelId label, utils::SkipList<Vertex>::Accessor &vertices,
                                   std::map<LabelId, utils::SkipList<Entry>>::iterator label_it,
                                   const ParallelizedIndexCreationInfo &parallel_exec_info, auto &index) {
    using IndexAccessor = decltype(label_it->second.access());

    CreateIndexOnMultipleThreads(vertices, label_it, index, label, parallel_exec_info,
                                 [](Vertex &vertex, LabelId label, IndexAccessor &index_accessor) {
                                   TryInsertLabelIndex(vertex, label, index_accessor);
                                 });

    return true;
  };

  auto locked_index = index_.Lock();
  auto [it, emplaced] =
      locked_index->emplace(std::piecewise_construct, std::forward_as_tuple(label), std::forward_as_tuple());
  if (!emplaced) {
    // Index already exists.
    return false;
  }

  if (parallel_exec_info) {
    return create_index_par(label, vertices, it, *parallel_exec_info, *locked_index);
  }
  return create_index_seq(label, vertices, it, *locked_index);
}

bool InMemoryLabelIndex::DropIndex(LabelId label) {
  auto locked_index = index_.Lock();
  return locked_index->erase(label) > 0;
}

bool InMemoryLabelIndex::IndexExists(LabelId label) const {
  auto locked_index = index_.ReadLock();
  return locked_index->find(label) != locked_index->end();
}

std::vector<LabelId> InMemoryLabelIndex::ListIndices() const {
  std::vector<LabelId> ret;
  auto locked_index = index_.ReadLock();
  ret.reserve(locked_index->size());
  for (const auto &item : *locked_index) {
    ret.push_back(item.first);
  }
  return ret;
}

void InMemoryLabelIndex::RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp) {
  auto locked_index = index_.Lock();
  for (auto &label_storage : *locked_index) {
    auto vertices_acc = label_storage.second.access();
    for (auto it = vertices_acc.begin(); it != vertices_acc.end();) {
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

InMemoryLabelIndex::Iterable::Iterable(utils::SkipList<Entry>::Accessor index_accessor, LabelId label, View view,
                                       Transaction *transaction, Indices *indices, Constraints *constraints,
                                       const Config &config)
    : index_accessor_(std::move(index_accessor)),
      label_(label),
      view_(view),
      transaction_(transaction),
      indices_(indices),
      constraints_(constraints),
      config_(config) {}

InMemoryLabelIndex::Iterable::Iterator::Iterator(Iterable *self, utils::SkipList<Entry>::Iterator index_iterator)
    : self_(self),
      index_iterator_(index_iterator),
      current_vertex_accessor_(nullptr, nullptr, nullptr, nullptr, self_->config_.items),
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
    auto accessor = VertexAccessor{index_iterator_->vertex, self_->transaction_, self_->indices_, self_->constraints_,
                                   self_->config_.items};
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
  return it->second.size();
}

void InMemoryLabelIndex::RunGC() {
  auto locked_index = index_.Lock();
  for (auto &index_entry : *locked_index) {
    index_entry.second.run_gc();
  }
}

InMemoryLabelIndex::Iterable InMemoryLabelIndex::Vertices(LabelId label, View view, Transaction *transaction,
                                                          Constraints *constraints) {
  auto locked_index = index_.Lock();
  const auto it = locked_index->find(label);
  MG_ASSERT(it != locked_index->end(), "Index for label {} doesn't exist", label.AsUint());
  return {it->second.access(), label, view, transaction, indices_, constraints, config_};
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
  auto locked_index = index_.Lock();
  std::vector<LabelId> deleted_indexes;
  deleted_indexes.reserve(locked_index->size());
  std::transform(locked_index->begin(), locked_index->end(), std::back_inserter(deleted_indexes),
                 [](const auto &elem) { return elem.first; });
  locked_index->clear();
  return deleted_indexes;
}

std::vector<LabelId> InMemoryLabelIndex::DeleteIndexStats(const storage::LabelId &label) {
  auto locked_index = index_.Lock();
  std::vector<LabelId> deleted_indexes;
  for (auto it = locked_index->cbegin(); it != locked_index->cend();) {
    if (it->first == label) {
      deleted_indexes.push_back(it->first);
      it = locked_index->erase(it);
    } else {
      ++it;
    }
  }

  return deleted_indexes;
}

}  // namespace memgraph::storage
