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
  auto it = index_.find(added_label);
  if (it == index_.end()) return;
  auto acc = it->second.access();
  acc.insert(Entry{vertex_after_update, tx.start_timestamp});
}

bool InMemoryLabelIndex::CreateIndex(LabelId label, utils::SkipList<Vertex>::Accessor vertices,
                                     const std::optional<ParallelizedIndexCreationInfo> &parallel_exec_info) {
  const auto create_index_seq = [this](LabelId label, utils::SkipList<Vertex>::Accessor &vertices,
                                       std::map<LabelId, utils::SkipList<Entry>>::iterator it) {
    using IndexAccessor = decltype(it->second.access());

    CreateIndexOnSingleThread(vertices, it, index_, label,
                              [](Vertex &vertex, LabelId label, IndexAccessor &index_accessor) {
                                TryInsertLabelIndex(vertex, label, index_accessor);
                              });

    return true;
  };

  const auto create_index_par = [this](LabelId label, utils::SkipList<Vertex>::Accessor &vertices,
                                       std::map<LabelId, utils::SkipList<Entry>>::iterator label_it,
                                       const ParallelizedIndexCreationInfo &parallel_exec_info) {
    using IndexAccessor = decltype(label_it->second.access());

    CreateIndexOnMultipleThreads(vertices, label_it, index_, label, parallel_exec_info,
                                 [](Vertex &vertex, LabelId label, IndexAccessor &index_accessor) {
                                   TryInsertLabelIndex(vertex, label, index_accessor);
                                 });

    return true;
  };

  auto [it, emplaced] = index_.emplace(std::piecewise_construct, std::forward_as_tuple(label), std::forward_as_tuple());
  if (!emplaced) {
    // Index already exists.
    return false;
  }

  if (parallel_exec_info) {
    return create_index_par(label, vertices, it, *parallel_exec_info);
  }
  return create_index_seq(label, vertices, it);
}

bool InMemoryLabelIndex::DropIndex(LabelId label) { return index_.erase(label) > 0; }

bool InMemoryLabelIndex::IndexExists(LabelId label) const { return index_.find(label) != index_.end(); }

std::vector<LabelId> InMemoryLabelIndex::ListIndices() const {
  std::vector<LabelId> ret;
  ret.reserve(index_.size());
  for (const auto &item : index_) {
    ret.push_back(item.first);
  }
  return ret;
}

void InMemoryLabelIndex::RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp) {
  for (auto &label_storage : index_) {
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
  auto it = index_.find(label);
  MG_ASSERT(it != index_.end(), "Index for label {} doesn't exist", label.AsUint());
  return it->second.size();
}

void InMemoryLabelIndex::RunGC() {
  for (auto &index_entry : index_) {
    index_entry.second.run_gc();
  }
}

InMemoryLabelIndex::Iterable InMemoryLabelIndex::Vertices(LabelId label, View view, Transaction *transaction,
                                                          Constraints *constraints) {
  const auto it = index_.find(label);
  MG_ASSERT(it != index_.end(), "Index for label {} doesn't exist", label.AsUint());
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
  std::vector<LabelId> deleted_indexes;
  auto locked_stats = stats_.Lock();
  deleted_indexes.reserve(locked_stats->size());
  std::transform(locked_stats->begin(), locked_stats->end(), std::back_inserter(deleted_indexes),
                 [](const auto &elem) { return elem.first; });
  locked_stats->clear();
  return deleted_indexes;
}

std::vector<LabelId> InMemoryLabelIndex::DeleteIndexStats(const storage::LabelId &label) {
  std::vector<LabelId> deleted_indexes;
  auto locked_stats = stats_.Lock();
  for (auto it = locked_stats->cbegin(); it != locked_stats->cend();) {
    if (it->first == label) {
      deleted_indexes.push_back(it->first);
      it = locked_stats->erase(it);
    } else {
      ++it;
    }
  }

  return deleted_indexes;
}

}  // namespace memgraph::storage
