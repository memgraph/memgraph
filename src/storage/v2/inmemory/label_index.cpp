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
#include "storage/v2/inmemory/indices_utils.hpp"

namespace memgraph::storage {

void InMemoryLabelIndex::UpdateOnAddLabel(LabelId label, Vertex *vertex, const Transaction &tx) {
  auto it = index_.find(label);
  if (it == index_.end()) return;
  auto acc = it->second.access();
  acc.insert(Entry{vertex, tx.start_timestamp});
}

bool InMemoryLabelIndex::CreateIndex(LabelId label, utils::SkipList<Vertex>::Accessor vertices,
                                     const std::optional<ParalellizedIndexCreationInfo> &paralell_exec_info) {
  auto create_index_seq = [this](LabelId label, utils::SkipList<Vertex>::Accessor &vertices,
                                 std::map<LabelId, utils::SkipList<Entry>>::iterator it) {
    using IndexAccessor = decltype(it->second.access());

    CreateIndexOnSingleThread(vertices, it, index_, label,
                              [](Vertex &vertex, LabelId label, IndexAccessor &index_accessor) {
                                TryInsertLabelIndex(vertex, label, index_accessor);
                              });

    return true;
  };

  auto create_index_par = [this](LabelId label, utils::SkipList<Vertex>::Accessor &vertices,
                                 std::map<LabelId, utils::SkipList<Entry>>::iterator label_it,
                                 const ParalellizedIndexCreationInfo &paralell_exec_info) {
    using IndexAccessor = decltype(label_it->second.access());

    CreateIndexOnMultipleThreads(vertices, label_it, index_, label, paralell_exec_info,
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

  if (paralell_exec_info) {
    return create_index_par(label, vertices, it, *paralell_exec_info);
  }
  return create_index_seq(label, vertices, it);
}

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

InMemoryLabelIndex::Iterable::Iterator::Iterator(Iterable *self, utils::SkipList<Entry>::Iterator index_iterator)
    : self_(self),
      index_iterator_(index_iterator),
      current_vertex_accessor_(nullptr, nullptr, nullptr, nullptr, self_->config_),
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
    if (CurrentVersionHasLabel(*index_iterator_->vertex, self_->label_, self_->transaction_, self_->view_)) {
      current_vertex_ = index_iterator_->vertex;
      current_vertex_accessor_ =
          VertexAccessor{current_vertex_, self_->transaction_, self_->indices_, self_->constraints_, self_->config_};
      break;
    }
  }
}

InMemoryLabelIndex::Iterable::Iterable(utils::SkipList<Entry>::Accessor index_accessor, LabelId label, View view,
                                       Transaction *transaction, Indices *indices, Constraints *constraints,
                                       Config::Items config)
    : index_accessor_(std::move(index_accessor)),
      label_(label),
      view_(view),
      transaction_(transaction),
      indices_(indices),
      constraints_(constraints),
      config_(config) {}

void InMemoryLabelIndex::RunGC() {
  for (auto &index_entry : index_) {
    index_entry.second.run_gc();
  }
}

}  // namespace memgraph::storage
