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

#include "storage/v2/inmemory/edge_type_index.hpp"
#include "storage/v2/constraints/constraints.hpp"
#include "storage/v2/indices/indices_utils.hpp"

namespace memgraph::storage {

bool InMemoryEdgeTypeIndex::CreateIndex(EdgeTypeId edge_type, utils::SkipList<Vertex>::Accessor vertices) {
  auto [it, emplaced] = index_.try_emplace(edge_type);
  if (!emplaced) {
    return false;
  }
  // TODO make this more readable.
  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
  try {
    auto edge_acc = it->second.access();
    for (auto &from_vertex : vertices) {
      if (from_vertex.deleted) {
        continue;
      }
      // Verify if it is enough to loop over only the outgoing edges.
      for (const auto &edge : from_vertex.out_edges) {
        const auto type = std::get<0>(edge);
        if (type == edge_type) {
          auto *to_vertex = std::get<1>(edge);
          if (to_vertex->deleted) {
            continue;
          }
          edge_acc.insert({&from_vertex, to_vertex, 0});
        }
      }
    }
  } catch (const utils::OutOfMemoryException &) {
    utils::MemoryTracker::OutOfMemoryExceptionBlocker oom_exception_blocker;
    index_.erase(it);
    throw;
  }

  return true;
}

bool InMemoryEdgeTypeIndex::DropIndex(EdgeTypeId label) { return index_.erase(label) > 0; }

bool InMemoryEdgeTypeIndex::IndexExists(EdgeTypeId label) const { return index_.find(label) != index_.end(); }

std::vector<EdgeTypeId> InMemoryEdgeTypeIndex::ListIndices() const {
  std::vector<EdgeTypeId> ret;
  ret.reserve(index_.size());
  for (const auto &item : index_) {
    ret.push_back(item.first);
  }
  return ret;
}

void InMemoryEdgeTypeIndex::RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp) {
  for (auto &label_storage : index_) {
    auto edges_acc = label_storage.second.access();
    for (auto it = edges_acc.begin(); it != edges_acc.end();) {
      auto next_it = it;
      ++next_it;

      if (it->timestamp >= oldest_active_start_timestamp) {
        it = next_it;
        continue;
      }

      // TODO(gvolfing)
      // Make sure we can detect entries that should be picked up by the gc.
      // if ((next_it != edges_acc.end() && it->edge == next_it->edge) ||
      //     !AnyVersionHasLabel(*it->edge, label_storage.first, oldest_active_start_timestamp)) {
      //   edges_acc.remove(*it);
      // }

      it = next_it;
    }
  }
}

uint64_t InMemoryEdgeTypeIndex::ApproximateEdgeCount(EdgeTypeId edge_type) const {
  if (auto it = index_.find(edge_type); it != index_.end()) {
    return it->second.size();
  }
  return 0;
}

InMemoryEdgeTypeIndex::Iterable::Iterable(utils::SkipList<Entry>::Accessor index_accessor, EdgeTypeId label, View view,
                                          Storage *storage, Transaction *transaction)
    : index_accessor_(std::move(index_accessor)),
      label_(label),
      view_(view),
      storage_(storage),
      transaction_(transaction) {}

InMemoryEdgeTypeIndex::Iterable::Iterator::Iterator(Iterable *self, utils::SkipList<Entry>::Iterator index_iterator)
    : self_(self),
      index_iterator_(index_iterator),
      current_edge_accessor_(EdgeRef{nullptr}, EdgeTypeId::FromInt(0), nullptr, nullptr, self_->storage_, nullptr),
      current_edge_(nullptr) {
  AdvanceUntilValid();
}

InMemoryEdgeTypeIndex::Iterable::Iterator &InMemoryEdgeTypeIndex::Iterable::Iterator::operator++() {
  ++index_iterator_;
  AdvanceUntilValid();
  return *this;
}

void InMemoryEdgeTypeIndex::Iterable::Iterator::AdvanceUntilValid() {
  for (; index_iterator_ != self_->index_accessor_.end(); ++index_iterator_) {
    auto *from_vertex = index_iterator_->from_vertex;
    auto *to_vertex = index_iterator_->to_vertex;

    auto [edge_ref, edge_type] =
        std::invoke([&, &from = from_vertex->out_edges, &to = to_vertex->in_edges]() -> std::pair<EdgeRef, EdgeTypeId> {
          for (const auto &from_entry : from) {
            const auto from_edge = std::get<2>(from_entry);
            for (const auto &to_entry : to) {
              const auto to_edge = std::get<2>(to_entry);
              if (from_edge == to_edge) {
                return std::make_pair(from_edge, std::get<0>(from_entry));
              }
            }
          }
          return {EdgeRef(nullptr), EdgeTypeId::FromUint(0)};
        });

    if (edge_ref == EdgeRef(nullptr)) {
      // TODO gvolfing - handle this properly:
      // It should not be possible to not find a matching from-to pair.
      MG_ASSERT(false, "gvolfing was lazy and it crashed your instance.");
    }

    // verify if this is the correct logic
    if (edge_ref == current_edge_) {
      continue;
    }

    auto accessor = EdgeAccessor{edge_ref, edge_type, from_vertex, to_vertex, self_->storage_, self_->transaction_};
    current_edge_accessor_ = accessor;
    current_edge_ = edge_ref;
    break;
  }
}

void InMemoryEdgeTypeIndex::RunGC() {
  // Enable this once we get to the GC.
  //   for (auto &index_entry : index_) {
  //     index_entry.second.run_gc();
  //   }
}

InMemoryEdgeTypeIndex::Iterable InMemoryEdgeTypeIndex::Edges(EdgeTypeId label, View view, Storage *storage,
                                                             Transaction *transaction) {
  auto debug_var = index_.size();
  const auto it = index_.find(label);
  MG_ASSERT(it != index_.end(), "Index for label {} doesn't exist", label.AsUint());
  auto debug_var_two = it->second.access().size();
  return {it->second.access(), label, view, storage, transaction};
}

// TODO Implement indexstat metadata handling functions.
// void InMemoryLabelIndex::SetIndexStats(const storage::LabelId &label, const storage::LabelIndexStats &stats) {
//   auto locked_stats = stats_.Lock();
//   locked_stats->insert_or_assign(label, stats);
// }

// std::optional<LabelIndexStats> InMemoryLabelIndex::GetIndexStats(const storage::LabelId &label) const {
//   auto locked_stats = stats_.ReadLock();
//   if (auto it = locked_stats->find(label); it != locked_stats->end()) {
//     return it->second;
//   }
//   return {};
// }

// std::vector<LabelId> InMemoryLabelIndex::ClearIndexStats() {
//   std::vector<LabelId> deleted_indexes;
//   auto locked_stats = stats_.Lock();
//   deleted_indexes.reserve(locked_stats->size());
//   std::transform(locked_stats->begin(), locked_stats->end(), std::back_inserter(deleted_indexes),
//                  [](const auto &elem) { return elem.first; });
//   locked_stats->clear();
//   return deleted_indexes;
// }

// // stats_ is a map with label as the key, so only one can exist at a time
// bool InMemoryLabelIndex::DeleteIndexStats(const storage::LabelId &label) {
//   auto locked_stats = stats_.Lock();
//   for (auto it = locked_stats->cbegin(); it != locked_stats->cend(); ++it) {
//     if (it->first == label) {
//       locked_stats->erase(it);
//       return true;
//     }
//   }
//   return false;
// }

}  // namespace memgraph::storage
