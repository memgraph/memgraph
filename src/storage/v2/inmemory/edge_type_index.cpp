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

// void InMemoryEdgeTypeIndex::UpdateOnAddLabel(EdgeTypeId added_label, Edge *vertex_after_update, const Transaction
// &tx) {
//   auto it = index_.find(added_label);
//   if (it == index_.end()) return;
//   auto acc = it->second.access();
//   acc.insert(Entry{vertex_after_update, tx.start_timestamp});
// }

// TODO(gvolfing) - make this work
bool InMemoryEdgeTypeIndex::CreateIndex(EdgeTypeId label, utils::SkipList<Edge>::Accessor vertices) {
  //   const auto create_index_seq = [this](EdgeTypeId label, utils::SkipList<Vertex>::Accessor &vertices,
  //                                        std::map<EdgeTypeId, utils::SkipList<Entry>>::iterator it) {
  //     using IndexAccessor = decltype(it->second.access());

  //     CreateIndexOnSingleThread(vertices, it, index_, label,
  //                               [](Vertex &vertex, EdgeTypeId label, IndexAccessor &index_accessor) {
  //                                 TryInsertLabelIndex(vertex, label, index_accessor);
  //                               });

  //     return true;
  //   };

  //   const auto create_index_par = [this](EdgeTypeId label, utils::SkipList<Vertex>::Accessor &vertices,
  //                                        std::map<EdgeTypeId, utils::SkipList<Entry>>::iterator label_it,
  //                                        const ParallelizedIndexCreationInfo &parallel_exec_info) {
  //     using IndexAccessor = decltype(label_it->second.access());

  //     CreateIndexOnMultipleThreads(vertices, label_it, index_, label, parallel_exec_info,
  //                                  [](Vertex &vertex, EdgeTypeId label, IndexAccessor &index_accessor) {
  //                                    TryInsertLabelIndex(vertex, label, index_accessor);
  //                                  });

  //     return true;
  //   };

  //   auto [it, emplaced] = index_.emplace(std::piecewise_construct, std::forward_as_tuple(label),
  //   std::forward_as_tuple()); if (!emplaced) {
  //     // Index already exists.
  //     return false;
  //   }

  //   if (parallel_exec_info) {
  //     return create_index_par(label, vertices, it, *parallel_exec_info);
  //   }
  //   return create_index_seq(label, vertices, it);
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

InMemoryEdgeTypeIndex::Iterable::Iterable(utils::SkipList<Entry>::Accessor index_accessor, EdgeTypeId label, View view,
                                          Storage *storage, Transaction *transaction)
    : index_accessor_(std::move(index_accessor)),
      label_(label),
      view_(view),
      storage_(storage),
      transaction_(transaction) {}

// EdgeAccessor(EdgeRef edge, EdgeTypeId edge_type, Vertex *from_vertex, Vertex *to_vertex, Storage *storage,
//              Transaction *transaction, bool for_deleted = false)

//   struct EdgeRef {
//   explicit EdgeRef(Gid gid) : gid(gid) {}
//   explicit EdgeRef(Edge *ptr) : ptr(ptr) {}

//   union {
//     Gid gid;
//     Edge *ptr;
//   };
// };

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
    // if (index_iterator_->edge == current_edge_) {
    //   continue;
    // }
    // gvolfing - we need this part because of the view!
    // Do we?
    // auto accessor = EdgeAccessor{index_iterator_->edge, self_->storage_, self_->transaction_};
    // auto res = accessor.HasLabel(self_->label_, self_->view_);
    // if (!res.HasError() and res.GetValue()) {
    //   current_edge_ = accessor.vertex_;
    //   current_edge_accessor_ = accessor;
    //   break;
    // }

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

    // is this correct logic?
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
  //   for (auto &index_entry : index_) {
  //     index_entry.second.run_gc();
  //   }
}

InMemoryEdgeTypeIndex::Iterable InMemoryEdgeTypeIndex::Edges(EdgeTypeId label, View view, Storage *storage,
                                                             Transaction *transaction) {
  const auto it = index_.find(label);
  MG_ASSERT(it != index_.end(), "Index for label {} doesn't exist", label.AsUint());
  return {it->second.access(), label, view, storage, transaction};
}

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
