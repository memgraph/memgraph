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

#include "storage/v3/splitter.hpp"

#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <set>

#include "storage/v3/config.hpp"
#include "storage/v3/delta.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/indices.hpp"
#include "storage/v3/key_store.hpp"
#include "storage/v3/name_id_mapper.hpp"
#include "storage/v3/schemas.hpp"
#include "storage/v3/shard.hpp"
#include "storage/v3/transaction.hpp"
#include "storage/v3/vertex.hpp"
#include "utils/logging.hpp"

namespace memgraph::storage::v3 {

Splitter::Splitter(const LabelId primary_label, VertexContainer &vertices, EdgeContainer &edges,
                   std::map<uint64_t, std::unique_ptr<Transaction>> &start_logical_id_to_transaction, Indices &indices,
                   const Config &config, const std::vector<SchemaProperty> &schema, const NameIdMapper &name_id_mapper)
    : primary_label_(primary_label),
      vertices_(vertices),
      edges_(edges),
      start_logical_id_to_transaction_(start_logical_id_to_transaction),
      indices_(indices),
      config_(config),
      schema_(schema),
      name_id_mapper_(name_id_mapper) {}

SplitData Splitter::SplitShard(const PrimaryKey &split_key, const std::optional<PrimaryKey> &max_primary_key,
                               const Hlc shard_version) {
  SplitData data{.primary_label = primary_label_,
                 .min_primary_key = split_key,
                 .max_primary_key = max_primary_key,
                 .schema = schema_,
                 .config = config_,
                 .id_to_name = name_id_mapper_.GetIdToNameMap(),
                 .shard_version = shard_version};

  std::set<uint64_t> collected_transactions_;
  data.vertices = CollectVertices(data, collected_transactions_, split_key);
  data.edges = CollectEdges(collected_transactions_, data.vertices, split_key);
  data.transactions = CollectTransactions(collected_transactions_, data.vertices, *data.edges, split_key);

  return data;
}

void Splitter::ScanDeltas(std::set<uint64_t> &collected_transactions_, Delta *delta) {
  while (delta != nullptr) {
    collected_transactions_.insert(delta->commit_info->start_or_commit_timestamp.logical_id);
    delta = delta->next;
  }
}

VertexContainer Splitter::CollectVertices(SplitData &data, std::set<uint64_t> &collected_transactions_,
                                          const PrimaryKey &split_key) {
  data.label_indices = indices_.label_index.SplitIndexEntries(split_key);
  data.label_property_indices = indices_.label_property_index.SplitIndexEntries(split_key);

  VertexContainer splitted_data;
  auto split_key_it = vertices_.find(split_key);
  while (split_key_it != vertices_.end()) {
    // Go through deltas and pick up transactions start_id/commit_id
    ScanDeltas(collected_transactions_, split_key_it->second.delta);

    auto next_it = std::next(split_key_it);

    const auto &[splitted_vertex_it, inserted, node] = splitted_data.insert(vertices_.extract(split_key_it->first));
    MG_ASSERT(inserted, "Failed to extract vertex!");

    split_key_it = next_it;
  }
  return splitted_data;
}

std::optional<EdgeContainer> Splitter::CollectEdges(std::set<uint64_t> &collected_transactions_,
                                                    const VertexContainer &split_vertices,
                                                    const PrimaryKey &split_key) {
  if (!config_.items.properties_on_edges) {
    return std::nullopt;
  }
  EdgeContainer splitted_edges;
  const auto split_vertex_edges = [&](const auto &edges_ref) {
    // This is safe since if properties_on_edges is true, the this must be a ptr
    for (const auto &edge_ref : edges_ref) {
      auto *edge = std::get<2>(edge_ref).ptr;
      const auto &other_vtx = std::get<1>(edge_ref);
      ScanDeltas(collected_transactions_, edge->delta);
      // Check if src and dest edge are both on splitted shard so we know if we
      // should remove orphan edge, or make a clone
      if (other_vtx.primary_key >= split_key) {
        // Remove edge from shard
        splitted_edges.insert(edges_.extract(edge->gid));
      } else {
        splitted_edges.insert({edge->gid, Edge{edge->gid, edge->delta}});
      }
    }
  };

  for (const auto &vertex : split_vertices) {
    split_vertex_edges(vertex.second.in_edges);
    split_vertex_edges(vertex.second.out_edges);
  }
  return splitted_edges;
}

std::map<uint64_t, std::unique_ptr<Transaction>> Splitter::CollectTransactions(
    const std::set<uint64_t> &collected_transactions_, VertexContainer &cloned_vertices, EdgeContainer &cloned_edges,
    const PrimaryKey &split_key) {
  std::map<uint64_t, std::unique_ptr<Transaction>> transactions;

  for (const auto &[commit_start, transaction] : start_logical_id_to_transaction_) {
    // We need all transaction whose deltas need to be resolved for any of the
    // entities
    if (collected_transactions_.contains(transaction->commit_info->start_or_commit_timestamp.logical_id)) {
      transactions.insert({commit_start, start_logical_id_to_transaction_[commit_start]->Clone()});
    }
  }

  // It is necessary to clone all the transactions first so we have new addresses
  // for deltas, before doing alignment of deltas and prev_ptr
  AdjustClonedTransactions(transactions, cloned_vertices, cloned_edges, split_key);
  return transactions;
}

void PruneDeltas(Transaction &cloned_transaction, std::map<uint64_t, std::unique_ptr<Transaction>> &cloned_transactions,
                 const PrimaryKey &split_key) {
  // Remove delta chains that don't point to objects on splitted shard
  auto cloned_delta_it = cloned_transaction.deltas.begin();

  while (cloned_delta_it != cloned_transaction.deltas.end()) {
    const auto prev = cloned_delta_it->prev.Get();
    switch (prev.type) {
      case PreviousPtr::Type::DELTA:
      case PreviousPtr::Type::NULLPTR:
        ++cloned_delta_it;
        break;
      case PreviousPtr::Type::VERTEX: {
        if (prev.vertex->first < split_key) {
          // We can remove this delta chain
          auto *current_next_delta = cloned_delta_it->next;
          cloned_delta_it = cloned_transaction.deltas.erase(cloned_delta_it);

          while (current_next_delta != nullptr) {
            auto *next_delta = current_next_delta->next;
            // Find next delta transaction delta list
            auto current_transaction_it = std::ranges::find_if(
                cloned_transactions,
                [&start_or_commit_timestamp =
                     current_next_delta->commit_info->start_or_commit_timestamp](const auto &transaction) {
                  return transaction.second->start_timestamp == start_or_commit_timestamp ||
                         transaction.second->commit_info->start_or_commit_timestamp == start_or_commit_timestamp;
                });
            MG_ASSERT(current_transaction_it != cloned_transactions.end(), "Error when pruning deltas!");
            // Remove it
            current_transaction_it->second->deltas.remove_if(
                [&current_next_delta = *current_next_delta](const auto &delta) { return delta == current_next_delta; });

            current_next_delta = next_delta;
          }
        } else {
          ++cloned_delta_it;
        }
        break;
      }
      case PreviousPtr::Type::EDGE:
        ++cloned_delta_it;
        break;
    }
  }
}

void Splitter::AdjustClonedTransactions(std::map<uint64_t, std::unique_ptr<Transaction>> &cloned_transactions,
                                        VertexContainer &cloned_vertices, EdgeContainer &cloned_edges,
                                        const PrimaryKey &split_key) {
  for (auto &[commit_start, cloned_transaction] : cloned_transactions) {
    AdjustClonedTransaction(*cloned_transaction, *start_logical_id_to_transaction_[commit_start], cloned_transactions,
                            cloned_vertices, cloned_edges, split_key);
  }
  // Prune deltas whose delta chain points to vertex/edge that should not belong on that shard
  // Prune must be after ajdust, since next, and prev are not set and we cannot follow the chain
  for (auto &[commit_start, cloned_transaction] : cloned_transactions) {
    PruneDeltas(*cloned_transaction, cloned_transactions, split_key);
  }
}

inline bool IsDeltaHeadOfChain(const PreviousPtr::Type &delta_type) {
  return delta_type == PreviousPtr::Type::VERTEX || delta_type == PreviousPtr::Type::EDGE;
}

bool DoesPrevPtrPointsToSplittedData(const PreviousPtr::Pointer &prev_ptr, const PrimaryKey &split_key) {
  return prev_ptr.type == PreviousPtr::Type::VERTEX && prev_ptr.vertex->first < split_key;
}

void Splitter::AdjustClonedTransaction(Transaction &cloned_transaction, const Transaction &transaction,
                                       std::map<uint64_t, std::unique_ptr<Transaction>> &cloned_transactions,
                                       VertexContainer &cloned_vertices, EdgeContainer &cloned_edges,
                                       const PrimaryKey & /*split_key*/) {
  auto delta_it = transaction.deltas.begin();
  auto cloned_delta_it = cloned_transaction.deltas.begin();

  while (delta_it != transaction.deltas.end()) {
    // We can safely ignore deltas which are not head of delta chain
    // Dont' adjust delta chain that points to irrelevant data vertices/edges
    if (const auto delta_prev = delta_it->prev.Get(); !IsDeltaHeadOfChain(delta_prev.type)) {
      ++delta_it;
      ++cloned_delta_it;
      continue;
    }

    const auto *delta = &*delta_it;
    auto *cloned_delta = &*cloned_delta_it;
    Delta *cloned_delta_prev_ptr = cloned_delta;
    while (delta->next != nullptr) {
      AdjustEdgeRef(*cloned_delta, cloned_edges);

      // Align next ptr
      AdjustDeltaNext(*delta, *cloned_delta, cloned_transactions);

      // Align prev ptr
      if (cloned_delta_prev_ptr != nullptr) {
        AdjustDeltaPrevPtr(*delta, *cloned_delta_prev_ptr, cloned_transactions, cloned_vertices, cloned_edges);
      }

      // TODO Next delta might not belong to the cloned transaction and thats
      // why we skip this delta of the delta chain
      if (cloned_delta->next != nullptr) {
        cloned_delta = cloned_delta->next;
        cloned_delta_prev_ptr = cloned_delta;
      } else {
        cloned_delta_prev_ptr = nullptr;
      }
      delta = delta->next;
    }
    // Align prev ptr
    if (cloned_delta_prev_ptr != nullptr) {
      AdjustDeltaPrevPtr(*delta, *cloned_delta_prev_ptr, cloned_transactions, cloned_vertices, cloned_edges);
    }

    ++delta_it;
    ++cloned_delta_it;
  }
  MG_ASSERT(delta_it == transaction.deltas.end() && cloned_delta_it == cloned_transaction.deltas.end(),
            "Both iterators must be exhausted!");
}

void Splitter::AdjustEdgeRef(Delta &cloned_delta, EdgeContainer &cloned_edges) const {
  switch (cloned_delta.action) {
    case Delta::Action::ADD_IN_EDGE:
    case Delta::Action::ADD_OUT_EDGE:
    case Delta::Action::REMOVE_IN_EDGE:
    case Delta::Action::REMOVE_OUT_EDGE: {
      // Find edge
      if (config_.items.properties_on_edges) {
        // Only case when not finding is when the edge is not on splitted shard
        // TODO Do this after prune an move condition into assert
        if (const auto cloned_edge_it =
                std::ranges::find_if(cloned_edges, [edge_ptr = cloned_delta.vertex_edge.edge.ptr](
                                                       const auto &elem) { return elem.second.gid == edge_ptr->gid; });
            cloned_edge_it != cloned_edges.end()) {
          cloned_delta.vertex_edge.edge = EdgeRef{&cloned_edge_it->second};
        }
      }
      break;
    }
    case Delta::Action::DELETE_OBJECT:
    case Delta::Action::RECREATE_OBJECT:
    case Delta::Action::SET_PROPERTY:
    case Delta::Action::ADD_LABEL:
    case Delta::Action::REMOVE_LABEL: {
      // noop
      break;
    }
  }
}

void Splitter::AdjustDeltaNext(const Delta &original, Delta &cloned,
                               std::map<uint64_t, std::unique_ptr<Transaction>> &cloned_transactions) {
  // Get cloned_delta->next transaction, using delta->next original transaction
  auto cloned_transaction_it = std::ranges::find_if(cloned_transactions, [&original](const auto &elem) {
    return elem.second->start_timestamp == original.next->commit_info->start_or_commit_timestamp ||
           elem.second->commit_info->start_or_commit_timestamp == original.next->commit_info->start_or_commit_timestamp;
  });
  // TODO(jbajic) What if next in delta chain does not belong to cloned transaction?
  // MG_ASSERT(cloned_transaction_it != cloned_transactions.end(), "Cloned transaction not found");
  if (cloned_transaction_it == cloned_transactions.end()) return;
  // Find cloned delta in delta list of cloned transaction
  auto found_cloned_delta_it = std::ranges::find_if(
      cloned_transaction_it->second->deltas, [&original](const auto &elem) { return elem.id == original.next->id; });
  MG_ASSERT(found_cloned_delta_it != cloned_transaction_it->second->deltas.end(), "Delta with given uuid must exist!");
  cloned.next = &*found_cloned_delta_it;
}

void Splitter::AdjustDeltaPrevPtr(const Delta &original, Delta &cloned,
                                  std::map<uint64_t, std::unique_ptr<Transaction>> &cloned_transactions,
                                  VertexContainer & /*cloned_vertices*/, EdgeContainer &cloned_edges) {
  auto ptr = original.prev.Get();
  switch (ptr.type) {
    case PreviousPtr::Type::NULLPTR: {
      // noop
      break;
    }
    case PreviousPtr::Type::DELTA: {
      // Same as for deltas except don't align next but prev
      auto cloned_transaction_it = std::ranges::find_if(cloned_transactions, [&ptr](const auto &elem) {
        return elem.second->start_timestamp == ptr.delta->commit_info->start_or_commit_timestamp ||
               elem.second->commit_info->start_or_commit_timestamp == ptr.delta->commit_info->start_or_commit_timestamp;
      });
      MG_ASSERT(cloned_transaction_it != cloned_transactions.end(), "Cloned transaction not found");
      // Find cloned delta in delta list of cloned transaction
      auto found_cloned_delta_it =
          std::ranges::find_if(cloned_transaction_it->second->deltas,
                               [delta = ptr.delta](const auto &elem) { return elem.id == delta->id; });
      MG_ASSERT(found_cloned_delta_it != cloned_transaction_it->second->deltas.end(),
                "Delta with given id must exist!");

      cloned.prev.Set(&*found_cloned_delta_it);
      break;
    }
    case PreviousPtr::Type::VERTEX: {
      // The vertex was extracted and it is safe to reuse address
      cloned.prev.Set(ptr.vertex);
      break;
    }
    case PreviousPtr::Type::EDGE: {
      // We can never be here if we have properties on edge disabled
      auto *cloned_edge = &*cloned_edges.find(ptr.edge->gid);
      cloned.prev.Set(&cloned_edge->second);
      break;
    }
  };
}

}  // namespace memgraph::storage::v3
