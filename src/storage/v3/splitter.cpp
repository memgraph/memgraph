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

#include <map>
#include <memory>
#include <optional>
#include <set>

#include "storage/v3/config.hpp"
#include "storage/v3/indices.hpp"
#include "storage/v3/key_store.hpp"
#include "storage/v3/transaction.hpp"
#include "storage/v3/vertex.hpp"

namespace memgraph::storage::v3 {

Splitter::Splitter(VertexContainer &vertices, EdgeContainer &edges,
                   std::map<uint64_t, std::unique_ptr<Transaction>> &start_logical_id_to_transaction, Indices &indices,
                   Config &config)
    : vertices_(vertices),
      edges_(edges),
      start_logical_id_to_transaction_(start_logical_id_to_transaction),
      indices_(indices),
      config_(config) {}

SplitData Splitter::SplitShard(const PrimaryKey &split_key) {
  SplitData data;

  std::set<uint64_t> collected_transactions_start_id;
  data.vertices = CollectVertices(data, collected_transactions_start_id, split_key);
  data.edges = CollectEdges(collected_transactions_start_id, data.vertices, split_key);
  data.transactions = CollectTransactions(collected_transactions_start_id, data.vertices, *data.edges);

  return data;
}

void Splitter::ScanDeltas(std::set<uint64_t> &collected_transactions_start_id, Delta *delta) {
  while (delta != nullptr) {
    collected_transactions_start_id.insert(delta->commit_info->start_or_commit_timestamp.logical_id);
    delta = delta->next;
  }
}

std::map<LabelId, LabelIndex::LabelIndexContainer> Splitter::CollectLabelIndices(
    const PrimaryKey &split_key,
    std::map<LabelId, std::multimap<const Vertex *, LabelIndex::Entry *>> &vertex_entry_map) {
  if (indices_.label_index.Empty()) {
    return {};
  }

  // Space O(i * n/2 * 2), i number of indexes, n number of vertices
  std::map<LabelId, LabelIndex::LabelIndexContainer> cloned_indices;
  for (auto &[label, index] : indices_.label_index.GetIndex()) {
    for (const auto &entry : index) {
      if (entry.vertex->first > split_key) {
        [[maybe_unused]] auto [it, inserted, node] = cloned_indices[label].insert(index.extract(entry));
        vertex_entry_map[label].insert({entry.vertex, &node.value()});
      }
    }
  }

  return cloned_indices;
}

std::map<std::pair<LabelId, PropertyId>, LabelPropertyIndex::LabelPropertyIndexContainer>
Splitter::CollectLabelPropertyIndices(
    const PrimaryKey &split_key,
    std::map<std::pair<LabelId, PropertyId>, std::multimap<const Vertex *, LabelPropertyIndex::Entry *>>
        &vertex_entry_map) {
  if (indices_.label_property_index.Empty()) {
    return {};
  }

  std::map<std::pair<LabelId, PropertyId>, LabelPropertyIndex::LabelPropertyIndexContainer> cloned_indices;
  for (auto &[label_prop_pair, index] : indices_.label_property_index.GetIndex()) {
    cloned_indices[label_prop_pair] = LabelPropertyIndex::LabelPropertyIndexContainer{};
    for (const auto &entry : index) {
      if (entry.vertex->first > split_key) {
        // We get this entry
        [[maybe_unused]] const auto [it, inserted, node] = cloned_indices[label_prop_pair].insert(index.extract(entry));
        vertex_entry_map[label_prop_pair].insert({entry.vertex, &node.value()});
      }
    }
  }

  return cloned_indices;
}

VertexContainer Splitter::CollectVertices(SplitData &data, std::set<uint64_t> &collected_transactions_start_id,
                                          const PrimaryKey &split_key) {
  // Collection of indices is here since it heavily depends on vertices
  // Old vertex pointer new entry pointer
  std::map<LabelId, std::multimap<const Vertex *, LabelIndex::Entry *>> label_index_vertex_entry_map;
  std::map<std::pair<LabelId, PropertyId>, std::multimap<const Vertex *, LabelPropertyIndex::Entry *>>
      label_property_vertex_entry_map;
  data.label_indices = CollectLabelIndices(split_key, label_index_vertex_entry_map);
  data.label_property_indices = CollectLabelPropertyIndices(split_key, label_property_vertex_entry_map);
  const auto update_indices = [](auto &index_map, const auto *old_vertex_ptr, auto &splitted_vertex_it) {
    for (auto &[label, vertex_entry_mappings] : index_map) {
      auto [it, end] = vertex_entry_mappings.equal_range(old_vertex_ptr);
      while (it != end) {
        it->second->vertex = &*splitted_vertex_it;
        ++it;
      }
    }
  };

  VertexContainer splitted_data;
  auto split_key_it = vertices_.find(split_key);
  while (split_key_it != vertices_.end()) {
    // Go through deltas and pick up transactions start_id
    ScanDeltas(collected_transactions_start_id, split_key_it->second.delta);

    const auto *old_vertex_ptr = &*split_key_it;
    auto next_it = std::next(split_key_it);

    const auto &[splitted_vertex_it, inserted, node] = splitted_data.insert(vertices_.extract(split_key_it->first));

    // Update indices
    update_indices(label_index_vertex_entry_map, old_vertex_ptr, splitted_vertex_it);
    update_indices(label_property_vertex_entry_map, old_vertex_ptr, splitted_vertex_it);

    split_key_it = next_it;
  }
  return splitted_data;
}

std::optional<EdgeContainer> Splitter::CollectEdges(std::set<uint64_t> &collected_transactions_start_id,
                                                    const VertexContainer &split_vertices,
                                                    const PrimaryKey &split_key) {
  if (!config_.items.properties_on_edges) {
    return std::nullopt;
  }
  EdgeContainer splitted_edges;
  const auto split_vertex_edges = [&](const auto &edges_ref) {
    // This is safe since if properties_on_edges is true, the this must be a
    // ptr
    for (const auto &edge_ref : edges_ref) {
      auto *edge = std::get<2>(edge_ref).ptr;
      const auto &other_vtx = std::get<1>(edge_ref);
      ScanDeltas(collected_transactions_start_id, edge->delta);
      // Check if src and dest edge are both on splitted shard
      // so we know if we should remove orphan edge
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

std::map<uint64_t, Transaction> Splitter::CollectTransactions(const std::set<uint64_t> &collected_transactions_start_id,
                                                              VertexContainer &cloned_vertices,
                                                              EdgeContainer &cloned_edges) {
  std::map<uint64_t, Transaction> transactions;
  for (const auto commit_start : collected_transactions_start_id) {
    // If it does not contain then the transaction has commited, and we ignore it
    if (start_logical_id_to_transaction_.contains(commit_start)) {
      transactions.insert({commit_start, start_logical_id_to_transaction_[commit_start]->Clone()});
    }
  }
  // It is necessary to clone all the transactions first so we have new addresses
  // for deltas, before doing alignment of deltas and prev_ptr
  AlignClonedTransactions(transactions, cloned_vertices, cloned_edges);
  return transactions;
}

void Splitter::AlignClonedTransaction(Transaction &cloned_transaction, const Transaction &transaction,
                                      std::map<uint64_t, Transaction> &cloned_transactions,
                                      VertexContainer &cloned_vertices, EdgeContainer &cloned_edges) {
  // Align next and prev in deltas
  // NOTE It is important that the order of delta lists is in same order
  auto delta_it = transaction.deltas.begin();
  auto cloned_delta_it = cloned_transaction.deltas.begin();
  while (delta_it != transaction.deltas.end() && cloned_delta_it != cloned_transaction.deltas.end()) {
    MG_ASSERT(delta_it->uuid == cloned_delta_it->uuid, "The order of deltas is not correct");
    // Find appropriate prev and delta->next for cloned deltas

    const auto *delta = &*delta_it;
    auto *cloned_delta = &*cloned_delta_it;
    while (delta != nullptr) {
      // Align delta, while ignoring deltas whose transactions have commited,
      // or aborted
      if (cloned_transactions.contains(delta->commit_info->start_or_commit_timestamp.logical_id)) {
        cloned_delta->next = &*std::ranges::find_if(
            cloned_transactions.at(delta->commit_info->start_or_commit_timestamp.logical_id).deltas,
            [delta](const auto &elem) { return elem.uuid == delta->uuid; });
      } else {
        delta = delta->next;
        continue;
      }
      // Align prev ptr
      auto ptr = delta->prev.Get();
      switch (ptr.type) {
        case PreviousPtr::Type::NULLPTR: {
          // noop
          break;
        }
        case PreviousPtr::Type::DELTA: {
          cloned_delta->prev.Set(ptr.delta);
          break;
        }
        case PreviousPtr::Type::VERTEX: {
          // What if the vertex is already moved to garbage collection...
          // Make test when you have deleted vertex
          auto *cloned_vertex = &*cloned_vertices.find(ptr.vertex->first);
          cloned_delta->prev.Set(cloned_vertex);
          break;
        }
        case PreviousPtr::Type::EDGE: {
          // TODO Case when there are no properties on edge is not handled
          auto *cloned_edge = &*cloned_edges.find(ptr.edge->gid);
          cloned_delta->prev.Set(&cloned_edge->second);
          break;
        }
      };

      cloned_delta = cloned_delta->next;
      delta = delta->next;
    }

    ++delta_it;
    ++cloned_delta_it;
  }
  MG_ASSERT(delta_it == transaction.deltas.end() && cloned_delta_it == cloned_transaction.deltas.end(),
            "Both iterators must be exhausted!");
}

void Splitter::AlignClonedTransactions(std::map<uint64_t, Transaction> &cloned_transactions,
                                       VertexContainer &cloned_vertices, EdgeContainer &cloned_edges) {
  for (auto &[commit_start, cloned_transaction] : cloned_transactions) {
    AlignClonedTransaction(cloned_transaction, *start_logical_id_to_transaction_[commit_start], cloned_transactions,
                           cloned_vertices, cloned_edges);
  }
}

}  // namespace memgraph::storage::v3
