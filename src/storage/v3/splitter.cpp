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

SplitData Splitter::SplitShard(const PrimaryKey &split_key, const std::optional<PrimaryKey> &max_primary_key) {
  SplitData data{.primary_label = primary_label_,
                 .min_primary_key = split_key,
                 .max_primary_key = max_primary_key,
                 .schema = schema_,
                 .config = config_,
                 .id_to_name = name_id_mapper_.GetIdToNameMap()};

  std::set<uint64_t> collected_transactions_;
  data.vertices = CollectVertices(data, collected_transactions_, split_key);
  data.edges = CollectEdges(collected_transactions_, data.vertices, split_key);
  data.transactions = CollectTransactions(collected_transactions_, data.vertices, *data.edges);

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
  // Collection of indices is here since it heavily depends on vertices
  // Old vertex pointer new entry pointer
  std::map<LabelId, std::multimap<const Vertex *, const LabelIndex::IndexContainer::iterator>>
      label_index_vertex_entry_map;
  std::map<std::pair<LabelId, PropertyId>,
           std::multimap<const Vertex *, const LabelPropertyIndex::IndexContainer::iterator>>
      label_property_vertex_entry_map;

  data.label_indices =
      CollectIndexEntries<LabelIndex, LabelId>(indices_.label_index, split_key, label_index_vertex_entry_map);
  data.label_property_indices = CollectIndexEntries<LabelPropertyIndex, std::pair<LabelId, PropertyId>>(
      indices_.label_property_index, split_key, label_property_vertex_entry_map);
  // This is needed to replace old vertex pointers in index entries with new ones
  const auto update_indices = [](auto &entry_vertex_map, auto &updating_index, const auto *old_vertex_ptr,
                                 auto &new_vertex_ptr) {
    for ([[maybe_unused]] auto &[index_type, vertex_entry_mappings] : entry_vertex_map) {
      auto [it, end] = vertex_entry_mappings.equal_range(old_vertex_ptr);
      while (it != end) {
        auto entry_to_update = *it->second;
        entry_to_update.vertex = &*new_vertex_ptr;
        updating_index.at(index_type).erase(it->second);
        updating_index.at(index_type).insert(std::move(entry_to_update));
        ++it;
      }
    }
  };

  VertexContainer splitted_data;
  auto split_key_it = vertices_.find(split_key);
  while (split_key_it != vertices_.end()) {
    // Go through deltas and pick up transactions start_id/commit_id
    ScanDeltas(collected_transactions_, split_key_it->second.delta);

    const auto *old_vertex_ptr = &*split_key_it;
    auto next_it = std::next(split_key_it);

    const auto &[splitted_vertex_it, inserted, node] = splitted_data.insert(vertices_.extract(split_key_it->first));
    MG_ASSERT(inserted, "Failed to extract vertex!");

    // Update indices
    update_indices(label_index_vertex_entry_map, data.label_indices, old_vertex_ptr, splitted_vertex_it);
    update_indices(label_property_vertex_entry_map, data.label_property_indices, old_vertex_ptr, splitted_vertex_it);

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
    const std::set<uint64_t> &collected_transactions_, VertexContainer &cloned_vertices, EdgeContainer &cloned_edges) {
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
  AdjustClonedTransactions(transactions, cloned_vertices, cloned_edges);
  return transactions;
}

void Splitter::AdjustClonedTransactions(std::map<uint64_t, std::unique_ptr<Transaction>> &cloned_transactions,
                                        VertexContainer &cloned_vertices, EdgeContainer &cloned_edges) {
  for (auto &[commit_start, cloned_transaction] : cloned_transactions) {
    AdjustClonedTransaction(*cloned_transaction, *start_logical_id_to_transaction_[commit_start], cloned_transactions,
                            cloned_vertices, cloned_edges);
  }
}

void Splitter::AdjustClonedTransaction(Transaction &cloned_transaction, const Transaction &transaction,
                                       std::map<uint64_t, std::unique_ptr<Transaction>> &cloned_transactions,
                                       VertexContainer &cloned_vertices, EdgeContainer &cloned_edges) {
  // Align next and prev in deltas
  // NOTE It is important that the order of delta lists is in same order
  auto delta_it = transaction.deltas.begin();
  auto cloned_delta_it = cloned_transaction.deltas.begin();
  while (delta_it != transaction.deltas.end() && cloned_delta_it != cloned_transaction.deltas.end()) {
    const auto *delta = &*delta_it;
    auto *cloned_delta = &*cloned_delta_it;
    while (delta != nullptr) {
      // Align deltas which belong to cloned transaction, skip others
      if (cloned_transactions.contains(delta->commit_info->start_or_commit_timestamp.logical_id)) {
        auto *found_delta_it = &*std::ranges::find_if(
            cloned_transactions.at(delta->commit_info->start_or_commit_timestamp.logical_id)->deltas,
            [delta](const auto &elem) { return elem.uuid == delta->uuid; });
        MG_ASSERT(found_delta_it, "Delta with given uuid must exist!");
        cloned_delta->next = &*found_delta_it;
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
          // TODO(jbajic) Maybe revisit when we apply Garbage collection with new
          // transaction management system
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

}  // namespace memgraph::storage::v3
