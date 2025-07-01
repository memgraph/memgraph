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

#pragma once

#include <map>
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/vector_index_utils.hpp"
#include "storage/v2/snapshot_observer_info.hpp"
#include "storage/v2/vertex.hpp"

namespace memgraph::storage {

/// @class VectorIndex
/// @brief High-level interface for managing vector indexes.
///
/// The VectorIndex class supports creating new indexes, adding nodes to an index,
/// listing all indexes, and searching for nodes using a query vector.
/// Currently, vector index operates in READ_UNCOMMITTED isolation level. Database can
/// still operate in any other isolation level.
/// This class is thread-safe and uses the Pimpl (Pointer to Implementation) idiom
/// to hide implementation details.
class VectorEdgeIndex {
 public:
  struct IndexStats {
    std::map<EdgeTypeId, std::vector<PropertyId>> et2p;
    std::map<PropertyId, std::vector<EdgeTypeId>> p2et;
  };
  struct EdgeIndexEntry {
    Vertex *from_vertex;
    Vertex *to_vertex;
    Edge *edge;

    friend bool operator<(EdgeIndexEntry const &lhs, EdgeIndexEntry const &rhs) {
      return std::tie(lhs.edge, lhs.from_vertex, lhs.to_vertex) < std::tie(rhs.edge, rhs.from_vertex, rhs.to_vertex);
    }

    friend bool operator==(EdgeIndexEntry const &lhs, EdgeIndexEntry const &rhs) {
      return std::tie(lhs.edge, lhs.from_vertex, lhs.to_vertex) == std::tie(rhs.edge, rhs.from_vertex, rhs.to_vertex);
    }
  };

  using VectorSearchEdgeResults = std::vector<std::tuple<EdgeIndexEntry, double, double>>;

  explicit VectorEdgeIndex();
  ~VectorEdgeIndex();
  VectorEdgeIndex(const VectorEdgeIndex &) = delete;
  VectorEdgeIndex &operator=(const VectorEdgeIndex &) = delete;
  VectorEdgeIndex(VectorEdgeIndex &&) noexcept;
  VectorEdgeIndex &operator=(VectorEdgeIndex &&) noexcept;

  /// @brief Creates a new index based on the provided specification.
  /// @param spec The specification for the index to be created.
  /// @param snapshot_info
  /// @param vertices vertices from which to create vector index
  /// @return true if the index was created successfully, false otherwise.
  bool CreateIndex(const VectorIndexSpec &spec, utils::SkipList<Vertex>::Accessor &vertices,
                   std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt);

  /// @brief Drops an existing index.
  /// @param index_name The name of the index to be dropped.
  /// @return true if the index was dropped successfully, false otherwise.
  bool DropIndex(std::string_view index_name);

  /// @brief Drops all existing indexes.
  void Clear();

  void UpdateOnSetProperty(Vertex *from_vertex, Vertex *to_vertex, Edge *edge, EdgeTypeId edge_type,
                           PropertyId property, const PropertyValue &value);

  /// @brief Lists the info of all existing indexes.
  /// @return A vector of VectorIndexInfo objects representing the indexes.
  std::vector<VectorIndexInfo> ListVectorIndicesInfo() const;

  /// @brief Lists the labels and properties that have vector indices.
  /// @return A vector of specs representing vector indices configurations.
  std::vector<VectorIndexSpec> ListIndices() const;

  /// @brief Returns number of edges in the index.
  /// @param edge_type The type of the edges in the index.
  /// @param property The property of the edges in the index.
  /// @return The number of edges in the index.
  std::optional<uint64_t> ApproximateEdgesVectorCount(EdgeTypeId edge_type, PropertyId property) const;

  /// @brief Searches for edges in the specified index using a query vector.
  /// @param index_name The name of the index to search.
  /// @param result_set_size The number of results to return.
  /// @param query_vector The vector to be used for the search query.
  /// @return A vector of tuples containing the edge, distance, and similarity of the search results.
  VectorSearchEdgeResults SearchEdges(std::string_view index_name, uint64_t result_set_size,
                                      const std::vector<float> &query_vector) const;

  /// @brief Restores the entries that were removed in the specified transaction.
  /// @param edge_type_prop The label and property of the vertices to be restored.
  /// @param prop_edges The edges to be restored.
  void RestoreEntries(
      const EdgeTypePropKey &edge_type_prop,
      std::span<std::pair<PropertyValue, std::tuple<Vertex *const, Vertex *const, Edge *const>> const> prop_edges);

  /// @brief Removes obsolete entries from the index.
  /// @param token A stop token to allow for cancellation of the operation.
  void RemoveObsoleteEntries(std::stop_token token) const;

  /// @brief Returns the index statistics.
  /// @return The index statistics.
  IndexStats Analysis() const;

  /// @brief Returns the EdgeTypeId for the given index name.
  /// @param index_name The name of the index.
  /// @return The EdgeTypeId associated with the index name.
  /// @throw query::VectorSearchException if the index does not exist.
  EdgeTypeId GetEdgeTypeId(std::string_view index_name);

 private:
  /// @brief Adds a vertex to an existing index.
  /// @param vertex The vertex to be added.
  /// @param label_prop The label and property key for the index.
  /// @param value The value of the property.
  /// @throw query::VectorSearchException
  bool UpdateVectorIndex(EdgeIndexEntry entry, const EdgeTypePropKey &edge_type_prop,
                         const PropertyValue *value = nullptr);

  struct Impl;
  std::unique_ptr<Impl> pimpl;
};

}  // namespace memgraph::storage

namespace std {
template <>
struct hash<memgraph::storage::VectorEdgeIndex::EdgeIndexEntry> {
  size_t operator()(const memgraph::storage::VectorEdgeIndex::EdgeIndexEntry &entry) const noexcept {
    std::size_t seed = 0;
    boost::hash_combine(seed, entry.from_vertex);
    boost::hash_combine(seed, entry.to_vertex);
    boost::hash_combine(seed, entry.edge);
    return seed;
  }
};
}  // namespace std
