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

/// @struct VectorEdgeIndexSpec
/// @brief Represents a specification for creating a vector index in the system.
///
/// This structure includes the index name, the label and property on which the index is created,
/// and the configuration options for the index in the form of a JSON object.
struct VectorEdgeIndexSpec {
  std::string index_name;
  EdgeTypeId edge_type_id;
  PropertyId property;
  unum::usearch::metric_kind_t metric_kind;
  std::uint16_t dimension;
  std::uint16_t resize_coefficient;
  std::size_t capacity;
  unum::usearch::scalar_kind_t scalar_kind;

  friend bool operator==(const VectorEdgeIndexSpec &, const VectorEdgeIndexSpec &) = default;
};

/// @struct VectorEdgeIndexInfo
/// @brief Represents information about a vector index in the system.
struct VectorEdgeIndexInfo {
  std::string index_name;
  EdgeTypeId edge_type_id;
  PropertyId property;
  std::string metric;
  std::uint16_t dimension;
  std::size_t capacity;
  std::size_t size;
  std::string scalar_kind;
};

/// @class VectoEdgerIndex
/// @brief High-level interface for managing vector edge indexes.
///
/// The VectorEdgeIndex class supports creating new indexes, adding edges to an index,
/// listing all indexes, and searching for edges using a query vector.
/// Currently, vector edge index operates in READ_UNCOMMITTED isolation level. Database can
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

  VectorEdgeIndex();
  ~VectorEdgeIndex();
  VectorEdgeIndex(VectorEdgeIndex &&) noexcept;
  VectorEdgeIndex &operator=(VectorEdgeIndex &&) noexcept;

  /// @brief Creates a new index based on the provided specification.
  /// @param spec The specification for the index to be created.
  /// @param snapshot_info
  /// @param vertices vertices from which to create vector edge index
  /// @return true if the index was created successfully, false otherwise.
  bool CreateIndex(const VectorEdgeIndexSpec &spec, utils::SkipList<Vertex>::Accessor &vertices,
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
  /// @return A vector of VectorEdgeIndexInfo objects representing the indexes.
  std::vector<VectorEdgeIndexInfo> ListVectorIndicesInfo() const;

  /// @brief Lists the labels and properties that have vector indices.
  /// @return A vector of specs representing vector indices configurations.
  std::vector<VectorEdgeIndexSpec> ListIndices() const;

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

  /// @brief Checks if a vector index exists for the given name.
  /// @param index_name The name of the index to check.
  /// @return true if the index exists, false otherwise.
  bool IndexExists(std::string_view index_name) const;

 private:
  /// @brief Adds or updates an edge in the vector index.
  /// @param entry The edge entry to be added or updated.
  /// @param edge_type_prop The edge type and property key for the index.
  /// @param value The property value to be indexed. If nullptr, the value will be taken from the edge's properties.
  /// @return true if the index was updated successfully, false otherwise.
  /// @throw query::VectorSearchException if the property is not a list or if the dimensions do not match.
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
