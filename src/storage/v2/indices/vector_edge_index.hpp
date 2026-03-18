// Copyright 2026 Memgraph Ltd.
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

#include <list>
#include <map>
#include "storage/v2/durability/serialization.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/vector_index_utils.hpp"
#include "storage/v2/snapshot_observer_info.hpp"
#include "storage/v2/vertex.hpp"

namespace memgraph::storage {

struct Indices;
class NameIdMapper;

/// @struct VectorEdgeIndexSpec
/// @brief Represents a specification for creating a vector index in the system.
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

/// @struct VectorEdgeIndexRecoveryInfo
/// @brief Recovery information for a vector edge index, including entry data.
struct VectorEdgeIndexRecoveryInfo {
  VectorEdgeIndexSpec spec;
  absl::flat_hash_map<Gid, utils::small_vector<float>> index_entries;
};

/// @struct VectorEdgeIndexRecovery
/// @brief Handles recovery operations for vector edge indices during WAL replay and snapshot recovery.
struct VectorEdgeIndexRecovery {
  static void UpdateOnIndexDrop(std::string_view index_name, NameIdMapper *name_id_mapper,
                                std::vector<VectorEdgeIndexRecoveryInfo> &recovery_info_vec,
                                utils::SkipList<Vertex>::Accessor &vertices);

  static void UpdateOnSetEdgeProperty(PropertyId property, const PropertyValue &value, const Edge *edge,
                                      std::vector<VectorEdgeIndexRecoveryInfo> &recovery_info_vec,
                                      NameIdMapper *name_id_mapper);

  static utils::small_vector<float> ExtractVectorForRecovery(
      const PropertyValue &value, const Edge *edge, const std::vector<VectorEdgeIndexRecoveryInfo> &recovery_info_vec,
      NameIdMapper *name_id_mapper);
};

/// @class VectorEdgeIndex
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
  struct AbortProcessor {
    std::map<EdgeTypeId, std::vector<PropertyId>> et2p;
    std::map<PropertyId, std::vector<EdgeTypeId>> p2et;

    struct EdgeAbortInfo {
      EdgeTypeId edge_type;
      Vertex *from_vertex;
      Vertex *to_vertex;
      // Map from PropertyId to (old_PropertyValue, saved_vector).
      // The saved_vector contains the actual float data since PropertyStore doesn't cache it.
      std::map<PropertyId, std::pair<PropertyValue, utils::small_vector<float>>> properties;
    };

    using AbortableInfo = std::map<Edge *, EdgeAbortInfo>;
    AbortableInfo cleanup_collection;

    void CollectOnPropertyChange(EdgeTypeId edge_type, PropertyId property, const PropertyValue &old_value,
                                 const utils::small_vector<float> &saved_vector, Vertex *from_vertex, Vertex *to_vertex,
                                 Edge *edge);
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
  bool CreateIndex(const VectorEdgeIndexSpec &spec, utils::SkipList<Vertex>::Accessor &vertices, Indices *indices,
                   NameIdMapper *name_id_mapper,
                   std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt);

  /// @brief Recovers a vector edge index based on recovery info.
  void RecoverIndex(VectorEdgeIndexRecoveryInfo &recovery_info, utils::SkipList<Vertex>::Accessor &vertices,
                    Indices *indices, NameIdMapper *name_id_mapper,
                    std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt);

  /// @brief Drops an existing index.
  bool DropIndex(std::string_view index_name, utils::SkipList<Vertex>::Accessor &vertices,
                 NameIdMapper *name_id_mapper);

  /// @brief Drops all existing indexes.
  void Clear();

  void UpdateOnSetProperty(Vertex *from_vertex, Vertex *to_vertex, Edge *edge, EdgeTypeId edge_type,
                           PropertyId property, const PropertyValue &value);

  /// @brief Lists the info of all existing indexes.
  std::vector<VectorEdgeIndexInfo> ListVectorIndicesInfo() const;

  /// @brief Lists the labels and properties that have vector indices.
  std::vector<VectorEdgeIndexSpec> ListIndices() const;

  /// @brief Returns number of edges in the index.
  std::optional<uint64_t> ApproximateEdgesVectorCount(EdgeTypeId edge_type, PropertyId property) const;

  /// @brief Searches for edges in the specified index using a query vector.
  VectorSearchEdgeResults SearchEdges(std::string_view index_name, uint64_t result_set_size,
                                      const std::vector<float> &query_vector) const;

  /// @brief Aborts the entries in the vector edge index.
  void AbortEntries(Indices *indices, NameIdMapper *name_id_mapper, AbortProcessor::AbortableInfo &cleanup_collection);

  /// @brief Removes edges from the index by GID.
  void RemoveEdges(std::list<Gid> const &deleted_edge_gids) const;

  /// @brief Returns an abort processor snapshot used during transaction abort.
  AbortProcessor GetAbortProcessor() const;

  /// @brief Checks if any vector index exists.
  bool Empty() const;

  /// @brief Returns the EdgeTypeId for the given index name.
  EdgeTypeId GetEdgeTypeId(std::string_view index_name);

  /// @brief Checks if a vector index exists for the given name.
  bool IndexExists(std::string_view index_name) const;

  /// @brief Retrieves the vector from an edge index entry.
  utils::small_vector<float> GetVectorPropertyFromEdgeIndex(Edge *edge, Vertex *from_vertex, Vertex *to_vertex,
                                                            std::string_view index_name,
                                                            NameIdMapper *name_id_mapper) const;

  /// @brief Gets all properties that have vector indices for the given edge type.
  std::unordered_map<PropertyId, uint64_t> GetIndicesByEdgeType(EdgeTypeId edge_type) const;

  /// @brief Gets all edge types that have vector indices for the given property.
  std::unordered_map<EdgeTypeId, uint64_t> GetIndicesByProperty(PropertyId property) const;

  /// @brief Pops a saved vector from the removed vectors cache (used during abort).
  utils::small_vector<float> PopRemovedVector(Gid edge_gid, PropertyId property);

  /// @brief Serializes all vector edge indices to a durability encoder in one pass.
  void SerializeAllVectorEdgeIndices(durability::BaseEncoder *encoder, std::unordered_set<uint64_t> &mapped_ids) const;

 private:
  /// @brief Sets up a new vector edge index structure without populating it.
  std::optional<uint64_t> SetupIndex(const VectorEdgeIndexSpec &spec, NameIdMapper *name_id_mapper);

  /// @brief Removes an edge from a vector index.
  void RemoveEdgeFromIndex(Edge *edge, Vertex *from_vertex, Vertex *to_vertex, uint64_t index_id);

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
