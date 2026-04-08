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
                                      std::vector<VectorEdgeIndexRecoveryInfo> &recovery_info_vec);
};

/// Abstract interface for vector edge index metadata queries accessed through ActiveIndices snapshots.
struct VectorEdgeIndexActiveIndices {
  virtual ~VectorEdgeIndexActiveIndices() = default;
  virtual std::vector<VectorEdgeIndexSpec> ListIndices() const = 0;
  virtual std::vector<VectorEdgeIndexInfo> ListVectorIndicesInfo() const = 0;
  virtual std::optional<uint64_t> ApproximateEdgesVectorCount(EdgeTypeId edge_type, PropertyId property) const = 0;
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
      std::map<PropertyId, PropertyValue> properties;
    };

    using AbortableInfo = std::map<Edge *, EdgeAbortInfo>;
    AbortableInfo cleanup_collection;

    bool IsInteresting(PropertyId property) const { return p2et.contains(property); }

    void CollectOnPropertyChange(EdgeTypeId edge_type, PropertyId property, const PropertyValue &old_value,
                                 Vertex *from_vertex, Vertex *to_vertex, Edge *edge);
  };

  struct EdgeIndexEntry {
    Vertex *from_vertex;
    Vertex *to_vertex;
    Edge *edge;
  };

  using VectorSearchEdgeResults = std::vector<std::tuple<EdgeIndexEntry, double, double>>;

  /// Concrete ActiveIndices implementation that delegates to VectorEdgeIndex const methods.
  struct ActiveIndices : VectorEdgeIndexActiveIndices {
    explicit ActiveIndices(VectorEdgeIndex const *owner = nullptr) : owner_(owner) {}

    std::vector<VectorEdgeIndexSpec> ListIndices() const override;
    std::vector<VectorEdgeIndexInfo> ListVectorIndicesInfo() const override;
    std::optional<uint64_t> ApproximateEdgesVectorCount(EdgeTypeId edge_type, PropertyId property) const override;

   private:
    VectorEdgeIndex const *owner_;
  };

  VectorEdgeIndex();
  ~VectorEdgeIndex();
  VectorEdgeIndex(VectorEdgeIndex &&) noexcept;
  VectorEdgeIndex &operator=(VectorEdgeIndex &&) noexcept;

  /// Returns the current active indices snapshot for use in transactions.
  auto GetActiveIndices() -> std::shared_ptr<VectorEdgeIndexActiveIndices> {
    return std::make_shared<ActiveIndices>(this);
  }

  /// @brief Creates a new index based on the provided specification.
  bool CreateIndex(const VectorEdgeIndexSpec &spec, utils::SkipList<Vertex>::Accessor &vertices,
                   NameIdMapper *name_id_mapper,
                   std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt);

  /// @brief Recovers a vector edge index based on recovery info.
  void RecoverIndex(VectorEdgeIndexRecoveryInfo &recovery_info, utils::SkipList<Vertex>::Accessor &vertices,
                    NameIdMapper *name_id_mapper,
                    std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt);

  /// @brief Drops an existing index.
  bool DropIndex(std::string_view index_name, NameIdMapper *name_id_mapper);

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
  void AbortEntries(AbortProcessor::AbortableInfo &cleanup_collection);

  /// @brief Removes edges from the index.
  void RemoveEdges(std::vector<Edge *> const &edges_to_remove);

  /// @brief Returns an abort processor snapshot used during transaction abort.
  AbortProcessor GetAbortProcessor() const;

  /// @brief Checks if any vector index exists.
  bool Empty() const;

  /// @brief Returns the EdgeTypeId for the given index name.
  EdgeTypeId GetEdgeTypeId(std::string_view index_name);

  /// @brief Checks if a vector index exists for the given name.
  bool IndexExists(std::string_view index_name) const;

  /// @brief Retrieves the vector from an edge index entry.
  utils::small_vector<float> GetVectorPropertyFromEdgeIndex(Edge *edge, std::string_view index_name,
                                                            NameIdMapper *name_id_mapper) const;

  /// @brief Looks up the endpoint vertices for an edge in any vector index.
  std::pair<Vertex *, Vertex *> GetEdgeEndpoints(Edge *edge) const;

  /// @brief Finds the index ID for the given (edge_type, property) pair.
  std::optional<uint64_t> GetIndexIdForEdgeTypeProperty(EdgeTypeId edge_type, PropertyId property) const;

  /// @brief Gets all edge types that have vector indices for the given property.
  std::unordered_map<EdgeTypeId, uint64_t> GetIndicesByProperty(PropertyId property) const;

  /// @brief Serializes all vector edge indices to a durability encoder in one pass.
  void SerializeAllVectorEdgeIndices(durability::BaseEncoder *encoder, std::unordered_set<uint64_t> &mapped_ids) const;

 private:
  /// @brief Sets up a new vector edge index structure without populating it.
  std::optional<uint64_t> SetupIndex(const VectorEdgeIndexSpec &spec, NameIdMapper *name_id_mapper);

  /// @brief Adds a single edge to the index, converting its property to VectorIndexId.
  void AddEdgeToIndex(uint64_t index_id, Edge *edge, Vertex *from_vertex, Vertex *to_vertex,
                      std::optional<std::size_t> thread_id = std::nullopt);

  /// @brief Removes an edge from a vector index.
  void RemoveEdgeFromIndex(Edge *edge, uint64_t index_id);

  struct Impl;
  std::unique_ptr<Impl> pimpl;
};

}  // namespace memgraph::storage
