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

#include <cstdint>
#include <unordered_map>

#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/snapshot_observer_info.hpp"
#include "storage/v2/vertex.hpp"
#include "usearch/index_dense.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::storage {

using mg_vector_index_t = unum::usearch::index_dense_gt<Vertex *, unum::usearch::uint40_t>;

/// @struct VectorIndexInfo
/// @brief Represents information about a vector index in the system.
///
/// This structure includes the index name, the label and property on which the index is created,
/// the dimension of the vectors in the index, and the size of the index.
struct VectorIndexInfo {
  std::string index_name;
  LabelId label_id;
  PropertyId property;
  std::string metric;
  std::uint16_t dimension;
  std::size_t capacity;
  std::size_t size;
  std::string scalar_kind;
};

/// @struct VectorIndexSpec
/// @brief Represents a specification for creating a vector index in the system.
///
/// This structure includes the index name, the label and property on which the index is created,
/// and the configuration options for the index in the form of a JSON object.
struct VectorIndexSpec {
  std::string index_name;
  LabelId label_id;
  PropertyId property;
  unum::usearch::metric_kind_t metric_kind;
  std::uint16_t dimension;
  std::uint16_t resize_coefficient;
  std::size_t capacity;
  unum::usearch::scalar_kind_t scalar_kind;

  friend bool operator==(const VectorIndexSpec &, const VectorIndexSpec &) = default;
};

struct VectorIndexRecoveryInfo {
  VectorIndexSpec spec;
  std::unordered_map<Gid, std::vector<float>> index_entries;
};

// Forward declaration
class NameIdMapper;

/// @struct VectorIndexRecovery
/// @brief Handles recovery operations for vector indices during WAL replay and snapshot recovery.
///
/// This struct encapsulates all recovery-related operations for vector indices,
/// separating recovery logic from the main VectorIndex class for better
/// separation of concerns and testability.
struct VectorIndexRecovery {
  /// @brief Updates recovery info when an index is dropped.
  /// @param index_name The name of the index being dropped.
  /// @param name_id_mapper Mapper for name/ID conversions.
  /// @param recovery_info_vec The vector of recovery info to update.
  /// @param vertices Accessor to the vertices skip list.
  static void UpdateOnIndexDrop(std::string_view index_name, NameIdMapper *name_id_mapper,
                                std::vector<VectorIndexRecoveryInfo> &recovery_info_vec,
                                utils::SkipList<Vertex>::Accessor &vertices);

  /// @brief Updates recovery info when a label is added to a vertex.
  /// @param label The label being added.
  /// @param vertex The vertex receiving the label.
  /// @param name_id_mapper Mapper for name/ID conversions.
  /// @param recovery_info_vec The vector of recovery info to update.
  static void UpdateOnLabelAddition(LabelId label, Vertex *vertex, NameIdMapper *name_id_mapper,
                                    std::vector<VectorIndexRecoveryInfo> &recovery_info_vec);

  /// @brief Updates recovery info when a label is removed from a vertex.
  /// @param label The label being removed.
  /// @param vertex The vertex losing the label.
  /// @param name_id_mapper Mapper for name/ID conversions.
  /// @param recovery_info_vec The vector of recovery info to update.
  static void UpdateOnLabelRemoval(LabelId label, Vertex *vertex, NameIdMapper *name_id_mapper,
                                   std::vector<VectorIndexRecoveryInfo> &recovery_info_vec);

  /// @brief Updates recovery info when a property changes on a vertex.
  /// @param property The property that changed.
  /// @param value The new property value.
  /// @param vertex The vertex with the changed property.
  /// @param recovery_info_vec The vector of recovery info to update.
  static void UpdateOnPropertyChange(PropertyId property, PropertyValue &value, Vertex *vertex,
                                     std::vector<VectorIndexRecoveryInfo> &recovery_info_vec);

 private:
  /// @brief Finds all recovery info entries matching a given label.
  /// @param label The label to match.
  /// @param recovery_info_vec The vector of recovery info to search.
  /// @return Vector of pointers to matching recovery info entries.
  static std::vector<VectorIndexRecoveryInfo *> FindMatchingIndices(
      LabelId label, std::vector<VectorIndexRecoveryInfo> &recovery_info_vec);

  /// @brief Extracts a vector from a property value, handling VectorIndexId cases.
  /// @param value The property value to extract from.
  /// @param vertex The vertex containing the property.
  /// @param recovery_info_vec The recovery info vector to look up vectors in.
  /// @param name_id_mapper Mapper for name/ID conversions.
  /// @return The extracted vector of floats.
  static std::vector<float> ExtractVectorForRecovery(const PropertyValue &value, Vertex *vertex,
                                                     const std::vector<VectorIndexRecoveryInfo> &recovery_info_vec,
                                                     NameIdMapper *name_id_mapper);
};

/// @class VectorIndex
/// @brief High-level interface for managing vector indexes.
///
/// The VectorIndex class supports creating new indexes, adding nodes to an index,
/// listing all indexes, and searching for nodes using a query vector.
/// Currently, vector index operates in READ_UNCOMMITTED isolation level. Database can
/// still operate in any other isolation level.
/// This class is thread-safe and uses the Pimpl (Pointer to Implementation) idiom
/// to hide implementation details.
class VectorIndex {
 public:
  using LabelToAdd = std::set<LabelId>;
  using LabelToRemove = std::set<LabelId>;
  using PropertyToAbort = std::map<PropertyId, PropertyValue>;
  using AbortableInfo = std::map<Vertex *, std::tuple<LabelToAdd, LabelToRemove, PropertyToAbort>>;
  struct AbortProcessor {
    std::map<LabelId, std::vector<PropertyId>> l2p;
    std::map<PropertyId, std::vector<LabelId>> p2l;

    void CollectOnLabelRemoval(LabelId label, Vertex *vertex);
    void CollectOnLabelAddition(LabelId label, Vertex *vertex);
    void CollectOnPropertyChange(PropertyId propId, const PropertyValue &old_value, Vertex *vertex);

    AbortableInfo cleanup_collection;
  };

  using VectorSearchNodeResults = std::vector<std::tuple<Vertex *, double, double>>;

  VectorIndex();
  ~VectorIndex();
  VectorIndex(VectorIndex &&) noexcept;
  VectorIndex &operator=(VectorIndex &&) noexcept;

  /// @brief Creates a new index based on the provided specification.
  /// @param spec The specification for the index to be created.
  /// @param snapshot_info
  /// @param vertices vertices from which to create vector index
  /// @return true if the index was created successfully, false otherwise.
  bool CreateIndex(const VectorIndexSpec &spec, utils::SkipList<Vertex>::Accessor &vertices,
                   NameIdMapper *name_id_mapper,
                   std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt);

  void RecoverIndexEntries(const VectorIndexRecoveryInfo &recovery_info, utils::SkipList<Vertex>::Accessor &vertices,
                           NameIdMapper *name_id_mapper);

  /// @brief Drops an existing index.
  /// @param index_name The name of the index to be dropped.
  /// @return true if the index was dropped successfully, false otherwise.
  bool DropIndex(std::string_view index_name, utils::SkipList<Vertex>::Accessor &vertices,
                 NameIdMapper *name_id_mapper);

  /// @brief Drops all existing indexes.
  void Clear();

  void RemoveNode(Vertex *vertex);

  void UpdateOnAddLabel(LabelId label, Vertex *vertex, NameIdMapper *name_id_mapper);

  void UpdateOnRemoveLabel(LabelId label, Vertex *vertex, NameIdMapper *name_id_mapper);

  void AbortEntries(NameIdMapper *name_id_mapper, AbortableInfo &cleanup_collection);

  /// @brief Updates the vector index when a property is modified on a vertex.
  /// @param value The new value of the property.
  /// @param vertex The vertex on which the property was modified.
  /// @param index_name Optional name of the index to update. If not provided and value is VectorIndexId, updates all
  /// indices.
  /// @param name_id_mapper Mapper for name/ID conversions.
  void UpdateIndex(const PropertyValue &value, Vertex *vertex, std::optional<std::string_view> index_name,
                   NameIdMapper *name_id_mapper);

  /// @brief Retrieves the vector of a vertex as a PropertyValue.
  /// @param vertex The vertex to retrieve the vector from.
  /// @param index_name The name of the index to retrieve the vector from.
  /// @return The vector of the vertex as a PropertyValue.
  PropertyValue GetPropertyValue(Vertex *vertex, std::string_view index_name) const;

  /// @brief Retrieves the vector of a vertex as a list of float values.
  /// @param vertex The vertex to retrieve the vector from.
  /// @param index_name The name of the index to retrieve the vector from.
  /// @return The vector of the vertex as a list of float values.
  std::vector<float> GetVectorProperty(Vertex *vertex, std::string_view index_name) const;

  /// @brief Lists the info of all existing indexes.
  /// @return A vector of VectorIndexInfo objects representing the indexes.
  std::vector<VectorIndexInfo> ListVectorIndicesInfo() const;

  /// @brief Lists the labels and properties that have vector indices.
  /// @return A vector of specs representing vector indices configurations.
  std::vector<VectorIndexSpec> ListIndices() const;

  /// @brief Returns number of vertices in the index.
  /// @param label The label of the vertices in the index.
  /// @param property The property of the vertices in the index.
  /// @return The number of vertices in the index.
  std::optional<uint64_t> ApproximateNodesVectorCount(LabelId label, PropertyId property) const;

  /// @brief Searches for nodes in the specified index using a query vector.
  /// @param index_name The name of the index to search.
  /// @param result_set_size The number of results to return.
  /// @param query_vector The vector to be used for the search query.
  /// @return A vector of tuples containing the vertex, distance, and similarity of the search results.
  VectorSearchNodeResults SearchNodes(std::string_view index_name, uint64_t result_set_size,
                                      const std::vector<float> &query_vector) const;

  /// @brief Returns an abort processor snapshot used during transaction abort.
  /// @return AbortProcessor containing label/property mappings for vector indices.
  AbortProcessor GetAbortProcessor() const;

  /// @brief Checks if a vector index exists for the given name.
  /// @param index_name The name of the index to check.
  /// @return true if the index exists, false otherwise.
  bool IndexExists(std::string_view index_name) const;

  /// @brief Checks if the property is in the vector index.
  /// @param vertex The vertex to check.
  /// @param property The property to check.
  /// @return true if the property is in the vector index, false otherwise.
  utils::small_vector<uint64_t> GetVectorIndexIdsForVertex(Vertex *vertex, PropertyId property,
                                                           NameIdMapper *name_id_mapper);

  /// @brief Gets all properties that have vector indices for the given label.
  /// @param label The label to get the properties for.
  /// @return A map of property ids to index names.
  std::unordered_map<PropertyId, std::string> GetProperties(LabelId label) const;

  /// @brief Gets all labels that have vector indices for the given property.
  /// @param property The property to get the labels for.
  /// @return A map of label ids to index names.
  std::unordered_map<LabelId, std::string> GetLabels(PropertyId property) const;

 private:
  void RemoveVertexFromIndex(Vertex *vertex, std::string_view index_name);

  /// @brief Creates the index structure (metric, index, and internal maps) without populating it.
  /// @param spec The specification for the index to be created.
  /// @return A pair containing the created index and the label_prop key.
  /// @throws query::VectorSearchException if the index already exists or creation fails.
  std::pair<mg_vector_index_t, LabelPropKey> CreateIndexStructure(const VectorIndexSpec &spec);

  /// @brief Populates an index with vertices from the skip list (used by both CreateIndex and RecoverIndexEntries).
  /// @param mg_vector_index The index to populate.
  /// @param spec The index specification.
  /// @param vertices Accessor to the vertices skip list.
  /// @param name_id_mapper Mapper for name/ID conversions.
  /// @param recovery_entries Optional map of recovery entries (nullptr for create, non-null for recovery).
  static void PopulateIndexFromVerticesForCreate(mg_vector_index_t &mg_vector_index, const VectorIndexSpec &spec,
                                                 utils::SkipList<Vertex>::Accessor &vertices,
                                                 NameIdMapper *name_id_mapper);
  static void PopulateIndexFromVerticesForRecovery(mg_vector_index_t &mg_vector_index, const VectorIndexSpec &spec,
                                                   utils::SkipList<Vertex>::Accessor &vertices,
                                                   NameIdMapper *name_id_mapper,
                                                   const std::unordered_map<Gid, std::vector<float>> &recovery_entries);

  struct Impl;
  std::unique_ptr<Impl> pimpl;
};

}  // namespace memgraph::storage
