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

#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/vector_index_utils.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/snapshot_observer_info.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::storage {

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
  std::vector<std::vector<float>> vectors;
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
  struct IndexStats {
    std::map<LabelId, std::vector<PropertyId>> l2p;
    std::map<PropertyId, std::vector<LabelId>> p2l;
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
                   std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt);

  void CreateIndex(const VectorIndexSpec &spec);

  /// @brief Drops an existing index.
  /// @param index_name The name of the index to be dropped.
  /// @return true if the index was dropped successfully, false otherwise.
  bool DropIndex(std::string_view index_name);

  /// @brief Drops all existing indexes.
  void Clear();

  /// @brief Updates the index when a property is modified on a vertex.
  /// @param property The property that was modified.
  /// @param value The new value of the property.
  /// @param vertex The vertex on which the property was modified.
  void UpdateOnSetProperty(const PropertyValue &value, Vertex *vertex, NameIdMapper *name_id_mapper);

  /// @brief Updates the index when a property is modified on a vertex.
  /// @param value The new value of the property.
  /// @param vertex The vertex on which the property was modified.
  /// @param index_name The name of the index to update.
  /// @return The vector of the vertex as a PropertyValue.
  std::vector<float> UpdateIndex(const PropertyValue &value, Vertex *vertex, std::string_view index_name);

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

  /// @brief Aborts the entries that were inserted in the specified transaction.
  /// @param label_prop The label of the vertices to be removed.
  /// @param vertices The vertices to be removed.
  void AbortEntries(const LabelPropKey &label_prop, std::span<Vertex *const> vertices);

  /// @brief Restores the entries that were removed in the specified transaction.
  /// @param label_prop The label and property of the vertices to be restored.
  /// @param prop_vertices The vertices to be restored.
  void RestoreEntries(const LabelPropKey &label_prop,
                      std::span<std::pair<PropertyValue, Vertex *> const> prop_vertices);

  /// @brief Removes obsolete entries from the index.
  /// @param token A stop token to allow for cancellation of the operation.
  void RemoveObsoleteEntries(std::stop_token token) const;

  /// @brief Returns the index statistics.
  /// @return The index statistics.
  IndexStats Analysis() const;

  /// @brief Checks if a vector index exists for the given name.
  /// @param index_name The name of the index to check.
  /// @return true if the index exists, false otherwise.
  bool IndexExists(std::string_view index_name) const;

  /// @brief Checks if the property is in the vector index.
  /// @param vertex The vertex to check.
  /// @param property The property to check.
  /// @return true if the property is in the vector index, false otherwise.
  std::optional<std::vector<uint64_t>> IsPropertyInVectorIndex(Vertex *vertex, PropertyId property,
                                                               NameIdMapper *name_id_mapper);

  /// @brief Gets all properties that have vector indices for the given label.
  /// @param label The label to get the properties for.
  /// @return A map of property ids to index names.
  std::unordered_map<PropertyId, std::string> GetProperties(LabelId label) const;

 private:
  /// @brief Gets all label-property combinations that match the given vertex and property.
  /// @param vertex The vertex to check labels against.
  /// @param property The property to match.
  /// @return A vector of matching LabelPropKey objects.
  std::vector<LabelPropKey> GetMatchingLabelProps(std::span<LabelId const> labels,
                                                  std::span<PropertyId const> properties) const;

  struct Impl;
  std::unique_ptr<Impl> pimpl;
};

}  // namespace memgraph::storage
