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

  explicit VectorIndex();
  ~VectorIndex();
  VectorIndex(const VectorIndex &) = delete;
  VectorIndex &operator=(const VectorIndex &) = delete;
  VectorIndex(VectorIndex &&) noexcept;
  VectorIndex &operator=(VectorIndex &&) noexcept;

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

  /// @brief Updates the index when a label is added to a vertex.
  /// @param added_label The label that was added to the vertex.
  /// @param vertex_after_update The vertex after the label was added.
  void UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update);

  /// @brief Updates the index when a label is removed from a vertex.
  /// @param removed_label The label that was removed from the vertex.
  /// @param vertex_before_update The vertex before the label was removed.
  void UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_before_update);

  /// @brief Updates the index when a property is modified on a vertex.
  /// @param property The property that was modified.
  /// @param value The new value of the property.
  /// @param vertex The vertex on which the property was modified.
  void UpdateOnSetProperty(PropertyId property, const PropertyValue &value, Vertex *vertex);

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

 private:
  /// @brief Adds a vertex to an existing index.
  /// @param vertex The vertex to be added.
  /// @param label_prop The label and property key for the index.
  /// @param value The value of the property.
  /// @throw query::VectorSearchException
  bool UpdateVectorIndex(Vertex *vertex, const LabelPropKey &label_prop, const PropertyValue *value = nullptr);

  struct Impl;
  std::unique_ptr<Impl> pimpl;
};

}  // namespace memgraph::storage
