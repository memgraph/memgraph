// Copyright 2024 Memgraph Ltd.
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

#include <cstddef>
#include <cstdint>
#include <json/json.hpp>
#include <string>
#include "storage/v2/id_types.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/vertex.hpp"

namespace memgraph::storage {

/// @struct VectorIndexInfo
/// @brief Represents information about a vector index in the system.
///
/// This structure includes the index name, the label and property on which the index is created,
/// the dimension of the vectors in the index, and the size of the index.
struct VectorIndexInfo {
  std::string index_name;
  LabelId label;
  PropertyId property;
  std::size_t dimension;
  std::size_t size;
};

/// @struct VectorIndexSpec
/// @brief Represents a specification for creating a vector index in the system.
///
/// This structure includes the index name, the label and property on which the index is created,
/// and the configuration options for the index in the form of a JSON object.
struct VectorIndexSpec {
  std::string index_name;
  LabelId label;
  PropertyId property;
  std::string metric;
  std::string scalar;
  std::uint64_t dimension;
  std::uint64_t size_limit;
};

/// @class VectorIndex
/// @brief High-level interface for managing vector indexes.
///
/// The VectorIndex class supports creating new indexes, adding nodes to an index,
/// listing all indexes, and searching for nodes using a query vector.
/// This class is thread-safe and uses the Pimpl (Pointer to Implementation) idiom
/// to hide implementation details.
class VectorIndex {
 public:
  struct IndexStats {
    std::map<LabelId, std::vector<PropertyId>> l2p;
    std::map<PropertyId, std::vector<LabelId>> p2l;
  };

  explicit VectorIndex();
  ~VectorIndex();
  VectorIndex(const VectorIndex &) = delete;
  VectorIndex &operator=(const VectorIndex &) = delete;
  VectorIndex(VectorIndex &&) noexcept;
  VectorIndex &operator=(VectorIndex &&) noexcept;

  /// @brief Parses a string representation of an index specification.
  /// @param index_spec The nlohmann::json object representing the index specification.
  /// @param name_id_mapper The NameIdMapper instance used to map label and property names to IDs.
  /// @throws std::invalid_argument if the index specification is invalid.
  /// @return A vector of VectorIndexSpec objects representing the parsed index specifications.
  static std::vector<VectorIndexSpec> ParseIndexSpec(const nlohmann::json &index_spec, NameIdMapper *name_id_mapper);

  /// @brief Creates a new index based on the specified configuration.
  /// @param spec The specification for the index to be created.
  void CreateIndex(const VectorIndexSpec &spec);

  /// @brief Updates the index when a label is added to a vertex.
  /// @param added_label The label that was added to the vertex.
  /// @param vertex_after_update The vertex after the label was added.
  void UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update) const;

  /// @brief Updates the index when a label is removed from a vertex.
  /// @param removed_label The label that was removed from the vertex.
  /// @param vertex_before_update The vertex before the label was removed.
  void UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_before_update) const;

  /// @brief Updates the index when a property is modified on a vertex.
  /// @param property The property that was modified.
  /// @param value The new value of the property.
  /// @param vertex The vertex on which the property was modified.
  void UpdateOnSetProperty(PropertyId property, const PropertyValue &value, Vertex *vertex) const;

  /// @brief Lists the names of all existing indexes.
  /// @return A vector of strings representing the names of all indexes.
  std::vector<VectorIndexInfo> ListAllIndices() const;

  /// @brief Searches for nodes in the specified index using a query vector.
  /// @param index_name The name of the index to search.
  /// @param start_timestamp The timestamp of transaction in which the search is performed.
  /// @param result_set_size The number of results to return.
  /// @param query_vector The vector to be used for the search query.
  /// @return A vector of pairs containing the global ID (Gid) and the associated score (distance).
  std::vector<std::tuple<Vertex *, double, double>> Search(std::string_view index_name, uint64_t result_set_size,
                                                           const std::vector<float> &query_vector) const;

  /// @brief Aborts the entries that were inserted in the specified transaction.
  /// @param label The label of the vertices to be removed.
  /// @param vertices The vertices to be removed.
  void AbortEntries(const LabelPropKey &label_prop, std::span<Vertex *const> vertices) const;

  /// @brief Restores the entries that were removed in the specified transaction.
  /// @param label The label of the vertices to be restored.
  /// @param vertices The vertices to be restored.
  void RestoreEntries(const LabelPropKey &label_prop,
                      std::span<std::pair<PropertyValue, Vertex *> const> prop_vertices) const;

  void RemoveObsoleteEntries(std::stop_token token) const;

  /// @brief Returns the index statistics.
  /// @return The index statistics.
  IndexStats Analysis() const;

  /// @brief Tries to insert a vertex into the index.
  /// @param vertex The vertex to be inserted.
  void TryInsertVertex(Vertex *vertex) const;

 private:
  /// @brief Adds a vertex to an existing index.
  /// @param vertex The vertex to be added.
  /// @param label_prop The label and property key for the index.
  /// @param commit_timestamp The commit timestamp for the operation.
  void UpdateVectorIndex(Vertex *vertex, const LabelPropKey &label_prop, const PropertyValue *value = nullptr) const;

  struct Impl;
  std::unique_ptr<Impl> pimpl;
};

}  // namespace memgraph::storage
