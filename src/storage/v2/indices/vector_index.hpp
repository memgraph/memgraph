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

#include <cstdint>
#include <json/json.hpp>
#include <string>
#include "storage/v2/id_types.hpp"
#include "storage/v2/vertex.hpp"

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_string(experimental_vector_indexes);
namespace memgraph::storage {

/// @struct VectorIndexSpec
/// @brief Represents a specification for creating a vector index in the system.
///
/// This structure includes the index name, the label and property on which the index is created,
/// and the configuration options for the index in the form of a JSON object.
struct VectorIndexSpec {
  std::string index_name;
  LabelId label;
  PropertyId property;
  nlohmann::json config;
};

/// @struct VectorIndexKey
/// @brief Represents a key for the vector index.
///
/// The key consists of a vertex and a commit timestamp. We use commit_timestamp to ensure that the transaction can
/// see only nodes that were committed before the start_timestamp.
struct VectorIndexKey {
  Vertex *vertex;
  uint64_t commit_timestamp;

  bool operator<(const VectorIndexKey &rhs) {
    return std::make_tuple(vertex, commit_timestamp) < std::make_tuple(rhs.vertex, rhs.commit_timestamp);
  }
  bool operator==(const VectorIndexKey &rhs) const {
    return vertex == rhs.vertex && commit_timestamp == rhs.commit_timestamp;
  }
};

/// We use a pair of Vertex and LabelPropKey to represent in which index the node should be added.
/// Take a look at transaction.hpp for more details.
using VectorIndexTuple = std::pair<Vertex *, LabelPropKey>;

/// @class VectorIndex
/// @brief High-level interface for managing vector indexes.
///
/// The VectorIndex class supports creating new indexes, adding nodes to an index,
/// listing all indexes, and searching for nodes using a query vector.
/// This class is thread-safe and uses the Pimpl (Pointer to Implementation) idiom
/// to hide implementation details.
class VectorIndex {
 public:
  VectorIndex();
  ~VectorIndex();
  VectorIndex(const VectorIndex &) = delete;
  VectorIndex &operator=(const VectorIndex &) = delete;
  VectorIndex(VectorIndex &&) noexcept;
  VectorIndex &operator=(VectorIndex &&) noexcept;

  /// @brief Creates a new index based on the specified configuration.
  /// @param spec The specification for the index to be created.
  void CreateIndex(const VectorIndexSpec &spec);

  /// @brief Checks to which index the node should be added and adds it to associated keys vector.
  /// @param vertex The vertex to be added to the associated vector index keys.
  /// @param keys The vector of keys associated with the index.
  void AddNodeToNewIndexEntries(Vertex *vertex, std::vector<VectorIndexTuple> &keys);

  /// @brief Adds a vertex to an existing index.
  /// @param vertex The vertex to be added.
  /// @param label_prop The label and property key for the index.
  /// @param commit_timestamp The commit timestamp for the operation.
  void AddNodeToIndex(Vertex *vertex, const LabelPropKey &label_prop, uint64_t commit_timestamp);

  /// @brief Lists the names of all existing indexes.
  /// @return A vector of strings representing the names of all indexes.
  std::vector<std::string> ListAllIndices() const;

  /// @brief Returns the size of the specified index.
  /// @param index_name The name of the index.
  /// @return The size of the index as a `std::size_t`.
  std::size_t Size(std::string_view index_name) const;

  /// @brief Searches for nodes in the specified index using a query vector.
  /// @param index_name The name of the index to search.
  /// @param start_timestamp The timestamp of transaction in which the search is performed.
  /// @param result_set_size The number of results to return.
  /// @param query_vector The vector to be used for the search query.
  /// @return A vector of pairs containing the global ID (Gid) and the associated score (distance).
  std::vector<std::pair<Gid, double>> Search(std::string_view index_name, uint64_t start_timestamp,
                                             uint64_t result_set_size, const std::vector<float> &query_vector) const;

 private:
  struct Impl;
  std::unique_ptr<Impl> pimpl;
};

}  // namespace memgraph::storage
