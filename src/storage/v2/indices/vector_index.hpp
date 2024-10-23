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

// TODO(DavIvek): The below code should be discarded and replaces with proper queries. IMPORTANT: Once we have the
// fully tested index implementation.

// The `VectorIndexSpec` structure represents a specification for creating a vector index in the system.
// It includes the index name, the label and property on which the index is created,
// and the configuration options for the index in the form of a JSON object.
struct VectorIndexSpec {
  // NOTE: The index name is required because CALL is used to query the index -> somehow we have to specify what's the
  // used index. Technically we could use only label+prop to address the right index but in practice we can have
  // multiple indexes on the same label+prop with different configs.
  std::string index_name;
  LabelId label;
  PropertyId property;
  nlohmann::json config;
};

// The `VectorIndexKey` structure is used as a key to manage nodes in the index, uniquely identifying
// an entry by a pointer to a vertex and a timestamp. Via start_timestamp we implement the MVCC logic.
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

using VectorIndexTuple = std::pair<Vertex *, LabelPropKey>;

// The `VectorIndex` class is a high-level interface for managing vector indexes.
// It supports creating new indexes, adding nodes to an index, listing all indexes,
// and searching for nodes using a query vector.
// The class is thread-safe.
// pimpl is used to hide the implementation details. Inside the class, we have a unique pointer to the implementation.
// Look into the implementation details in the vector_index.cpp file.
class VectorIndex {
 public:
  VectorIndex();
  ~VectorIndex();
  VectorIndex(const VectorIndex &) = delete;
  VectorIndex &operator=(const VectorIndex &) = delete;
  VectorIndex(VectorIndex &&) noexcept;
  VectorIndex &operator=(VectorIndex &&) noexcept;

  void CreateIndex(const VectorIndexSpec &spec);
  void AddNodeToNewIndexEntries(Vertex *vertex, std::vector<VectorIndexTuple> &keys);
  void AddNodeToIndex(Vertex *vertex, const LabelPropKey &label_prop, uint64_t commit_timestamp);
  std::vector<std::string> ListAllIndices();
  std::size_t Size(std::string_view index_name);
  std::vector<std::pair<Gid, double>> Search(std::string_view index_name, uint64_t start_timestamp,
                                             uint64_t result_set_size, const std::vector<float> &query_vector);

 private:
  struct Impl;
  std::unique_ptr<Impl> pimpl;
};

}  // namespace memgraph::storage
