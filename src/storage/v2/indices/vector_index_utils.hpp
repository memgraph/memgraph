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
#include "storage/v2/vertex.hpp"

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

}  // namespace memgraph::storage
