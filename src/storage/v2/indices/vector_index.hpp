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

#include <json/json.hpp>
#include <string>
#include "absl/container/flat_hash_map.h"
#include "storage/v2/id_types.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex.hpp"

namespace memgraph::storage {

// TODO(davivek): The below code should be discarded and replaces with proper queries. IMPORTANT: Once we have the
// fully tested index implementation.
struct VectorIndexSpec {
  // NOTE: The index name is required because CALL is used to query the index -> somehow we have to specify what's the
  // used index. Technically we could use only label+prop to address the right index but in practice we can have
  // multiple indexes on the same label+prop with different configs.
  std::string index_name;
  LabelId label;
  PropertyId property;
  nlohmann::json config;
};

struct VectorIndexKey {
  Vertex *vertex;
  uint64_t timestamp;

  bool operator<(const VectorIndexKey &rhs) {
    return std::make_tuple(vertex, timestamp) < std::make_tuple(rhs.vertex, rhs.timestamp);
  }
  bool operator==(const VectorIndexKey &rhs) const { return vertex == rhs.vertex && timestamp == rhs.timestamp; }
};

class VectorIndex {
 public:
  VectorIndex();
  ~VectorIndex();

  void CreateIndex(std::string const &index_name, std::vector<VectorIndexSpec> const &specs);

 private:
  struct Impl;
  std::unique_ptr<Impl> pimpl;
  // std::map<std::pair<LabelId, PropertyId>, std::unique_ptr<Impl>> indexes_;
  // absl::flat_hash_map<std::string, std::pair<LabelId, PropertyId>> index_to_label_prop_;
};

}  // namespace memgraph::storage
