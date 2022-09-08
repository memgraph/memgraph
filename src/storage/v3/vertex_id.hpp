// Copyright 2022 Memgraph Ltd.
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

#include "storage/v3/id_types.hpp"
#include "storage/v3/key_store.hpp"

namespace memgraph::storage::v3 {

// TODO(antaljanosbenjamin): It is possible to use a union of the current primary key and a vertex pointer: for local
// vertices we can spare some space by eliminating copying the primary label and key, however it might introduce some
// overhead for "remove vertices", because of the extra enum that is necessary for this optimization.
struct VertexId {
  VertexId(const LabelId primary_label, PrimaryKey primary_key)
      : primary_label{primary_label}, primary_key{std::move(primary_key)} {}
  LabelId primary_label;
  PrimaryKey primary_key;
};

inline bool operator==(const VertexId &lhs, const VertexId &rhs) {
  return lhs.primary_label == rhs.primary_label && lhs.primary_key == rhs.primary_key;
}
}  // namespace memgraph::storage::v3
