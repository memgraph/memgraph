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

#include <set>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "storage/v2/id_types.hpp"

namespace memgraph::storage {

struct IndicesInfo;
class NameIdMapper;

/// An ordinary property index or unique constraint on a property that a vector index also covers.
struct VectorPropertyConflict {
  std::string vector_index;       // e.g. "vector index vi" or "vector edge index ve"
  std::string vector_index_name;  // name as written in the DROP VECTOR INDEX hint
  std::string property;           // name of the shared property
  std::string other;              // e.g. "label+property index :L(a, emb)"
};

/// Every pair of a vector index and an ordinary index or unique constraint covering the same property.
std::vector<VectorPropertyConflict> FindVectorPropertyConflicts(
    IndicesInfo const &indices, std::span<std::pair<LabelId, std::set<PropertyId>> const> unique_constraints,
    NameIdMapper &name_id_mapper);

/// Error for creating `conflict.other` while the vector index exists.
std::string OrdinaryIndexOnVectorPropertyError(VectorPropertyConflict const &conflict);
/// Error for creating the vector index while `conflict.other` exists.
std::string VectorIndexOnIndexedPropertyError(VectorPropertyConflict const &conflict);
/// Warning logged when recovery finds both.
std::string VectorPropertyConflictWarning(VectorPropertyConflict const &conflict);

}  // namespace memgraph::storage
