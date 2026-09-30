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

#include "storage/v2/delta.hpp"

#include "storage/v2/property_value.hpp"

namespace memgraph::storage {

Delta::Delta(SetPropertyTag /*tag*/, PropertyId key, PropertyValue const &value, CommitInfo *commit_info,
             uint64_t command_id, utils::PageSlabMemoryResource *res)
    : commit_info(commit_info),
      command_id(command_id),
      property{.action = Action::SET_PROPERTY,
               .key = key,
               .value = std::pmr::polymorphic_allocator<Delta>{res}.new_object<pmr::PropertyValue>(value)} {}

Delta::Delta(SetPropertyTag /*tag*/, Vertex *out_vertex, PropertyId key, PropertyValue value, CommitInfo *commit_info,
             uint64_t command_id, utils::PageSlabMemoryResource *res)
    : commit_info(commit_info),
      command_id(command_id),
      property{.action = Action::SET_PROPERTY,
               .key = key,
               .value = std::pmr::polymorphic_allocator<Delta>{res}.new_object<pmr::PropertyValue>(std::move(value)),
               .out_vertex = out_vertex} {}

}  // namespace memgraph::storage
