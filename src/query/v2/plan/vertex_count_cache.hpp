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

/// @file
#pragma once

#include <optional>

#include "query/v2/bindings/typed_value.hpp"
#include "query/v2/shard_request_manager.hpp"
#include "storage/v3/conversions.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/property_value.hpp"
#include "utils/bound.hpp"
#include "utils/fnv.hpp"

namespace memgraph::query::v2::plan {

/// A stand in class for `TDbAccessor` which provides memoized calls to
/// `VerticesCount`.
template <class TDbAccessor>
class VertexCountCache {
 public:
  explicit VertexCountCache(TDbAccessor *shard_request_manager) : shard_request_manager_{shard_request_manager} {}

  auto NameToLabel(const std::string &name) { return shard_request_manager_->LabelNameToLabelId(name); }
  auto NameToProperty(const std::string &name) { return shard_request_manager_->NameToProperty(name); }
  auto NameToEdgeType(const std::string & /*name*/) {
    MG_ASSERT(false, "NameToEdgeType");
    return storage::v3::EdgeTypeId::FromInt(0);
  }

  int64_t VerticesCount() { return 1; }

  int64_t VerticesCount(storage::v3::LabelId /*label*/) { return 1; }

  int64_t VerticesCount(storage::v3::LabelId /*label*/, storage::v3::PropertyId /*property*/) { return 1; }

  int64_t VerticesCount(storage::v3::LabelId /*label*/, storage::v3::PropertyId /*property*/,
                        const storage::v3::PropertyValue & /*value*/) {
    return 1;
  }

  int64_t VerticesCount(storage::v3::LabelId /*label*/, storage::v3::PropertyId /*property*/,
                        const std::optional<utils::Bound<storage::v3::PropertyValue>> & /*lower*/,
                        const std::optional<utils::Bound<storage::v3::PropertyValue>> & /*upper*/) {
    return 1;
  }

  bool LabelIndexExists(storage::v3::LabelId /*label*/) { return false; }

  bool LabelPropertyIndexExists(storage::v3::LabelId /*label*/, storage::v3::PropertyId /*property*/) { return false; }

  msgs::ShardRequestManagerInterface *shard_request_manager_;
};

template <class TDbAccessor>
auto MakeVertexCountCache(TDbAccessor *db) {
  return VertexCountCache<TDbAccessor>(db);
}

}  // namespace memgraph::query::v2::plan
