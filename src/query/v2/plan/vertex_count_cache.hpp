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

#include <iterator>
#include <optional>

#include "query/v2/bindings/typed_value.hpp"
#include "query/v2/plan/preprocess.hpp"
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

  auto NameToLabel(const std::string &name) { return shard_request_manager_->NameToLabel(name); }
  auto NameToProperty(const std::string &name) { return shard_request_manager_->NameToProperty(name); }
  auto NameToEdgeType(const std::string &name) { return shard_request_manager_->NameToEdgeType(name); }

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

  bool LabelIndexExists(storage::v3::LabelId label) { return shard_request_manager_->IsPrimaryLabel(label); }

  bool LabelPropertyIndexExists(storage::v3::LabelId /*label*/, storage::v3::PropertyId /*property*/) { return false; }

  std::vector<std::pair<query::v2::Expression *, query::v2::plan::FilterInfo>> ExtractPrimaryKey(
      storage::v3::LabelId label, std::vector<query::v2::plan::FilterInfo> property_filters) {
    std::vector<std::pair<query::v2::Expression *, query::v2::plan::FilterInfo>> pk;
    const auto schema = shard_request_manager_->GetSchemaForLabel(label);

    std::vector<storage::v3::PropertyId> schema_properties;
    schema_properties.reserve(schema.size());

    std::transform(schema.begin(), schema.end(), std::back_inserter(schema_properties),
                   [](const auto &schema_elem) { return schema_elem.property_id; });

    for (const auto &property_filter : property_filters) {
      const auto &property_id = NameToProperty(property_filter.property_filter->property_.name);
      if (std::find(schema_properties.begin(), schema_properties.end(), property_id) != schema_properties.end()) {
        pk.emplace_back(std::make_pair(property_filter.expression, property_filter));
      }
    }

    return pk.size() == schema_properties.size()
               ? pk
               : std::vector<std::pair<query::v2::Expression *, query::v2::plan::FilterInfo>>{};
  }

  msgs::ShardRequestManagerInterface *shard_request_manager_;
};

template <class TDbAccessor>
auto MakeVertexCountCache(TDbAccessor *db) {
  return VertexCountCache<TDbAccessor>(db);
}

}  // namespace memgraph::query::v2::plan
