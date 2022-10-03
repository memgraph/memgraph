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

#include "coordinator/shard_map.hpp"
#include "storage/v3/temporal.hpp"

namespace memgraph::coordinator {

using memgraph::common::SchemaType;
using memgraph::storage::v3::TemporalData;
using memgraph::storage::v3::TemporalType;

PrimaryKey SchemaToMinKey(const std::vector<SchemaProperty> &schema) {
  PrimaryKey ret{};

  const int64_t min_int = std::numeric_limits<int64_t>::min();

  const TemporalData date{TemporalType::Date, min_int};
  const TemporalData local_time{TemporalType::LocalTime, min_int};
  const TemporalData local_date_time{TemporalType::LocalDateTime, min_int};
  const TemporalData duration{TemporalType::Duration, min_int};

  for (const auto &schema_property : schema) {
    switch (schema_property.type) {
      case SchemaType::BOOL:
        ret.emplace_back(PropertyValue(false));
        break;
      case SchemaType::INT:
        ret.emplace_back(PropertyValue(min_int));
        break;
      case SchemaType::STRING:
        ret.emplace_back(PropertyValue(""));
        break;
      case SchemaType::DATE:
        ret.emplace_back(PropertyValue(date));
        break;
      case SchemaType::LOCALTIME:
        ret.emplace_back(PropertyValue(local_time));
        break;
      case SchemaType::LOCALDATETIME:
        ret.emplace_back(PropertyValue(local_date_time));
        break;
      case SchemaType::DURATION:
        ret.emplace_back(PropertyValue(duration));
        break;
    }
  }

  return ret;
}

std::optional<LabelId> ShardMap::InitializeNewLabel(std::string label_name, std::vector<SchemaProperty> schema,
                                                    size_t replication_factor, Hlc last_shard_map_version) {
  if (shard_map_version != last_shard_map_version || labels.contains(label_name)) {
    return std::nullopt;
  }

  const LabelId label_id = LabelId::FromUint(++max_label_id);

  labels.emplace(std::move(label_name), label_id);

  PrimaryKey initial_key = SchemaToMinKey(schema);
  Shard empty_shard = {};

  Shards shards = {
      {initial_key, empty_shard},
  };

  LabelSpace label_space{
      .schema = std::move(schema),
      .shards = shards,
      .replication_factor = replication_factor,
  };

  label_spaces.emplace(label_id, label_space);

  IncrementShardMapVersion();

  return label_id;
}

}  // namespace memgraph::coordinator
