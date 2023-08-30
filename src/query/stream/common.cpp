// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "query/stream/common.hpp"
#include "dbms/global.hpp"

#include <json/json.hpp>

namespace memgraph::query::stream {
namespace {
const std::string kBatchIntervalKey{"batch_interval"};
const std::string kBatchSizeKey{"batch_size"};
const std::string kTransformationName{"transformation_name"};
const std::string kDatabaseName{"database_name"};
}  // namespace

void to_json(nlohmann::json &data, CommonStreamInfo &&common_info) {
  data[kBatchIntervalKey] = common_info.batch_interval.count();
  data[kBatchSizeKey] = common_info.batch_size;
  data[kTransformationName] = common_info.transformation_name;
  data[kDatabaseName] = common_info.database_name;
}

void from_json(const nlohmann::json &data, CommonStreamInfo &common_info) {
  if (const auto batch_interval = data.at(kBatchIntervalKey); !batch_interval.is_null()) {
    using BatchInterval = decltype(common_info.batch_interval);
    common_info.batch_interval = BatchInterval{batch_interval.get<typename BatchInterval::rep>()};
  } else {
    common_info.batch_interval = kDefaultBatchInterval;
  }

  if (const auto batch_size = data.at(kBatchSizeKey); !batch_size.is_null()) {
    common_info.batch_size = batch_size.get<decltype(common_info.batch_size)>();
  } else {
    common_info.batch_size = kDefaultBatchSize;
  }

  data.at(kTransformationName).get_to(common_info.transformation_name);

  // TODO Better handling
  try {
    data.at(kDatabaseName).get_to(common_info.database_name);
  } catch (const std::out_of_range &) {
    common_info.database_name = dbms::kDefaultDB;
  }
}
}  // namespace memgraph::query::stream
