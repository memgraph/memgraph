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

#include "query/stream/common.hpp"

#include <json/json.hpp>

namespace memgraph::query::stream {
namespace {
const std::string kBatchIntervalKey{"batch_interval"};
const std::string kBatchSizeKey{"batch_size"};
const std::string kTransformationName{"transformation_name"};
const std::string kBatchLimit{"batch_limit"};
}  // namespace

void to_json(nlohmann::json &data, CommonStreamInfo &&common_info) {
  data[kBatchIntervalKey] = common_info.batch_interval.count();
  data[kBatchSizeKey] = common_info.batch_size;
  data[kTransformationName] = common_info.transformation_name;

  if (common_info.batch_limit.has_value()) {
    data[kBatchLimit] = common_info.batch_limit.value();
  } else {
    data[kBatchLimit] = kBatchLimitNullValue;
  }
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

  common_info.batch_limit = std::nullopt;
  if (const auto batch_limit = data.at(kBatchLimit); !batch_limit.is_null()) {
    if (const auto value = batch_limit.get<decltype(kBatchLimitNullValue)>(); value != kBatchLimitNullValue) {
      common_info.batch_limit = value;
    }
  }
}
}  // namespace memgraph::query::stream
