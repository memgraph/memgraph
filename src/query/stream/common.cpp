// Copyright 2021 Memgraph Ltd.
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

namespace query {
namespace {
const std::string kBatchIntervalKey{"batch_interval"};
const std::string kBatchSizeKey{"batch_size"};
const std::string kTransformationName{"transformation_name"};
}  // namespace

void to_json(nlohmann::json &data, CommonStreamInfo &&common_info) {
  if (common_info.batch_interval) {
    data[kBatchIntervalKey] = common_info.batch_interval->count();
  } else {
    data[kBatchIntervalKey] = nullptr;
  }

  if (common_info.batch_size) {
    data[kBatchSizeKey] = *common_info.batch_size;
  } else {
    data[kBatchSizeKey] = nullptr;
  }

  data[kTransformationName] = common_info.transformation_name;
}

void from_json(const nlohmann::json &data, CommonStreamInfo &common_info) {
  if (const auto batch_interval = data.at(kBatchIntervalKey); !batch_interval.is_null()) {
    using BatchInterval = typename decltype(common_info.batch_interval)::value_type;
    common_info.batch_interval = BatchInterval{batch_interval.get<typename BatchInterval::rep>()};
  } else {
    common_info.batch_interval = {};
  }

  if (const auto batch_size = data.at(kBatchSizeKey); !batch_size.is_null()) {
    common_info.batch_size = batch_size.get<typename decltype(common_info.batch_size)::value_type>();
  } else {
    common_info.batch_size = {};
  }

  data.at(kTransformationName).get_to(common_info.transformation_name);
}
}  // namespace query
