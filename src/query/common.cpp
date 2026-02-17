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

#include "query/common.hpp"
#include "flags/run_time_configurable.hpp"

import memgraph.utils.aws;

namespace memgraph::query {

int64_t QueryTimestamp() {
  return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
      .count();
}

auto BuildRunTimeS3Config() -> std::map<std::string, std::string, std::less<>> {
  std::map<std::string, std::string, std::less<>> config;
  if (auto aws_region = memgraph::flags::run_time::GetAwsRegion(); !aws_region.empty()) {
    config.emplace(utils::kAwsRegionQuerySetting, std::move(aws_region));
  }

  if (auto aws_acc_key = memgraph::flags::run_time::GetAwsAccessKey(); !aws_acc_key.empty()) {
    config.emplace(utils::kAwsAccessKeyQuerySetting, std::move(aws_acc_key));
  }

  if (auto aws_sec_key = memgraph::flags::run_time::GetAwsSecretKey(); !aws_sec_key.empty()) {
    config.emplace(utils::kAwsSecretKeyQuerySetting, std::move(aws_sec_key));
  }
  if (auto aws_endpoint_url = memgraph::flags::run_time::GetAwsEndpointUrl(); !aws_endpoint_url.empty()) {
    config.emplace(utils::kAwsEndpointUrlQuerySetting, std::move(aws_endpoint_url));
  }
  return config;
}

}  // namespace memgraph::query
