// Copyright 2025 Memgraph Ltd.
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

#include <map>
#include <optional>
#include <string>

namespace memgraph::query {

using namespace std::string_view_literals;
constexpr auto kAwsRegionQuerySetting = "aws_region"sv;
constexpr auto kAwsAccessKeyQuerySetting = "aws_access_key"sv;
constexpr auto kAwsSecretKeyQuerySetting = "aws_secret_key"sv;

struct ParquetFileConfig {
  std::string file;  // URI path for S3 or path on local disk
  std::optional<std::string> aws_region;
  std::optional<std::string> aws_access_key;
  std::optional<std::string> aws_secret_key;

  static auto FromQueryConfig(std::string file, std::map<std::string, std::string, std::less<>> query_config)
      -> ParquetFileConfig {
    ParquetFileConfig config;
    config.file = std::move(file);

    if (auto const region_it = query_config.find(kAwsRegionQuerySetting); region_it != query_config.end()) {
      config.aws_region = std::move(region_it->second);
    }

    if (auto const access_key_it = query_config.find(kAwsAccessKeyQuerySetting); access_key_it != query_config.end()) {
      config.aws_access_key = std::move(access_key_it->second);
    }

    if (auto const secret_key_it = query_config.find(kAwsSecretKeyQuerySetting); secret_key_it != query_config.end()) {
      config.aws_secret_key = std::move(secret_key_it->second);
    }

    return config;
  };
};

}  // namespace memgraph::query
