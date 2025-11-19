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

module;

#include <map>
#include <optional>
#include <string>

export module memgraph.query.arrow_parquet.parquet_file_config;

using namespace std::string_view_literals;

export namespace memgraph::query {

constexpr auto kAwsRegionQuerySetting = "aws_region"sv;
constexpr auto kAwsAccessKeyQuerySetting = "aws_access_key"sv;
constexpr auto kAwsSecretKeyQuerySetting = "aws_secret_key"sv;
constexpr auto kAwsEndpointUrlQuerySetting = "aws_endpoint_url"sv;

struct ParquetFileConfig {
  std::string file;  // URI path for S3 or path on local disk
  std::optional<std::string> aws_region;
  std::optional<std::string> aws_access_key;
  std::optional<std::string> aws_secret_key;
  std::optional<std::string> aws_endpoint_url;

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

    if (auto const endpoint_it = query_config.find(kAwsEndpointUrlQuerySetting); endpoint_it != query_config.end()) {
      config.aws_endpoint_url = std::move(endpoint_it->second);
    }

    return config;
  };
};

}  // namespace memgraph::query
