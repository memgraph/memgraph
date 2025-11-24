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

#include "flags/run_time_configurable.hpp"

// TODO: (andi) This is a copy of the s3_config for arrow_parquet
// Need to have only a single copy of this file

export module memgraph.csv.s3_config;

using namespace std::string_view_literals;

// TODO: (andi) This will become memgraph::utils
export namespace memgraph::csv {

constexpr auto kAwsRegionQuerySetting = "aws_region"sv;
constexpr auto kAwsAccessKeyQuerySetting = "aws_access_key"sv;
constexpr auto kAwsSecretKeyQuerySetting = "aws_secret_key"sv;
constexpr auto kAwsEndpointUrlQuerySetting = "aws_endpoint_url"sv;

constexpr auto kAwsAccessKeyEnv = "AWS_ACCESS_KEY";
constexpr auto kAwsRegionEnv = "AWS_REGION";
constexpr auto kAwsSecretKeyEnv = "AWS_SECRET_KEY";
constexpr auto kAwsEndpointUrlEnv = "AWS_ENDPOINT_URL";

struct S3Config {
  std::optional<std::string> aws_region;
  std::optional<std::string> aws_access_key;
  std::optional<std::string> aws_secret_key;
  std::optional<std::string> aws_endpoint_url;

  // Query settings -> run_time flags -> env variables
  static auto Build(std::map<std::string, std::string, std::less<>> query_config) -> S3Config {
    S3Config config;

    // Helper to extract value from map
    auto extract_from_map = [&](std::string_view key) -> std::optional<std::string> {
      if (auto it = query_config.find(key); it != query_config.end()) {
        return std::move(it->second);
      }
      return std::nullopt;
    };

    // Helper to get environment variable
    auto get_env = [](const char *env_name) -> std::optional<std::string> {
      if (const auto *env_val = std::getenv(env_name)) {
        return std::string{env_val};
      }
      return std::nullopt;
    };

    // Helper to get runtime flag (returns optional to avoid empty string checks)
    auto get_flag = [](auto &&flag_getter) -> std::optional<std::string> {
      auto value = flag_getter();
      return value.empty() ? std::nullopt : std::make_optional(std::move(value));
    };

    // Priority: query_config > runtime flags > environment variables
    config.aws_region = extract_from_map(kAwsRegionQuerySetting)
                            .or_else([&] { return get_flag(memgraph::flags::run_time::GetAwsRegion); })
                            .or_else([&] { return get_env(kAwsRegionEnv); });

    config.aws_access_key = extract_from_map(kAwsAccessKeyQuerySetting)
                                .or_else([&] { return get_flag(memgraph::flags::run_time::GetAwsAccessKey); })
                                .or_else([&] { return get_env(kAwsAccessKeyEnv); });

    config.aws_secret_key = extract_from_map(kAwsSecretKeyQuerySetting)
                                .or_else([&] { return get_flag(memgraph::flags::run_time::GetAwsSecretKey); })
                                .or_else([&] { return get_env(kAwsSecretKeyEnv); });

    config.aws_endpoint_url = extract_from_map(kAwsEndpointUrlQuerySetting)
                                  .or_else([&] { return get_flag(memgraph::flags::run_time::GetAwsEndpointUrl); })
                                  .or_else([&] { return get_env(kAwsEndpointUrlEnv); });

    return config;
  }
};

}  // namespace memgraph::csv
