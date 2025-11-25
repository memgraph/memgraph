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

#include <aws/core/Aws.h>
#include <aws/s3/model/GetObjectRequest.h>

export module memgraph.utils.aws;

using namespace std::string_view_literals;

export namespace memgraph::utils {

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
  static auto Build(std::map<std::string, std::string, std::less<>> query_config,
                    std::map<std::string, std::string, std::less<>> run_time_config) -> S3Config {
    S3Config config;

    // C++26 get
    auto extract_from_map = [](std::map<std::string, std::string, std::less<>> const &config,
                               std::string_view key) -> std::optional<std::string> {
      if (auto it = config.find(key); it != config.end()) {
        return it->second;
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

    // Priority: query_config > runtime flags > environment variables
    config.aws_region = extract_from_map(query_config, kAwsRegionQuerySetting)
                            .or_else([&] { return extract_from_map(run_time_config, kAwsRegionQuerySetting); })
                            .or_else([&] { return get_env(kAwsRegionEnv); });

    config.aws_access_key = extract_from_map(query_config, kAwsAccessKeyQuerySetting)
                                .or_else([&] { return extract_from_map(run_time_config, kAwsAccessKeyQuerySetting); })
                                .or_else([&] { return get_env(kAwsAccessKeyEnv); });

    config.aws_secret_key = extract_from_map(query_config, kAwsSecretKeyQuerySetting)
                                .or_else([&] { return extract_from_map(run_time_config, kAwsSecretKeyQuerySetting); })
                                .or_else([&] { return get_env(kAwsSecretKeyEnv); });

    config.aws_endpoint_url =
        extract_from_map(query_config, kAwsEndpointUrlQuerySetting)
            .or_else([&] { return extract_from_map(run_time_config, kAwsEndpointUrlQuerySetting); })
            .or_else([&] { return get_env(kAwsEndpointUrlEnv); });

    return config;
  }
};

// Singleton for AWS API initialization
class GlobalS3APIManager {
 public:
  GlobalS3APIManager(GlobalS3APIManager const &) = delete;
  GlobalS3APIManager &operator=(GlobalS3APIManager const &) = delete;
  GlobalS3APIManager(GlobalS3APIManager &&) = delete;
  GlobalS3APIManager &operator=(GlobalS3APIManager &&) = delete;

  static auto GetInstance() -> GlobalS3APIManager & {
    static GlobalS3APIManager instance;
    return instance;
  }

 private:
  GlobalS3APIManager() { Aws::InitAPI(options); }
  ~GlobalS3APIManager() { Aws::ShutdownAPI(options); }

  Aws::SDKOptions options;
};

auto BuildGetObjectRequest(std::string_view bucket_name, std::string_view object_key)
    -> Aws::S3::Model::GetObjectRequest {
  Aws::S3::Model::GetObjectRequest request;
  request.SetBucket(std::string(bucket_name));
  request.SetKey(std::string(object_key));
  return request;
}

auto ExtractBucketAndObjectKey(std::string_view uri) -> std::pair<std::string_view, std::string_view> {
  constexpr std::string_view s3_prefix = "s3://";

  // Validate and remove prefix
  if (!uri.starts_with(s3_prefix)) {
    throw std::invalid_argument("URI must start with s3://");
  }
  uri.remove_prefix(s3_prefix.size());

  // Find first slash separating bucket from the object key
  auto const slash_pos = uri.find('/');
  if (slash_pos == std::string_view::npos || slash_pos == uri.size() - 1) {
    throw std::invalid_argument("URI must contain bucket and object key");
  }

  return {uri.substr(0, slash_pos), uri.substr(slash_pos + 1)};
}

}  // namespace memgraph::utils
