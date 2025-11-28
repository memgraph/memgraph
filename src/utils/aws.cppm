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
#include <ostream>
#include <string>

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/transfer/TransferManager.h>

#include "spdlog/spdlog.h"
#include "utils/counter.hpp"
#include "utils/exceptions.hpp"

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

  void Validate() const {
    if (!aws_region.has_value()) {
      throw BasicException(
          "AWS region configuration parameter not provided. Please provide it through the query, run-time setting {} "
          "or "
          "env variable {}",
          kAwsRegionQuerySetting, kAwsRegionEnv);
    }
    if (!aws_access_key.has_value()) {
      throw BasicException(
          "AWS access key configuration parameter not provided. Please provide it through the query, run-time setting "
          "{} "
          "or env variable {}",
          kAwsAccessKeyQuerySetting, kAwsAccessKeyEnv);
    }

    if (!aws_secret_key.has_value()) {
      throw BasicException(
          "AWS secret key configuration parameter not provided. Please provide it through the query, run-time setting "
          "{} "
          "or env variable {}",
          kAwsSecretKeyQuerySetting, kAwsSecretKeyEnv);
    }
  }

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

  // Initialize AWS's API if not already initialized
  static auto GetInstance() -> GlobalS3APIManager & {
    static GlobalS3APIManager instance;
    return instance;
  }

 private:
  GlobalS3APIManager() { Aws::InitAPI(options); }
  ~GlobalS3APIManager() { Aws::ShutdownAPI(options); }

  Aws::SDKOptions options;
};

auto BuildClientConfiguration(std::string const &aws_region, std::optional<std::string> const &aws_endpoint_url)
    -> Aws::Client::ClientConfiguration {
  Aws::Client::ClientConfiguration client_config;
  client_config.region = aws_region;
  if (aws_endpoint_url.has_value()) {
    client_config.endpointOverride = *aws_endpoint_url;
  }
  return client_config;
}

// Builds a GetObjectRequest for AWS S3 library from the bucket_name and object_key
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

auto GetS3ObjectOutcome(std::string uri, S3Config const &s3_config) -> Aws::S3::Model::GetObjectOutcome {
  Aws::Auth::AWSCredentials const credentials(*s3_config.aws_access_key, *s3_config.aws_secret_key);
  // Use path-style for S3-compatible services (4th param = false)
  Aws::S3::S3Client const s3_client(credentials,
                                    BuildClientConfiguration(*s3_config.aws_region, s3_config.aws_endpoint_url),
                                    Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, false);

  auto outcome = s3_client.GetObject(std::apply(BuildGetObjectRequest, ExtractBucketAndObjectKey(uri)));

  if (!outcome.IsSuccess()) {
    throw BasicException("Failed to get object from S3 {}. Error: {}", uri, outcome.GetError().GetMessage());
  }

  spdlog::trace("File {} successfully downloaded. ", uri);

  return outcome;
}

// Writes the content of the S3 object from the uri into ostream
void GetS3Object(std::string uri, S3Config const &s3_config, std::ostream &ostream) {
  s3_config.Validate();
  GlobalS3APIManager::GetInstance();
  ostream << GetS3ObjectOutcome(std::move(uri), s3_config).GetResult().GetBody().rdbuf();
}

// Writes the content of the S3 object from the uri into a file on the local disk
void GetS3Object(std::string uri, S3Config const &s3_config, std::string local_file_path) {
  s3_config.Validate();
  GlobalS3APIManager::GetInstance();

  Aws::Auth::AWSCredentials const credentials(*s3_config.aws_access_key, *s3_config.aws_secret_key);
  auto const s3_client = std::make_shared<Aws::S3::S3Client>(
      credentials, BuildClientConfiguration(*s3_config.aws_region, s3_config.aws_endpoint_url),
      Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, false);

  Aws::Utils::Threading::DefaultExecutor executor;
  Aws::Transfer::TransferManagerConfiguration config(&executor);
  config.downloadProgressCallback = [](const Aws::Transfer::TransferManager * /*mgr*/,
                                       const std::shared_ptr<const Aws::Transfer::TransferHandle> &handle) {
    static thread_local auto counter = ResettableCounter(500);
    if (counter()) {
      auto progress =
          static_cast<double>(handle->GetBytesTransferred()) * 100.0 / static_cast<double>(handle->GetBytesTotalSize());
      spdlog::trace("Downloaded {:.2f}% of the file", progress);
    }
  };

  config.s3Client = s3_client;

  auto const transfer_manager = Aws::Transfer::TransferManager::Create(config);
  auto const [bucket_name, object_key] = ExtractBucketAndObjectKey(uri);
  auto const transfer_handle =
      transfer_manager->DownloadFile(Aws::String{bucket_name}, Aws::String{object_key}, Aws::String{local_file_path});
  transfer_handle->WaitUntilFinished();
  if (transfer_handle->GetStatus() == Aws::Transfer::TransferStatus::COMPLETED) {
    spdlog::trace("Downloaded {} bytes of the file {}", transfer_handle->GetBytesTotalSize(), uri);
  } else {
    throw utils::BasicException("Error occurred while downloading file {}. Error: {}", uri,
                                transfer_handle->GetLastError().GetMessage());
  }
}

}  // namespace memgraph::utils
