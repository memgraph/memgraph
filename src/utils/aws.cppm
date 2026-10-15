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

#include <expected>
#include <map>
#include <optional>
#include <ostream>
#include <streambuf>
#include <string>

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/transfer/TransferManager.h>

#include "spdlog/spdlog.h"
#include "utils/counter.hpp"

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

enum class AwsValidationError : uint8_t { AWS_REGION, AWS_ACCESS_KEY, AWS_SECRET_KEY };

auto AwsValidationErrorToStr(AwsValidationError err) -> std::string {
  switch (err) {
    using enum AwsValidationError;
    case AWS_REGION:
      return fmt::format(
          "AWS region configuration parameter not provided. Please provide it through the query, run-time setting {} "
          "or env variable {}",
          kAwsRegionQuerySetting,
          kAwsRegionEnv);
    case AWS_ACCESS_KEY:
      return fmt::format(
          "AWS access key configuration parameter not provided. Please provide it through the query, run-time "
          "setting {} or env variable {}",
          kAwsAccessKeyQuerySetting,
          kAwsAccessKeyEnv);
    case AWS_SECRET_KEY:
      return fmt::format(
          "AWS secret key configuration parameter not provided. Please provide it through the query, run-time "
          "setting {} or env variable {}",
          kAwsSecretKeyQuerySetting,
          kAwsSecretKeyEnv);
  }
}

struct S3Config {
  std::optional<std::string> aws_region;
  std::optional<std::string> aws_access_key;
  std::optional<std::string> aws_secret_key;
  std::optional<std::string> aws_endpoint_url;

  // ValidationError if there is a mistake, nullopt if all good
  [[nodiscard]] auto Validate() const -> std::optional<AwsValidationError> {
    if (!aws_region.has_value()) {
      return AwsValidationError::AWS_REGION;
    }
    if (!aws_access_key.has_value()) {
      return AwsValidationError::AWS_ACCESS_KEY;
    }

    if (!aws_secret_key.has_value()) {
      return AwsValidationError::AWS_SECRET_KEY;
    }
    return std::nullopt;
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
  // How long the transfer may stay below lowSpeedLimit before it is given up on, which the default
  // of zero disables entirely. A body that stops arriving part way through would otherwise leave the
  // thread reading it blocked with nothing to time it out, and a reader waiting on that thread has
  // no way to stop it. The limit is one byte a second, so a slow transfer that is still making
  // progress is not affected however long it takes.
  client_config.requestTimeoutMs = 30'000;
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

enum class S3ErrorCode : uint8_t { S3_PREFIX_MISSING, BUCKET_OBJECT_KEY_MISSING, S3_API_ERROR };

struct S3Error {
  S3ErrorCode status_code;
  std::string message;
};

auto ExtractBucketAndObjectKey(std::string_view uri)
    -> std::expected<std::pair<std::string_view, std::string_view>, S3Error> {
  constexpr std::string_view s3_prefix = "s3://";

  // Validate and remove prefix
  if (!uri.starts_with(s3_prefix)) {
    return std::unexpected{
        S3Error{.status_code = S3ErrorCode::S3_PREFIX_MISSING, .message = "URI must start with s3://"}};
  }
  uri.remove_prefix(s3_prefix.size());

  // Find first slash separating bucket from the object key
  auto const slash_pos = uri.find('/');
  if (slash_pos == std::string_view::npos || slash_pos == uri.size() - 1) {
    return std::unexpected{S3Error{.status_code = S3ErrorCode::BUCKET_OBJECT_KEY_MISSING,
                                   .message = "URI must contain bucket and object key"}};
  }

  return std::make_pair(uri.substr(0, slash_pos), uri.substr(slash_pos + 1));
}

constexpr auto kStreamingGetTag = "MemgraphS3StreamingGet";

// Writes the object at `uri` into `sink` as the body arrives, so neither this function nor the SDK
// ever holds the whole object. The SDK's default response stream would collect it in full first.
auto GetS3ObjectStreaming(std::string_view uri, S3Config const &s3_config, std::streambuf &sink)
    -> std::expected<void, S3Error> {
  GlobalS3APIManager::GetInstance();

  auto bucket_info = ExtractBucketAndObjectKey(uri);
  if (!bucket_info.has_value()) {
    return std::unexpected{bucket_info.error()};
  }

  auto client_config = BuildClientConfiguration(*s3_config.aws_region, s3_config.aws_endpoint_url);
  // Every attempt writes the body it receives from the start into this same sink, which has no
  // position to rewind. A retry after a partial response would therefore hand the caller that
  // partial body followed by a whole one and still report success, so a failure has to surface
  // instead of being retried under the sink.
  client_config.retryStrategy = Aws::MakeShared<Aws::Client::DefaultRetryStrategy>(kStreamingGetTag, 0);

  Aws::Auth::AWSCredentials const credentials(*s3_config.aws_access_key, *s3_config.aws_secret_key);
  Aws::S3::S3Client const s3_client(
      credentials, client_config, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, false);

  auto request = BuildGetObjectRequest(bucket_info->first, bucket_info->second);
  request.SetResponseStreamFactory([&sink]() { return Aws::New<Aws::IOStream>(kStreamingGetTag, &sink); });

  auto const outcome = s3_client.GetObject(request);
  if (!outcome.IsSuccess()) {
    // The SDK names a failure by reading back the body it just wrote, which a sink that only accepts
    // writes cannot supply. The status always survives, because it comes from the headers.
    auto const &error = outcome.GetError();
    auto const status = static_cast<int>(error.GetResponseCode());
    auto const &name = error.GetExceptionName();

    auto message = std::string{};
    if (status == 0) {
      message = fmt::format("Failed to get object from S3 {}. {}", uri, error.GetMessage());
    } else if (name.empty()) {
      message = fmt::format("Failed to get object from S3 {}. The server replied {}", uri, status);
    } else {
      message = fmt::format("Failed to get object from S3 {}. The server replied {}: {}", uri, status, name);
    }

    return std::unexpected{S3Error{.status_code = S3ErrorCode::S3_API_ERROR, .message = std::move(message)}};
  }

  return {};
}

// Writes the content of the S3 object from the uri into ostream
// returns nullopt is all good, value of the error if something wrong
// On failure the stream may already hold part of the object, so a caller must discard it rather than
// use what is there.
auto GetS3Object(std::string uri, S3Config const &s3_config, std::ostream &ostream) -> std::expected<void, S3Error> {
  auto *sink = ostream.rdbuf();
  if (sink == nullptr) {
    return std::unexpected{
        S3Error{.status_code = S3ErrorCode::S3_API_ERROR, .message = "Output stream has no buffer to write into"}};
  }

  return GetS3ObjectStreaming(uri, s3_config, *sink);
}

// Writes the content of the S3 object from the uri into a file on the local disk
auto GetS3Object(std::string uri, S3Config const &s3_config, std::string local_file_path)
    -> std::expected<void, S3Error> {
  GlobalS3APIManager::GetInstance();

  Aws::Auth::AWSCredentials const credentials(*s3_config.aws_access_key, *s3_config.aws_secret_key);
  auto const s3_client =
      std::make_shared<Aws::S3::S3Client>(credentials,
                                          BuildClientConfiguration(*s3_config.aws_region, s3_config.aws_endpoint_url),
                                          Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                                          false);

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
  auto const bucket_info = ExtractBucketAndObjectKey(uri);
  if (!bucket_info.has_value()) {
    return std::unexpected{bucket_info.error()};
  }

  auto const transfer_handle = transfer_manager->DownloadFile(
      Aws::String{bucket_info->first}, Aws::String{bucket_info->second}, Aws::String{std::move(local_file_path)});
  transfer_handle->WaitUntilFinished();

  if (transfer_handle->GetStatus() != Aws::Transfer::TransferStatus::COMPLETED) {
    return std::unexpected{
        S3Error{.status_code = S3ErrorCode::S3_API_ERROR, .message = transfer_handle->GetLastError().GetMessage()}};
  }
  spdlog::trace("Downloaded {} bytes of the file {}", transfer_handle->GetBytesTotalSize(), uri);
  return {};
}

}  // namespace memgraph::utils
