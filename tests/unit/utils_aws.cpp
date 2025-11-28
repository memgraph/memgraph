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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "utils/exceptions.hpp"

import memgraph.utils.aws;

using memgraph::utils::BasicException;
using memgraph::utils::ExtractBucketAndObjectKey;
using memgraph::utils::kAwsAccessKeyEnv;
using memgraph::utils::kAwsAccessKeyQuerySetting;
using memgraph::utils::kAwsRegionEnv;
using memgraph::utils::kAwsRegionQuerySetting;
using memgraph::utils::kAwsSecretKeyEnv;
using memgraph::utils::kAwsSecretKeyQuerySetting;
using memgraph::utils::S3Config;

TEST(S3Config, BuildFromQueryConfig) {
  std::map<std::string, std::string, std::less<>> query_config;
  query_config.emplace(kAwsRegionQuerySetting, "eu-west-1");
  query_config.emplace(kAwsAccessKeyQuerySetting, "acc_key");
  query_config.emplace(kAwsSecretKeyQuerySetting, "secret_key");

  auto s3_config = S3Config::Build(std::move(query_config), {});
  ASSERT_EQ(s3_config.aws_region, "eu-west-1");
  ASSERT_EQ(s3_config.aws_access_key, "acc_key");
  ASSERT_EQ(s3_config.aws_secret_key, "secret_key");
}

TEST(S3Config, BuildFromRuntimeConfig) {
  std::map<std::string, std::string, std::less<>> run_time_config;
  run_time_config.emplace(kAwsRegionQuerySetting, "eu-west-1");
  run_time_config.emplace(kAwsAccessKeyQuerySetting, "acc_key");
  run_time_config.emplace(kAwsSecretKeyQuerySetting, "secret_key");

  auto s3_config = S3Config::Build({}, std::move(run_time_config));
  ASSERT_EQ(s3_config.aws_region, "eu-west-1");
  ASSERT_EQ(s3_config.aws_access_key, "acc_key");
  ASSERT_EQ(s3_config.aws_secret_key, "secret_key");
}

TEST(S3Config, BuildFromEnv) {
  // NOLINTNEXTLINE
  setenv(kAwsRegionEnv, "eu-west-2", 1);
  // NOLINTNEXTLINE
  setenv(kAwsAccessKeyEnv, "acc_key_env", 1);
  // NOLINTNEXTLINE
  setenv(kAwsSecretKeyEnv, "secret_key_env", 1);
  memgraph::utils::OnScopeExit const on_exit{[]() {
    // NOLINTNEXTLINE
    unsetenv(kAwsRegionEnv);
    // NOLINTNEXTLINE
    unsetenv(kAwsAccessKeyEnv);
    // NOLINTNEXTLINE
    unsetenv(kAwsSecretKeyEnv);
  }};
  auto s3_config = S3Config::Build({}, {});
  ASSERT_EQ(s3_config.aws_region, "eu-west-2");
  ASSERT_EQ(s3_config.aws_access_key, "acc_key_env");
  ASSERT_EQ(s3_config.aws_secret_key, "secret_key_env");
}

TEST(S3Config, PreferQueryOverRuntime) {
  std::map<std::string, std::string, std::less<>> query_config;
  query_config.emplace(kAwsRegionQuerySetting, "eu-west-1");
  query_config.emplace(kAwsAccessKeyQuerySetting, "acc_key");
  query_config.emplace(kAwsSecretKeyQuerySetting, "secret_key");

  std::map<std::string, std::string, std::less<>> run_time_config;
  run_time_config.emplace(kAwsRegionQuerySetting, "eu-east-1");
  run_time_config.emplace(kAwsAccessKeyQuerySetting, "acc_key_runtime");
  run_time_config.emplace(kAwsSecretKeyQuerySetting, "secret_key_runtime");

  auto s3_config = S3Config::Build(std::move(query_config), std::move(run_time_config));
  ASSERT_EQ(s3_config.aws_region, "eu-west-1");
  ASSERT_EQ(s3_config.aws_access_key, "acc_key");
  ASSERT_EQ(s3_config.aws_secret_key, "secret_key");
}

TEST(S3Config, PreferRuntimeOverEnv) {
  // NOLINTNEXTLINE
  setenv(kAwsRegionEnv, "eu-west-2", 1);
  // NOLINTNEXTLINE
  setenv(kAwsAccessKeyEnv, "acc_key_env", 1);
  // NOLINTNEXTLINE
  setenv(kAwsSecretKeyEnv, "secret_key_env", 1);
  memgraph::utils::OnScopeExit const on_exit{[]() {
    // NOLINTNEXTLINE
    unsetenv(kAwsRegionEnv);
    // NOLINTNEXTLINE
    unsetenv(kAwsAccessKeyEnv);
    // NOLINTNEXTLINE
    unsetenv(kAwsSecretKeyEnv);
  }};

  std::map<std::string, std::string, std::less<>> run_time_config;
  run_time_config.emplace(kAwsRegionQuerySetting, "eu-east-1");
  run_time_config.emplace(kAwsAccessKeyQuerySetting, "acc_key_runtime");
  run_time_config.emplace(kAwsSecretKeyQuerySetting, "secret_key_runtime");

  auto s3_config = S3Config::Build({}, std::move(run_time_config));
  ASSERT_EQ(s3_config.aws_region, "eu-east-1");
  ASSERT_EQ(s3_config.aws_access_key, "acc_key_runtime");
  ASSERT_EQ(s3_config.aws_secret_key, "secret_key_runtime");
}

TEST(S3Config, PreferQueryOverEnv) {
  // NOLINTNEXTLINE
  setenv(kAwsRegionEnv, "eu-west-2", 1);
  // NOLINTNEXTLINE
  setenv(kAwsAccessKeyEnv, "acc_key_env", 1);
  // NOLINTNEXTLINE
  setenv(kAwsSecretKeyEnv, "secret_key_env", 1);

  memgraph::utils::OnScopeExit const on_exit{[]() {
    // NOLINTNEXTLINE
    unsetenv(kAwsRegionEnv);
    // NOLINTNEXTLINE
    unsetenv(kAwsAccessKeyEnv);
    // NOLINTNEXTLINE
    unsetenv(kAwsSecretKeyEnv);
  }};

  std::map<std::string, std::string, std::less<>> query_config;
  query_config.emplace(kAwsRegionQuerySetting, "eu-east-1");
  query_config.emplace(kAwsAccessKeyQuerySetting, "acc_key_query");
  query_config.emplace(kAwsSecretKeyQuerySetting, "secret_key_query");

  auto s3_config = S3Config::Build({}, std::move(query_config));
  ASSERT_EQ(s3_config.aws_region, "eu-east-1");
  ASSERT_EQ(s3_config.aws_access_key, "acc_key_query");
  ASSERT_EQ(s3_config.aws_secret_key, "secret_key_query");
}

TEST(S3Config, ValidateOK) {
  S3Config s3_config{.aws_region = "eu-west-1", .aws_access_key = "test", .aws_secret_key = "123"};
  ASSERT_NO_THROW(s3_config.Validate());
}

TEST(S3Config, MissingSecretKey) {
  S3Config s3_config{.aws_region = "eu-west-1", .aws_access_key = "test", .aws_endpoint_url = "localhost:4566"};
  ASSERT_THROW(s3_config.Validate(), BasicException);
}

TEST(S3Config, MissingAccessKey) {
  S3Config s3_config{.aws_region = "eu-west-1", .aws_secret_key = "test", .aws_endpoint_url = "localhost:4566"};
  ASSERT_THROW(s3_config.Validate(), BasicException);
}

TEST(S3Config, MissingRegion) {
  S3Config s3_config{.aws_access_key = "test_123", .aws_secret_key = "test", .aws_endpoint_url = "localhost:4566"};
  ASSERT_THROW(s3_config.Validate(), BasicException);
}

TEST(ExtractBucketAndObjectKey, Regular) {
  constexpr auto test = "s3://deps.memgraph.io/pokec/dataset/nodes.csv";
  auto const [bucket_name, object_key] = ExtractBucketAndObjectKey(test);
  ASSERT_EQ(bucket_name, "deps.memgraph.io");
  ASSERT_EQ(object_key, "pokec/dataset/nodes.csv");
}

TEST(ExtractBucketAndObjectKey, SimpleFile) {
  constexpr auto test = "s3://my-bucket/file.csv";
  auto const [bucket_name, object_key] = ExtractBucketAndObjectKey(test);
  ASSERT_EQ(bucket_name, "my-bucket");
  ASSERT_EQ(object_key, "file.csv");
}

TEST(ExtractBucketAndObjectKey, NestedPath) {
  constexpr auto test = "s3://bucket/a/b/c/d/file.txt";
  auto const [bucket_name, object_key] = ExtractBucketAndObjectKey(test);
  ASSERT_EQ(bucket_name, "bucket");
  ASSERT_EQ(object_key, "a/b/c/d/file.txt");
}

TEST(ExtractBucketAndObjectKey, BucketWithDashes) {
  constexpr auto test = "s3://my-test-bucket-123/data.json";
  auto const [bucket_name, object_key] = ExtractBucketAndObjectKey(test);
  ASSERT_EQ(bucket_name, "my-test-bucket-123");
  ASSERT_EQ(object_key, "data.json");
}

TEST(ExtractBucketAndObjectKey, BucketWithDots) {
  constexpr auto test = "s3://my.bucket.name/file.txt";
  auto const [bucket_name, object_key] = ExtractBucketAndObjectKey(test);
  ASSERT_EQ(bucket_name, "my.bucket.name");
  ASSERT_EQ(object_key, "file.txt");
}

TEST(ExtractBucketAndObjectKey, ObjectKeyWithSpecialCharacters) {
  constexpr auto test = "s3://bucket/path/file-name_123.csv";
  auto const [bucket_name, object_key] = ExtractBucketAndObjectKey(test);
  ASSERT_EQ(bucket_name, "bucket");
  ASSERT_EQ(object_key, "path/file-name_123.csv");
}

TEST(ExtractBucketAndObjectKey, ObjectKeyWithSpaces) {
  constexpr auto test = "s3://bucket/path/file%20with%20spaces.txt";
  auto const [bucket_name, object_key] = ExtractBucketAndObjectKey(test);
  ASSERT_EQ(bucket_name, "bucket");
  ASSERT_EQ(object_key, "path/file%20with%20spaces.txt");
}

TEST(ExtractBucketAndObjectKey, LongPath) {
  constexpr auto test = "s3://my-bucket/very/long/nested/path/to/some/file/in/deep/directory/data.csv";
  auto const [bucket_name, object_key] = ExtractBucketAndObjectKey(test);
  ASSERT_EQ(bucket_name, "my-bucket");
  ASSERT_EQ(object_key, "very/long/nested/path/to/some/file/in/deep/directory/data.csv");
}

TEST(ExtractBucketAndObjectKey, ObjectKeyStartingWithSlash) {
  constexpr auto test = "s3://bucket//file.txt";
  auto const [bucket_name, object_key] = ExtractBucketAndObjectKey(test);
  ASSERT_EQ(bucket_name, "bucket");
  ASSERT_EQ(object_key, "/file.txt");
}

TEST(ExtractBucketAndObjectKey, MinimalBucketName) {
  constexpr auto test = "s3://b/key";
  auto const [bucket_name, object_key] = ExtractBucketAndObjectKey(test);
  ASSERT_EQ(bucket_name, "b");
  ASSERT_EQ(object_key, "key");
}

TEST(ExtractBucketAndObjectKey, ObjectWithMultipleExtensions) {
  constexpr auto test = "s3://bucket/file.tar.gz";
  auto const [bucket_name, object_key] = ExtractBucketAndObjectKey(test);
  ASSERT_EQ(bucket_name, "bucket");
  ASSERT_EQ(object_key, "file.tar.gz");
}

TEST(ExtractBucketAndObjectKey, NumericBucketName) {
  constexpr auto test = "s3://123456789/data.csv";
  auto const [bucket_name, object_key] = ExtractBucketAndObjectKey(test);
  ASSERT_EQ(bucket_name, "123456789");
  ASSERT_EQ(object_key, "data.csv");
}

TEST(ExtractBucketAndObjectKey, MissingS3Prefix) {
  EXPECT_THROW({ ExtractBucketAndObjectKey("http://bucket/key"); }, std::invalid_argument);
}

TEST(ExtractBucketAndObjectKey, MissingObjectKey) {
  EXPECT_THROW({ ExtractBucketAndObjectKey("s3://bucket"); }, std::invalid_argument);
}

TEST(ExtractBucketAndObjectKey, MissingObjectKeyWithTrailingSlash) {
  EXPECT_THROW({ ExtractBucketAndObjectKey("s3://bucket/"); }, std::invalid_argument);
}

TEST(ExtractBucketAndObjectKey, EmptyString) {
  EXPECT_THROW({ ExtractBucketAndObjectKey(""); }, std::invalid_argument);
}

TEST(ExtractBucketAndObjectKey, OnlyS3Prefix) {
  EXPECT_THROW({ ExtractBucketAndObjectKey("s3://"); }, std::invalid_argument);
}

TEST(ExtractBucketAndObjectKey, InvalidProtocol) {
  EXPECT_THROW({ ExtractBucketAndObjectKey("s4://bucket/key"); }, std::invalid_argument);
}
