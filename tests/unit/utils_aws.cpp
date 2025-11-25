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

#include <string>

import memgraph.utils.aws;

using memgraph::utils::ExtractBucketAndObjectKey;

TEST(ExtractBucketAndObjectKey, Regular) {
  auto const test = "s3://deps.memgraph.io/pokec/dataset/nodes.csv";
  auto const [bucket_name, object_key] = ExtractBucketAndObjectKey(test);
  ASSERT_EQ(bucket_name, "deps.memgraph.io");
  ASSERT_EQ(object_key, "pokec/dataset/nodes.csv");
}

TEST(ExtractBucketAndObjectKey, SimpleFile) {
  auto const test = "s3://my-bucket/file.csv";
  auto const [bucket_name, object_key] = ExtractBucketAndObjectKey(test);
  ASSERT_EQ(bucket_name, "my-bucket");
  ASSERT_EQ(object_key, "file.csv");
}

TEST(ExtractBucketAndObjectKey, NestedPath) {
  auto const test = "s3://bucket/a/b/c/d/file.txt";
  auto const [bucket_name, object_key] = ExtractBucketAndObjectKey(test);
  ASSERT_EQ(bucket_name, "bucket");
  ASSERT_EQ(object_key, "a/b/c/d/file.txt");
}

TEST(ExtractBucketAndObjectKey, BucketWithDashes) {
  auto const test = "s3://my-test-bucket-123/data.json";
  auto const [bucket_name, object_key] = ExtractBucketAndObjectKey(test);
  ASSERT_EQ(bucket_name, "my-test-bucket-123");
  ASSERT_EQ(object_key, "data.json");
}

TEST(ExtractBucketAndObjectKey, BucketWithDots) {
  auto const test = "s3://my.bucket.name/file.txt";
  auto const [bucket_name, object_key] = ExtractBucketAndObjectKey(test);
  ASSERT_EQ(bucket_name, "my.bucket.name");
  ASSERT_EQ(object_key, "file.txt");
}

TEST(ExtractBucketAndObjectKey, ObjectKeyWithSpecialCharacters) {
  auto const test = "s3://bucket/path/file-name_123.csv";
  auto const [bucket_name, object_key] = ExtractBucketAndObjectKey(test);
  ASSERT_EQ(bucket_name, "bucket");
  ASSERT_EQ(object_key, "path/file-name_123.csv");
}

TEST(ExtractBucketAndObjectKey, ObjectKeyWithSpaces) {
  auto const test = "s3://bucket/path/file%20with%20spaces.txt";
  auto const [bucket_name, object_key] = ExtractBucketAndObjectKey(test);
  ASSERT_EQ(bucket_name, "bucket");
  ASSERT_EQ(object_key, "path/file%20with%20spaces.txt");
}

TEST(ExtractBucketAndObjectKey, LongPath) {
  auto const test = "s3://my-bucket/very/long/nested/path/to/some/file/in/deep/directory/data.csv";
  auto const [bucket_name, object_key] = ExtractBucketAndObjectKey(test);
  ASSERT_EQ(bucket_name, "my-bucket");
  ASSERT_EQ(object_key, "very/long/nested/path/to/some/file/in/deep/directory/data.csv");
}

TEST(ExtractBucketAndObjectKey, ObjectKeyStartingWithSlash) {
  auto const test = "s3://bucket//file.txt";
  auto const [bucket_name, object_key] = ExtractBucketAndObjectKey(test);
  ASSERT_EQ(bucket_name, "bucket");
  ASSERT_EQ(object_key, "/file.txt");
}

TEST(ExtractBucketAndObjectKey, MinimalBucketName) {
  auto const test = "s3://b/key";
  auto const [bucket_name, object_key] = ExtractBucketAndObjectKey(test);
  ASSERT_EQ(bucket_name, "b");
  ASSERT_EQ(object_key, "key");
}

TEST(ExtractBucketAndObjectKey, ObjectWithMultipleExtensions) {
  auto const test = "s3://bucket/file.tar.gz";
  auto const [bucket_name, object_key] = ExtractBucketAndObjectKey(test);
  ASSERT_EQ(bucket_name, "bucket");
  ASSERT_EQ(object_key, "file.tar.gz");
}

TEST(ExtractBucketAndObjectKey, NumericBucketName) {
  auto const test = "s3://123456789/data.csv";
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
