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

#include "gtest/gtest.h"

#include "arrow/filesystem/s3fs.h"
#include "arrow/io/api.h"
#include "spdlog/spdlog.h"

#include <string_view>

#include "arrow/filesystem/s3fs.h"
#include "arrow/table.h"
#include "parquet/arrow/reader.h"

TEST(ArrowS3, Simple) {
  arrow::fs::S3GlobalOptions global_options;
  global_options.log_level = arrow::fs::S3LogLevel::Fatal;
  auto init_status = arrow::fs::InitializeS3(global_options);
  ASSERT_TRUE(init_status.ok());

  auto s3_options = arrow::fs::S3Options::FromAccessKey("1", "2");
  s3_options.region = "eu-west-1";

  auto maybe_s3fs = arrow::fs::S3FileSystem::Make(s3_options);
  ASSERT_TRUE(maybe_s3fs.ok());
  auto s3fs = *maybe_s3fs;
  constexpr std::string_view pokec_path = "deps.memgraph.io/dataset/pokec/nodes_1m.parquet";
  auto input = s3fs->OpenInputFile(std::string(pokec_path));
  if (!input.ok()) {
    spdlog::error("Error: {}", input.status().message());
  }
  ASSERT_TRUE(input.ok());

  auto maybe_parquet_reader = parquet::arrow::OpenFile(*input, arrow::default_memory_pool());
  ASSERT_TRUE(maybe_parquet_reader.ok());

  auto &parquet_reader = *maybe_parquet_reader;

  std::shared_ptr<arrow::Table> table;
  auto res = parquet_reader->ReadTable(&table);
  ASSERT_TRUE(res.ok());

  ASSERT_EQ(table->num_rows(), 1000000);

  // TODO: (andi) This should always be done even if the test fails
  auto finalize_status = arrow::fs::FinalizeS3();
  ASSERT_TRUE(finalize_status.ok());
}
