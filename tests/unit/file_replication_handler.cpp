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

#include <gtest/gtest.h>

#include "rpc/file_replication_handler.hpp"
#include "slk/streams.hpp"
#include "storage/v2/durability/marker.hpp"
#include "storage/v2/replication/serialization.hpp"

#include "slk_common.hpp"

using memgraph::rpc::FileReplicationHandler;
using memgraph::slk::Builder;
using memgraph::slk::Reader;
using memgraph::slk::SegmentSize;
using memgraph::storage::durability::Marker;
using memgraph::storage::replication::Encoder;

class FileReplicationHandlerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!std::filesystem::exists(test_folder_)) {
      std::filesystem::create_directories(test_folder_);
    }
  }

  void TearDown() override {
    if (!std::filesystem::exists(test_folder_)) return;
    std::filesystem::remove_all(test_folder_);
  }

  // Because FileReplicationHandler saves files into /tmp/memgraph
  std::filesystem::path test_folder_{std::filesystem::temp_directory_path() / "memgraph"};
};

TEST_F(FileReplicationHandlerTest, BasicCreationDestruction) { FileReplicationHandler file_replication_handler; }

TEST_F(FileReplicationHandlerTest, EmptyGetRemainingBytes) {
  FileReplicationHandler file_replication_handler;
  ASSERT_EQ(file_replication_handler.GetRemainingBytesToWrite(), 0);
}

TEST_F(FileReplicationHandlerTest, EmptyResetFile) {
  FileReplicationHandler file_replication_handler;
  ASSERT_NO_THROW(file_replication_handler.ResetCurrentFile());
}

TEST_F(FileReplicationHandlerTest, EmptyHasOpenedFile) {
  FileReplicationHandler file_replication_handler;
  ASSERT_FALSE(file_replication_handler.HasOpenedFile());
}

TEST_F(FileReplicationHandlerTest, EmptyActiveFiles) {
  FileReplicationHandler file_replication_handler;
  ASSERT_EQ(file_replication_handler.GetActiveFileNames(), std::vector<std::filesystem::path>());
}

TEST_F(FileReplicationHandlerTest, WritingToClosedFile) {
  FileReplicationHandler file_replication_handler;
  ASSERT_NO_THROW(file_replication_handler.WriteToFile(nullptr, 0));
}

TEST_F(FileReplicationHandlerTest, OpenEmptyFileName) {
  FileReplicationHandler file_replication_handler;
  std::vector<uint8_t> buffer;
  Builder builder([&buffer](const uint8_t *data, size_t size, bool have_more) {
    for (size_t i = 0; i < size; ++i) buffer.push_back(data[i]);
  });

  Encoder encoder{&builder};

  const std::string file_name = "";
  encoder.WriteString(file_name);

  constexpr auto file_size = 24UL;
  encoder.WriteUint(file_size);

  constexpr auto data_size = 18;  // 2*1B for markers + 8B string size + 8B uint
  constexpr auto total_size = data_size + sizeof(SegmentSize);

  builder.FlushInternal(total_size, false);

  // We don't send first 4B because they are reserved for size which we don't test here§
  ASSERT_FALSE(
      file_replication_handler.OpenFile(buffer.data() + sizeof(SegmentSize), buffer.size() - sizeof(SegmentSize))
          .has_value());
}

TEST_F(FileReplicationHandlerTest, OpenFileWithParentPath) {
  FileReplicationHandler file_replication_handler;
  std::vector<uint8_t> buffer;
  Builder builder([&buffer](const uint8_t *data, size_t size, bool have_more) {
    for (size_t i = 0; i < size; ++i) buffer.push_back(data[i]);
  });

  Encoder encoder{&builder};

  const std::string file_name = "random_dir/random_path_123";
  encoder.WriteString(file_name);

  constexpr auto file_size = 24UL;
  encoder.WriteUint(file_size);

  constexpr auto data_size = 45;  // 2*1B for markers + 8B string size + 8B uint + 27B
  constexpr auto total_size = data_size + sizeof(SegmentSize);

  builder.FlushInternal(total_size, false);

  // We don't send first 4B because they are reserved for size which we don't test here§
  ASSERT_FALSE(
      file_replication_handler.OpenFile(buffer.data() + sizeof(SegmentSize), buffer.size() - sizeof(SegmentSize))
          .has_value());
}

TEST_F(FileReplicationHandlerTest, OpenFileNoData) {
  FileReplicationHandler file_replication_handler;
  std::vector<uint8_t> buffer;
  Builder builder([&buffer](const uint8_t *data, size_t size, bool have_more) {
    for (size_t i = 0; i < size; ++i) buffer.push_back(data[i]);
  });

  Encoder encoder{&builder};

  const std::string file_name = "random_path_123";  // 15
  encoder.WriteString(file_name);

  constexpr auto file_size = 24UL;
  encoder.WriteUint(file_size);

  constexpr auto data_size = 33;  // 2*1B for markers + 8B string size + 8B uint + 15B for string
  constexpr auto total_size = data_size + sizeof(SegmentSize);

  builder.FlushInternal(total_size, false);

  // We don't send first 4B because they are reserved for size which we don't test here§
  ASSERT_EQ(file_replication_handler.OpenFile(buffer.data() + sizeof(SegmentSize), buffer.size() - sizeof(SegmentSize)),
            data_size);

  auto const &active_files = file_replication_handler.GetActiveFileNames();
  ASSERT_EQ(active_files.size(), 1);
  ASSERT_EQ(active_files[0].filename(), file_name);
  ASSERT_TRUE(file_replication_handler.HasOpenedFile());
  ASSERT_EQ(file_replication_handler.GetRemainingBytesToWrite(), file_size);
}

TEST_F(FileReplicationHandlerTest, FileWithData) {
  FileReplicationHandler file_replication_handler;
  std::vector<uint8_t> buffer;
  Builder builder([&buffer](const uint8_t *data, size_t size, bool have_more) {
    for (size_t i = 0; i < size; ++i) buffer.push_back(data[i]);
  });

  Encoder encoder{&builder};

  const std::string file_name = "random_path_123";  // 15
  encoder.WriteString(file_name);

  constexpr auto file_size = 24UL;
  encoder.WriteUint(file_size);

  auto file_data = GetRandomData(24);
  builder.Save(file_data.data(), file_data.size());

  constexpr auto data_size = 57;  // 2*1B for markers + 8B string size + 8B uint + 15B for string + 24B of file data
  constexpr auto total_size = data_size + sizeof(SegmentSize);

  builder.FlushInternal(total_size, false);

  // We don't send first 4B because they are reserved for size which we don't test here§
  ASSERT_EQ(file_replication_handler.OpenFile(buffer.data() + sizeof(SegmentSize), buffer.size() - sizeof(SegmentSize)),
            data_size);

  auto const &active_files = file_replication_handler.GetActiveFileNames();
  ASSERT_EQ(active_files.size(), 1);
  ASSERT_EQ(active_files[0].filename(), file_name);
  ASSERT_FALSE(file_replication_handler.HasOpenedFile());
  ASSERT_EQ(file_replication_handler.GetRemainingBytesToWrite(), 0);
}
