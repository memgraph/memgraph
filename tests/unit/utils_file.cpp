// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <chrono>
#include <fstream>
#include <map>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include <gtest/gtest.h>

#include "utils/file.hpp"
#include "utils/spin_lock.hpp"
#include "utils/string.hpp"
#include "utils/synchronized.hpp"

namespace fs = std::filesystem;

// Tests for InputFile::SetPosition buffer optimization
class InputFileSetPositionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = fs::temp_directory_path() / "MG_test_inputfile_setposition";
    fs::create_directories(test_dir_);
    test_file_ = test_dir_ / "test_data.bin";

    // Create a test file with known content (256 bytes: 0x00, 0x01, ..., 0xFF)
    std::ofstream ofs(test_file_, std::ios::binary);
    for (int i = 0; i < 256; ++i) {
      uint8_t byte = static_cast<uint8_t>(i);
      ofs.write(reinterpret_cast<char *>(&byte), 1);
    }
    ofs.close();
  }

  void TearDown() override {
    if (fs::exists(test_dir_)) {
      fs::remove_all(test_dir_);
    }
  }

  fs::path test_dir_;
  fs::path test_file_;
};

TEST_F(InputFileSetPositionTest, SeekWithinBufferBackward) {
  memgraph::utils::InputFile file;
  ASSERT_TRUE(file.Open(test_file_));

  // Read some data to populate the buffer
  uint8_t buffer[10];
  ASSERT_TRUE(file.Read(buffer, 10));

  // Verify we read correctly
  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(buffer[i], static_cast<uint8_t>(i));
  }

  EXPECT_EQ(file.GetPosition(), 10);

  // Seek backward within the buffer (to position 5)
  auto pos = file.SetPosition(memgraph::utils::InputFile::Position::SET, 5);
  ASSERT_TRUE(pos.has_value());
  EXPECT_EQ(*pos, 5);
  EXPECT_EQ(file.GetPosition(), 5);

  // Read and verify - should get bytes 5, 6, 7, 8, 9
  uint8_t verify[5];
  ASSERT_TRUE(file.Read(verify, 5));
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(verify[i], static_cast<uint8_t>(5 + i));
  }
}

TEST_F(InputFileSetPositionTest, SeekWithinBufferToStart) {
  memgraph::utils::InputFile file;
  ASSERT_TRUE(file.Open(test_file_));

  // Read some data to populate the buffer
  uint8_t buffer[20];
  ASSERT_TRUE(file.Read(buffer, 20));

  // Seek back to the start of the buffer (position 0)
  auto pos = file.SetPosition(memgraph::utils::InputFile::Position::SET, 0);
  ASSERT_TRUE(pos.has_value());
  EXPECT_EQ(*pos, 0);
  EXPECT_EQ(file.GetPosition(), 0);

  // Read and verify - should get bytes 0, 1, 2, ...
  uint8_t verify[10];
  ASSERT_TRUE(file.Read(verify, 10));
  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(verify[i], static_cast<uint8_t>(i));
  }
}

TEST_F(InputFileSetPositionTest, SeekBeforeBufferInvalidatesIt) {
  memgraph::utils::InputFile file;
  ASSERT_TRUE(file.Open(test_file_));

  // Seek to position 50 first
  file.SetPosition(memgraph::utils::InputFile::Position::SET, 50);

  // Read some data to populate the buffer starting at position 50
  uint8_t buffer[20];
  ASSERT_TRUE(file.Read(buffer, 20));

  // Verify we read correctly (bytes 50-69)
  for (int i = 0; i < 20; ++i) {
    EXPECT_EQ(buffer[i], static_cast<uint8_t>(50 + i));
  }

  // Seek before the buffer start (to position 10, which is before 50)
  auto pos = file.SetPosition(memgraph::utils::InputFile::Position::SET, 10);
  ASSERT_TRUE(pos.has_value());
  EXPECT_EQ(*pos, 10);

  // Read and verify - should get bytes 10, 11, 12, ...
  uint8_t verify[10];
  ASSERT_TRUE(file.Read(verify, 10));
  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(verify[i], static_cast<uint8_t>(10 + i));
  }
}

TEST_F(InputFileSetPositionTest, SeekPastBufferEndInvalidatesIt) {
  memgraph::utils::InputFile file;
  ASSERT_TRUE(file.Open(test_file_));

  // Read a small amount to populate a buffer
  uint8_t buffer[10];
  ASSERT_TRUE(file.Read(buffer, 10));

  // Seek way past the buffer
  auto pos = file.SetPosition(memgraph::utils::InputFile::Position::SET, 200);
  ASSERT_TRUE(pos.has_value());
  EXPECT_EQ(*pos, 200);

  // Read and verify - should get bytes 200, 201, ...
  uint8_t verify[10];
  ASSERT_TRUE(file.Read(verify, 10));
  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(verify[i], static_cast<uint8_t>(200 + i));
  }
}

TEST_F(InputFileSetPositionTest, MultipleSeeksWithinBuffer) {
  memgraph::utils::InputFile file;
  ASSERT_TRUE(file.Open(test_file_));

  // Read to populate buffer
  uint8_t buffer[50];
  ASSERT_TRUE(file.Read(buffer, 50));

  // Multiple seeks within the buffer
  for (int target : {25, 10, 40, 5, 30, 0, 49}) {
    auto pos = file.SetPosition(memgraph::utils::InputFile::Position::SET, target);
    ASSERT_TRUE(pos.has_value());
    EXPECT_EQ(*pos, target);
    EXPECT_EQ(file.GetPosition(), target);

    uint8_t byte;
    ASSERT_TRUE(file.Read(&byte, 1));
    EXPECT_EQ(byte, static_cast<uint8_t>(target));
  }
}

TEST_F(InputFileSetPositionTest, SeekRelativeToCurrentWithinBuffer) {
  memgraph::utils::InputFile file;
  ASSERT_TRUE(file.Open(test_file_));

  // Read to populate buffer and move position to 20
  uint8_t buffer[20];
  ASSERT_TRUE(file.Read(buffer, 20));
  EXPECT_EQ(file.GetPosition(), 20);

  // Seek backward relative to current (20 - 10 = 10)
  auto pos = file.SetPosition(memgraph::utils::InputFile::Position::RELATIVE_TO_CURRENT, -10);
  ASSERT_TRUE(pos.has_value());
  EXPECT_EQ(*pos, 10);

  uint8_t byte;
  ASSERT_TRUE(file.Read(&byte, 1));
  EXPECT_EQ(byte, 10);
}

TEST_F(InputFileSetPositionTest, SeekWithNoExistingBuffer) {
  memgraph::utils::InputFile file;
  ASSERT_TRUE(file.Open(test_file_));

  // Seek without reading first (no buffer populated)
  auto pos = file.SetPosition(memgraph::utils::InputFile::Position::SET, 100);
  ASSERT_TRUE(pos.has_value());
  EXPECT_EQ(*pos, 100);

  uint8_t byte;
  ASSERT_TRUE(file.Read(&byte, 1));
  EXPECT_EQ(byte, 100);
}

TEST_F(InputFileSetPositionTest, SeekToPositionZero) {
  memgraph::utils::InputFile file;
  ASSERT_TRUE(file.Open(test_file_));

  // Read to populate buffer
  uint8_t buffer[100];
  ASSERT_TRUE(file.Read(buffer, 100));

  // Seek back to 0
  auto pos = file.SetPosition(memgraph::utils::InputFile::Position::SET, 0);
  ASSERT_TRUE(pos.has_value());
  EXPECT_EQ(*pos, 0);
  EXPECT_EQ(file.GetPosition(), 0);

  // Read first byte
  uint8_t byte;
  ASSERT_TRUE(file.Read(&byte, 1));
  EXPECT_EQ(byte, 0);
}

TEST_F(InputFileSetPositionTest, SeekPastEndOfFile) {
  memgraph::utils::InputFile file;
  ASSERT_TRUE(file.Open(test_file_));

  // Seek past end of file (file is 256 bytes)
  auto pos = file.SetPosition(memgraph::utils::InputFile::Position::SET, 300);
  ASSERT_TRUE(pos.has_value());
  EXPECT_EQ(*pos, 300);

  // Reading should fail since we're past EOF
  uint8_t byte;
  EXPECT_FALSE(file.Read(&byte, 1));
}

TEST_F(InputFileSetPositionTest, RepeatedSeekAndReadCycle) {
  memgraph::utils::InputFile file;
  ASSERT_TRUE(file.Open(test_file_));

  // Simulate a pattern of seeking and reading that exercises buffer reuse
  for (int iteration = 0; iteration < 5; ++iteration) {
    // Read forward
    uint8_t buffer[30];
    ASSERT_TRUE(file.Read(buffer, 30));

    // Seek back within buffer multiple times
    for (int back = 20; back >= 0; back -= 5) {
      auto pos = file.SetPosition(memgraph::utils::InputFile::Position::SET, back);
      ASSERT_TRUE(pos.has_value());

      uint8_t byte;
      ASSERT_TRUE(file.Read(&byte, 1));
      EXPECT_EQ(byte, static_cast<uint8_t>(back));
    }

    // Reset to start for next iteration
    file.SetPosition(memgraph::utils::InputFile::Position::SET, 0);
  }
}

// This test exposes a bug in the SetPosition buffer optimization:
// When seeking within the buffer, lseek() moves the fd position, but the buffer is reused.
// Later, when the buffer is exhausted and LoadBuffer() is called, it reads from the
// wrong fd position (where lseek moved it) instead of from buffer_start_ + buffer_size_.
TEST_F(InputFileSetPositionTest, SeekWithinBufferThenExhaustBuffer) {
  // Create a file larger than kFileBufferSize (262144 bytes)
  constexpr size_t kLargeFileSize = memgraph::utils::kFileBufferSize + 100'000;
  auto large_file = test_dir_ / "large_file.bin";

  // Fill with sequential bytes (mod 256 to fit in uint8_t)
  {
    std::ofstream ofs(large_file, std::ios::binary);
    for (size_t i = 0; i < kLargeFileSize; ++i) {
      uint8_t byte = static_cast<uint8_t>(i % 256);
      ofs.write(reinterpret_cast<char *>(&byte), 1);
    }
    ofs.close();
  }

  memgraph::utils::InputFile file;
  ASSERT_TRUE(file.Open(large_file));

  // Step 1: Read a small amount to trigger buffer load
  // This loads kFileBufferSize bytes into buffer, fd moves to kFileBufferSize
  uint8_t initial_read[100];
  ASSERT_TRUE(file.Read(initial_read, 100));

  // Verify initial read is correct
  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(initial_read[i], static_cast<uint8_t>(i % 256));
  }

  // Current state:
  // - buffer contains bytes [0, kFileBufferSize)
  // - buffer_position_ = 100
  // - fd position = kFileBufferSize (after LoadBuffer)

  // Step 2: Seek backward within the buffer (e.g., to position 50)
  // With the buggy optimization:
  // - lseek() moves fd to 50
  // - But buffer is reused, buffer_position_ = 50
  auto pos = file.SetPosition(memgraph::utils::InputFile::Position::SET, 50);
  ASSERT_TRUE(pos.has_value());
  EXPECT_EQ(*pos, 50);

  // Step 3: Read enough data to exhaust the buffer and require LoadBuffer
  // We're at position 50 in the buffer, buffer has kFileBufferSize bytes
  // So we have (kFileBufferSize - 50) bytes left in buffer
  // Read more than that to force LoadBuffer
  size_t bytes_left_in_buffer = memgraph::utils::kFileBufferSize - 50;
  size_t read_size = bytes_left_in_buffer + 1000;  // Read past buffer end

  std::vector<uint8_t> large_read(read_size);
  ASSERT_TRUE(file.Read(large_read.data(), read_size));

  // Step 4: Verify the data
  // Correct behavior: bytes 50 through (50 + read_size - 1)
  // Buggy behavior: first (kFileBufferSize - 50) bytes are correct,
  //                 then LoadBuffer reads from fd position 50 (wrong!)
  //                 so we'd get bytes 50-... again instead of kFileBufferSize-...

  for (size_t i = 0; i < read_size; ++i) {
    size_t expected_file_pos = 50 + i;
    uint8_t expected_byte = static_cast<uint8_t>(expected_file_pos % 256);
    EXPECT_EQ(large_read[i], expected_byte)
        << "Mismatch at index " << i << " (file position " << expected_file_pos << "): "
        << "expected " << static_cast<int>(expected_byte) << ", got " << static_cast<int>(large_read[i]);
  }
}

// Tests for OutputFile::SetPosition
class OutputFileSetPositionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = fs::temp_directory_path() / "MG_test_outputfile_setposition";
    fs::create_directories(test_dir_);
    test_file_ = test_dir_ / "test_output.bin";
  }

  void TearDown() override {
    if (fs::exists(test_dir_)) {
      fs::remove_all(test_dir_);
    }
  }

  // Helper to read file contents
  std::vector<uint8_t> ReadFileContents(const fs::path &path) {
    std::ifstream ifs(path, std::ios::binary);
    return std::vector<uint8_t>(std::istreambuf_iterator<char>(ifs), std::istreambuf_iterator<char>());
  }

  fs::path test_dir_;
  fs::path test_file_;
};

TEST_F(OutputFileSetPositionTest, BasicSetPositionAbsolute) {
  memgraph::utils::OutputFile file;
  file.Open(test_file_, memgraph::utils::OutputFile::Mode::OVERWRITE_EXISTING);

  // Write some data
  uint8_t data[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  file.Write(data, 10);

  // Seek to position 5
  auto pos = file.SetPosition(memgraph::utils::OutputFile::Position::SET, 5);
  EXPECT_EQ(pos, 5);
  EXPECT_EQ(file.GetPosition(), 5);

  // Overwrite from position 5
  uint8_t new_data[] = {50, 51, 52};
  file.Write(new_data, 3);

  file.Sync();
  file.Close();

  // Verify file contents: 0,1,2,3,4,50,51,52,8,9
  auto contents = ReadFileContents(test_file_);
  ASSERT_EQ(contents.size(), 10);
  EXPECT_EQ(contents[0], 0);
  EXPECT_EQ(contents[4], 4);
  EXPECT_EQ(contents[5], 50);
  EXPECT_EQ(contents[6], 51);
  EXPECT_EQ(contents[7], 52);
  EXPECT_EQ(contents[8], 8);
  EXPECT_EQ(contents[9], 9);
}

TEST_F(OutputFileSetPositionTest, SetPositionRelativeToCurrentAfterWrite) {
  memgraph::utils::OutputFile file;
  file.Open(test_file_, memgraph::utils::OutputFile::Mode::OVERWRITE_EXISTING);

  // Write 20 bytes
  uint8_t data[20];
  for (int i = 0; i < 20; ++i) data[i] = static_cast<uint8_t>(i);
  file.Write(data, 20);

  // Position should be 20
  EXPECT_EQ(file.GetPosition(), 20);

  // Seek backward relative to current (20 - 10 = 10)
  auto pos = file.SetPosition(memgraph::utils::OutputFile::Position::RELATIVE_TO_CURRENT, -10);
  EXPECT_EQ(pos, 10);
  EXPECT_EQ(file.GetPosition(), 10);

  // Overwrite at position 10
  uint8_t new_data[] = {100, 101, 102};
  file.Write(new_data, 3);

  file.Sync();
  file.Close();

  // Verify file contents
  auto contents = ReadFileContents(test_file_);
  ASSERT_EQ(contents.size(), 20);
  EXPECT_EQ(contents[9], 9);
  EXPECT_EQ(contents[10], 100);
  EXPECT_EQ(contents[11], 101);
  EXPECT_EQ(contents[12], 102);
  EXPECT_EQ(contents[13], 13);
}

TEST_F(OutputFileSetPositionTest, SetPositionRelativeToEnd) {
  memgraph::utils::OutputFile file;
  file.Open(test_file_, memgraph::utils::OutputFile::Mode::OVERWRITE_EXISTING);

  // Write 20 bytes
  uint8_t data[20];
  for (int i = 0; i < 20; ++i) data[i] = static_cast<uint8_t>(i);
  file.Write(data, 20);
  file.Sync();

  // Seek to 5 bytes before end
  auto pos = file.SetPosition(memgraph::utils::OutputFile::Position::RELATIVE_TO_END, -5);
  EXPECT_EQ(pos, 15);
  EXPECT_EQ(file.GetPosition(), 15);

  // Overwrite
  uint8_t new_data[] = {150, 151};
  file.Write(new_data, 2);

  file.Sync();
  file.Close();

  auto contents = ReadFileContents(test_file_);
  ASSERT_EQ(contents.size(), 20);
  EXPECT_EQ(contents[14], 14);
  EXPECT_EQ(contents[15], 150);
  EXPECT_EQ(contents[16], 151);
  EXPECT_EQ(contents[17], 17);
}

TEST_F(OutputFileSetPositionTest, SetPositionToZero) {
  memgraph::utils::OutputFile file;
  file.Open(test_file_, memgraph::utils::OutputFile::Mode::OVERWRITE_EXISTING);

  // Write some data
  uint8_t data[] = {10, 11, 12, 13, 14};
  file.Write(data, 5);

  // Seek to beginning
  auto pos = file.SetPosition(memgraph::utils::OutputFile::Position::SET, 0);
  EXPECT_EQ(pos, 0);

  // Overwrite from beginning
  uint8_t new_data[] = {100, 101};
  file.Write(new_data, 2);

  file.Sync();
  file.Close();

  auto contents = ReadFileContents(test_file_);
  ASSERT_EQ(contents.size(), 5);
  EXPECT_EQ(contents[0], 100);
  EXPECT_EQ(contents[1], 101);
  EXPECT_EQ(contents[2], 12);
}

TEST_F(OutputFileSetPositionTest, MultipleSeeksAndWrites) {
  memgraph::utils::OutputFile file;
  file.Open(test_file_, memgraph::utils::OutputFile::Mode::OVERWRITE_EXISTING);

  // Write initial 30 bytes
  uint8_t data[30];
  for (int i = 0; i < 30; ++i) data[i] = static_cast<uint8_t>(i);
  file.Write(data, 30);

  // Multiple seeks and writes
  file.SetPosition(memgraph::utils::OutputFile::Position::SET, 5);
  uint8_t w1 = 55;
  file.Write(&w1, 1);

  file.SetPosition(memgraph::utils::OutputFile::Position::SET, 15);
  uint8_t w2 = 155;
  file.Write(&w2, 1);

  file.SetPosition(memgraph::utils::OutputFile::Position::SET, 25);
  uint8_t w3 = 255;
  file.Write(&w3, 1);

  file.Sync();
  file.Close();

  auto contents = ReadFileContents(test_file_);
  ASSERT_EQ(contents.size(), 30);
  EXPECT_EQ(contents[5], 55);
  EXPECT_EQ(contents[15], 155);
  EXPECT_EQ(contents[25], 255);
  // Unchanged positions
  EXPECT_EQ(contents[4], 4);
  EXPECT_EQ(contents[6], 6);
}

TEST_F(OutputFileSetPositionTest, GetPositionAfterWrite) {
  memgraph::utils::OutputFile file;
  file.Open(test_file_, memgraph::utils::OutputFile::Mode::OVERWRITE_EXISTING);

  EXPECT_EQ(file.GetPosition(), 0);

  uint8_t data[50];
  file.Write(data, 50);
  EXPECT_EQ(file.GetPosition(), 50);

  file.Write(data, 25);
  EXPECT_EQ(file.GetPosition(), 75);

  file.SetPosition(memgraph::utils::OutputFile::Position::SET, 10);
  EXPECT_EQ(file.GetPosition(), 10);

  file.Write(data, 5);
  EXPECT_EQ(file.GetPosition(), 15);

  file.Close();
}

TEST_F(OutputFileSetPositionTest, RelativeToCurrentMultipleTimes) {
  memgraph::utils::OutputFile file;
  file.Open(test_file_, memgraph::utils::OutputFile::Mode::OVERWRITE_EXISTING);

  // Write 100 bytes
  uint8_t data[100];
  for (int i = 0; i < 100; ++i) data[i] = static_cast<uint8_t>(i);
  file.Write(data, 100);

  // Position is now 100
  EXPECT_EQ(file.GetPosition(), 100);

  // Go back 30
  auto pos = file.SetPosition(memgraph::utils::OutputFile::Position::RELATIVE_TO_CURRENT, -30);
  EXPECT_EQ(pos, 70);

  // Go back another 20
  pos = file.SetPosition(memgraph::utils::OutputFile::Position::RELATIVE_TO_CURRENT, -20);
  EXPECT_EQ(pos, 50);

  // Go forward 10
  pos = file.SetPosition(memgraph::utils::OutputFile::Position::RELATIVE_TO_CURRENT, 10);
  EXPECT_EQ(pos, 60);

  // Write at position 60
  uint8_t mark = 0xAB;
  file.Write(&mark, 1);

  file.Sync();
  file.Close();

  auto contents = ReadFileContents(test_file_);
  EXPECT_EQ(contents[60], 0xAB);
  EXPECT_EQ(contents[59], 59);
  EXPECT_EQ(contents[61], 61);
}

const std::vector<std::string> kDirsAll = {"existing_dir_777",
                                           "existing_dir_770",
                                           "existing_dir_700",
                                           "existing_dir_000",
                                           "symlink_dir_777",
                                           "symlink_dir_770",
                                           "symlink_dir_700",
                                           "symlink_dir_000"};

const std::vector<std::string> kFilesAll = {"existing_file_666",
                                            "existing_file_660",
                                            "existing_file_600",
                                            "existing_file_000",
                                            "symlink_file_666",
                                            "symlink_file_660",
                                            "symlink_file_600",
                                            "symlink_file_000"};

const std::map<std::string, fs::perms> kPermsAll = {
    {"777", fs::perms::owner_all | fs::perms::group_all | fs::perms::others_all},
    {"770", fs::perms::owner_all | fs::perms::group_all},
    {"700", fs::perms::owner_all},
    {"666",
     fs::perms::owner_read | fs::perms::owner_write | fs::perms::group_read | fs::perms::group_write |
         fs::perms::others_read | fs::perms::others_write},
    {"660", fs::perms::owner_read | fs::perms::owner_write | fs::perms::group_read | fs::perms::group_write},
    {"600", fs::perms::owner_read | fs::perms::owner_write},
    {"000", fs::perms::none},
};

fs::perms GetPermsFromFilename(const std::string &name) {
  auto split = memgraph::utils::Split(name, "_");
  return kPermsAll.at(split[split.size() - 1]);
}

void CreateFile(const fs::path &path) {
  std::ofstream stream(path);
  stream << "hai hai hai hai!" << std::endl << "nandare!!!" << std::endl;
}

void CreateFiles(const fs::path &path) {
  CreateFile(path / "existing_file_666");
  fs::create_symlink(path / "existing_file_666", path / "symlink_file_666");
  fs::permissions(path / "existing_file_666",
                  fs::perms::owner_read | fs::perms::owner_write | fs::perms::group_read | fs::perms::group_write |
                      fs::perms::others_read | fs::perms::others_write);

  CreateFile(path / "existing_file_660");
  fs::create_symlink(path / "existing_file_660", path / "symlink_file_660");
  fs::permissions(path / "existing_file_660",
                  fs::perms::owner_read | fs::perms::owner_write | fs::perms::group_read | fs::perms::group_write);

  CreateFile(path / "existing_file_600");
  fs::create_symlink(path / "existing_file_600", path / "symlink_file_600");
  fs::permissions(path / "existing_file_600", fs::perms::owner_read | fs::perms::owner_write);

  CreateFile(path / "existing_file_000");
  fs::create_symlink(path / "existing_file_000", path / "symlink_file_000");
  fs::permissions(path / "existing_file_000", fs::perms::none);
}

class GetUniqueDownloadPathTest : public testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = std::filesystem::temp_directory_path() / "get_unique_path_test";
    std::filesystem::create_directories(test_dir_);
  }

  void TearDown() override {
    if (std::filesystem::exists(test_dir_)) {
      std::filesystem::remove_all(test_dir_);
    }
  }

  // Creates empty file
  static void CreateFile(std::filesystem::path const &path) {
    std::ofstream file(path);
    file.close();
  }

  std::filesystem::path test_dir_;
};

using memgraph::utils::CreateUniqueDownloadFile;

TEST_F(GetUniqueDownloadPathTest, ThreadSafeFiles) {
  auto const path = test_dir_ / "thread.csv";
  // hardware_concurrency can return 0, should be considered only a hint
  auto const num_threads = std::max(std::thread::hardware_concurrency(), 8U);

  std::vector<std::filesystem::path> storage(num_threads);
  for (auto i = 0U; i < num_threads; i++) {
    std::jthread exec{
        [&path, thread_id = i, &storage]() { storage[thread_id] = CreateUniqueDownloadFile(path).first; }};
  }

  std::unordered_set<std::filesystem::path> checker;
  for (auto const &path : storage) {
    ASSERT_TRUE(checker.insert(path).second);
  }
}

TEST_F(GetUniqueDownloadPathTest, CreateFirstAttempt) {
  auto const path = test_dir_ / "nonexistent.csv";
  ASSERT_EQ(path, CreateUniqueDownloadFile(path).first);
}

TEST_F(GetUniqueDownloadPathTest, CreateSecondAttempt) {
  auto const path = test_dir_ / "file.csv";
  CreateFile(path);
  auto const new_path = test_dir_ / "file_1.csv";
  ASSERT_EQ(new_path, CreateUniqueDownloadFile(path).first);
}

TEST_F(GetUniqueDownloadPathTest, MultipleFilesExist) {
  auto base = test_dir_ / "file.txt";
  CreateFile(base);
  CreateFile(test_dir_ / "file_1.txt");
  CreateFile(test_dir_ / "file_2.txt");

  auto result = CreateUniqueDownloadFile(base).first;

  EXPECT_EQ(result, test_dir_ / "file_3.txt");
}

TEST_F(GetUniqueDownloadPathTest, GapInSequence) {
  auto base = test_dir_ / "file.txt";
  CreateFile(base);
  CreateFile(test_dir_ / "file_1.txt");
  // Skip file_2.txt
  CreateFile(test_dir_ / "file_3.txt");

  auto result = CreateUniqueDownloadFile(base).first;

  // Should return _2 since _1 exists but _2 doesn't
  EXPECT_EQ(result, test_dir_ / "file_2.txt");
}

TEST_F(GetUniqueDownloadPathTest, NoExtension) {
  auto base = test_dir_ / "file";
  CreateFile(base);

  auto result = CreateUniqueDownloadFile(base).first;

  EXPECT_EQ(result, test_dir_ / "file_1");
}

TEST_F(GetUniqueDownloadPathTest, MultipleExtensions) {
  auto base = test_dir_ / "archive.tar.gz";
  CreateFile(base);

  auto result = CreateUniqueDownloadFile(base).first;

  EXPECT_EQ(result, test_dir_ / "archive.tar_1.gz");
}

TEST_F(GetUniqueDownloadPathTest, LargeSequenceNumber) {
  auto base = test_dir_ / "file.txt";
  CreateFile(base);

  // Create files up to _99
  for (int i = 1; i <= 99; ++i) {
    CreateFile(test_dir_ / std::format("file_{}.txt", i));
  }

  auto result = CreateUniqueDownloadFile(base).first;

  EXPECT_EQ(result, test_dir_ / "file_100.txt");
}

TEST_F(GetUniqueDownloadPathTest, MaxSuffixReached) {
  auto base = test_dir_ / "file.txt";
  CreateFile(base);

  // Create files up to and including _10000
  for (int i = 1; i <= 10'000; ++i) {
    CreateFile(test_dir_ / std::format("file_{}.txt", i));
  }

  EXPECT_THROW(CreateUniqueDownloadFile(base), memgraph::utils::BasicException);
}

class UtilsFileTest : public ::testing::Test {
 public:
  void SetUp() override {
    Clear();
    fs::create_directory(storage);

    fs::create_directory(storage / "existing_dir_777");
    fs::create_directory_symlink(storage / "existing_dir_777", storage / "symlink_dir_777");
    CreateFiles(storage / "existing_dir_777");

    fs::create_directory(storage / "existing_dir_770");
    fs::create_directory_symlink(storage / "existing_dir_770", storage / "symlink_dir_770");
    CreateFiles(storage / "existing_dir_770");

    fs::create_directory(storage / "existing_dir_700");
    fs::create_directory_symlink(storage / "existing_dir_700", storage / "symlink_dir_700");
    CreateFiles(storage / "existing_dir_700");

    fs::create_directory(storage / "existing_dir_000");
    fs::create_directory_symlink(storage / "existing_dir_000", storage / "symlink_dir_000");
    CreateFiles(storage / "existing_dir_000");

    fs::permissions(storage / "existing_dir_777", fs::perms::owner_all | fs::perms::group_all | fs::perms::others_all);
    fs::permissions(storage / "existing_dir_770", fs::perms::owner_all | fs::perms::group_all);
    fs::permissions(storage / "existing_dir_700", fs::perms::owner_all);
    fs::permissions(storage / "existing_dir_000", fs::perms::none);
  }

  void TearDown() override {
    // Validate that the test structure was left intact.
    for (const auto &dir : kDirsAll) {
      {
        ASSERT_TRUE(fs::exists(storage / dir));
        auto dir_status = fs::symlink_status(storage / dir);
        if (!memgraph::utils::StartsWith(dir, "symlink")) {
          ASSERT_EQ(dir_status.permissions() & fs::perms::all, GetPermsFromFilename(dir));
        }
        fs::permissions(storage / dir, fs::perms::owner_all, fs::perm_options::add);
      }
      for (const auto &file : kFilesAll) {
        ASSERT_TRUE(fs::exists(storage / dir / file));
        auto file_status = fs::symlink_status(storage / dir / file);
        if (!memgraph::utils::StartsWith(file, "symlink")) {
          ASSERT_EQ(file_status.permissions() & fs::perms::all, GetPermsFromFilename(file));
        }
      }
    }

    Clear();
  }

  fs::path storage{fs::temp_directory_path() / "MG_test_unit_utils_file"};

 private:
  void Clear() {
    if (fs::exists(storage)) {
      for (const auto &file : fs::recursive_directory_iterator(storage)) {
        std::error_code error_code;  // For exception suppression.
        fs::permissions(file.path(), fs::perms::owner_all, fs::perm_options::add, error_code);
      }
      fs::remove_all(storage);
    }
  }
};

TEST(UtilsFile, PermissionDenied) {
  auto ret = memgraph::utils::ReadLines("/root/.bashrc");
  ASSERT_EQ(ret.size(), 0);
}

TEST_F(UtilsFileTest, ReadLines) {
  for (const auto &dir : kDirsAll) {
    for (const auto &file : kFilesAll) {
      auto ret = memgraph::utils::ReadLines(storage / dir / file);
      if (memgraph::utils::EndsWith(dir, "000") || memgraph::utils::EndsWith(file, "000")) {
        ASSERT_EQ(ret.size(), 0);
      } else {
        ASSERT_EQ(ret.size(), 2);
      }
    }
  }
}

TEST_F(UtilsFileTest, EnsureDirAndDeleteDir) {
  for (const auto &dir : kDirsAll) {
    for (const auto &file : kFilesAll) {
      ASSERT_FALSE(memgraph::utils::EnsureDir(storage / dir / file));
      ASSERT_FALSE(memgraph::utils::DeleteDir(storage / dir / file));
    }
    auto path = storage / dir / "test";
    auto ret = memgraph::utils::EnsureDir(path);
    if (memgraph::utils::EndsWith(dir, "000")) {
      ASSERT_FALSE(ret);
      ASSERT_FALSE(memgraph::utils::DeleteDir(path));
    } else {
      ASSERT_TRUE(ret);
      ASSERT_TRUE(fs::exists(path));
      ASSERT_TRUE(fs::is_directory(path));
      CreateFile(path / "test");
      ASSERT_TRUE(memgraph::utils::DeleteDir(path));
    }
  }
}

TEST_F(UtilsFileTest, EnsureDirOrDie) {
  for (const auto &dir : kDirsAll) {
    for (const auto &file : kFilesAll) {
      ASSERT_DEATH(memgraph::utils::EnsureDirOrDie(storage / dir / file), "");
    }
    auto path = storage / dir / "test";
    if (memgraph::utils::EndsWith(dir, "000")) {
      ASSERT_DEATH(memgraph::utils::EnsureDirOrDie(path), "");
    } else {
      memgraph::utils::EnsureDirOrDie(path);
    }
  }
}

TEST_F(UtilsFileTest, OutputFileExisting) {
  for (const auto &dir : kDirsAll) {
    for (const auto &file : kFilesAll) {
      memgraph::utils::OutputFile handle;
      if (memgraph::utils::EndsWith(dir, "000") || memgraph::utils::EndsWith(file, "000")) {
        ASSERT_DEATH(handle.Open(storage / dir / file, memgraph::utils::OutputFile::Mode::APPEND_TO_EXISTING), "");
      } else {
        handle.Open(storage / dir / file, memgraph::utils::OutputFile::Mode::APPEND_TO_EXISTING);
        ASSERT_TRUE(handle.IsOpen());
        ASSERT_EQ(handle.path(), storage / dir / file);
        handle.Write("hello world!\n", 13);
        handle.Sync();
        handle.Close();
      }
    }
  }
}

TEST_F(UtilsFileTest, OutputFileNew) {
  for (const auto &dir : kDirsAll) {
    memgraph::utils::OutputFile handle;
    auto path = storage / dir / "test";
    if (memgraph::utils::EndsWith(dir, "000")) {
      ASSERT_DEATH(handle.Open(path, memgraph::utils::OutputFile::Mode::APPEND_TO_EXISTING), "");
    } else {
      handle.Open(path, memgraph::utils::OutputFile::Mode::APPEND_TO_EXISTING);
      ASSERT_TRUE(handle.IsOpen());
      ASSERT_EQ(handle.path(), path);
      handle.Write("hello world!\n");
      handle.Sync();
      handle.Close();
    }
  }
}

TEST_F(UtilsFileTest, OutputFileInvalidUsage) {
  memgraph::utils::OutputFile handle;
  ASSERT_DEATH(handle.Write("hello!"), "");
  ASSERT_DEATH(handle.Sync(), "");
  ASSERT_DEATH(handle.Close(), "");
  handle.Open(storage / "existing_dir_777" / "existing_file_777",
              memgraph::utils::OutputFile::Mode::APPEND_TO_EXISTING);
  ASSERT_DEATH(handle.Open(storage / "existing_dir_770" / "existing_file_770",
                           memgraph::utils::OutputFile::Mode::APPEND_TO_EXISTING),
               "");
  handle.Write("hello!");
  handle.Sync();
  handle.Close();
}

TEST_F(UtilsFileTest, OutputFileMove) {
  memgraph::utils::OutputFile original;
  original.Open(storage / "existing_dir_777" / "existing_file_777",
                memgraph::utils::OutputFile::Mode::APPEND_TO_EXISTING);

  memgraph::utils::OutputFile moved(std::move(original));

  ASSERT_DEATH(original.Write("hello!"), "");
  ASSERT_DEATH(original.Sync(), "");
  ASSERT_DEATH(original.Close(), "");
  ASSERT_EQ(original.path(), "");
  ASSERT_FALSE(original.IsOpen());

  ASSERT_TRUE(moved.IsOpen());
  ASSERT_EQ(moved.path(), storage / "existing_dir_777" / "existing_file_777");
  moved.Write("hello!");
  moved.Sync();
  moved.Close();

  original.Open(storage / "existing_dir_770" / "existing_file_770",
                memgraph::utils::OutputFile::Mode::APPEND_TO_EXISTING);
  original.Close();
}

TEST_F(UtilsFileTest, OutputFileDescriptorLeackage) {
  for (int i = 0; i < 100'000; ++i) {
    memgraph::utils::OutputFile handle;
    handle.Open(storage / "existing_dir_777" / "existing_file_777",
                memgraph::utils::OutputFile::Mode::APPEND_TO_EXISTING);
  }
}

TEST_F(UtilsFileTest, ConcurrentReadingAndWritting) {
  const auto file_path = storage / "existing_dir_777" / "existing_file_777";
  memgraph::utils::OutputFile handle;
  handle.Open(file_path, memgraph::utils::OutputFile::Mode::OVERWRITE_EXISTING);

  std::uniform_int_distribution<int> random_short_wait(1, 10);

  const auto sleep_for = [&](int milliseconds) {
    std::this_thread::sleep_for(std::chrono::milliseconds(milliseconds));
  };

  static constexpr size_t number_of_writes = 500;
  std::thread writer_thread([&] {
    std::default_random_engine engine{586'478'780};
    uint8_t current_number = 0;
    for (size_t i = 0; i < number_of_writes; ++i) {
      handle.Write(&current_number, 1);
      ++current_number;
      handle.TryFlushing();
      sleep_for(random_short_wait(engine));
    }
  });

  static constexpr size_t reader_threads_num = 7;
  // number_of_reads needs to be higher than number_of_writes
  // so we maximize the chance of having at least one reading
  // thread that will read all of the data.
  static constexpr size_t number_of_reads = 550;
  std::vector<std::thread> reader_threads(reader_threads_num);
  memgraph::utils::Synchronized<std::vector<size_t>, memgraph::utils::SpinLock> max_read_counts;
  for (size_t i = 0; i < reader_threads_num; ++i) {
    reader_threads.emplace_back([&, thread_id = i] {
      std::default_random_engine engine{586'478'780 + thread_id};
      for (size_t i = 0; i < number_of_reads; ++i) {
        handle.DisableFlushing();
        auto [buffer, buffer_size] = handle.CurrentBuffer();
        memgraph::utils::InputFile input_handle;
        input_handle.Open(file_path);
        std::optional<uint8_t> previous_number;
        size_t total_read_count = 0;
        uint8_t current_number;
        // Read the file
        while (input_handle.Read(&current_number, 1)) {
          if (previous_number) {
            const uint8_t expected_next = *previous_number + 1;
            ASSERT_TRUE(current_number == expected_next);
          }
          previous_number = current_number;
          ++total_read_count;
        }
        // Read the buffer
        while (buffer_size > 0) {
          if (previous_number) {
            const uint8_t expected_next = *previous_number + 1;
            ASSERT_TRUE(*buffer == expected_next);
          }
          previous_number = *buffer;
          ++buffer;
          --buffer_size;
          ++total_read_count;
        }
        handle.EnableFlushing();
        input_handle.Close();
        // Last read will always have the highest amount of
        // bytes read.
        if (i == number_of_reads - 1) {
          max_read_counts.WithLock([&](auto &read_counts) { read_counts.push_back(total_read_count); });
        }
        sleep_for(random_short_wait(engine));
      }
    });
  }

  if (writer_thread.joinable()) {
    writer_thread.join();
  }
  for (auto &reader_thread : reader_threads) {
    if (reader_thread.joinable()) {
      reader_thread.join();
    }
  }

  handle.Close();
  // Check if any of the threads read the entire data.
  ASSERT_TRUE(max_read_counts.WithLock([&](auto &read_counts) {
    return std::any_of(
        read_counts.cbegin(), read_counts.cend(), [](const auto read_count) { return read_count == number_of_writes; });
  }));
}
