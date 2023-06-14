// Copyright 2022 Memgraph Ltd.
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
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "utils/file.hpp"
#include "utils/spin_lock.hpp"
#include "utils/string.hpp"
#include "utils/synchronized.hpp"

namespace fs = std::filesystem;

const std::vector<std::string> kDirsAll = {"existing_dir_777", "existing_dir_770", "existing_dir_700",
                                           "existing_dir_000", "symlink_dir_777",  "symlink_dir_770",
                                           "symlink_dir_700",  "symlink_dir_000"};

const std::vector<std::string> kFilesAll = {"existing_file_666", "existing_file_660", "existing_file_600",
                                            "existing_file_000", "symlink_file_666",  "symlink_file_660",
                                            "symlink_file_600",  "symlink_file_000"};

const std::map<std::string, fs::perms> kPermsAll = {
    {"777", fs::perms::owner_all | fs::perms::group_all | fs::perms::others_all},
    {"770", fs::perms::owner_all | fs::perms::group_all},
    {"700", fs::perms::owner_all},
    {"666", fs::perms::owner_read | fs::perms::owner_write | fs::perms::group_read | fs::perms::group_write |
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
  fs::permissions(path / "existing_file_666", fs::perms::owner_read | fs::perms::owner_write | fs::perms::group_read |
                                                  fs::perms::group_write | fs::perms::others_read |
                                                  fs::perms::others_write);

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
      for (auto &file : fs::recursive_directory_iterator(storage)) {
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

TEST_F(UtilsFileTest, OutputFileDescriptorLeakage) {
  for (int i = 0; i < 100000; ++i) {
    memgraph::utils::OutputFile handle;
    handle.Open(storage / "existing_dir_777" / "existing_file_777",
                memgraph::utils::OutputFile::Mode::APPEND_TO_EXISTING);
  }
}

TEST_F(UtilsFileTest, ConcurrentReadingAndWriting) {
  const auto file_path = storage / "existing_dir_777" / "existing_file_777";
  memgraph::utils::OutputFile handle;
  handle.Open(file_path, memgraph::utils::OutputFile::Mode::OVERWRITE_EXISTING);

  std::default_random_engine engine(586478780);
  std::uniform_int_distribution<int> random_short_wait(1, 10);

  const auto sleep_for = [&](int milliseconds) {
    std::this_thread::sleep_for(std::chrono::milliseconds(milliseconds));
  };

  static constexpr size_t number_of_writes = 500;
  std::thread writer_thread([&] {
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
    reader_threads.emplace_back([&] {
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
    return std::any_of(read_counts.cbegin(), read_counts.cend(),
                       [](const auto read_count) { return read_count == number_of_writes; });
  }));
}
