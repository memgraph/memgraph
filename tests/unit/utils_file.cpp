#include <chrono>
#include <fstream>
#include <map>
#include <string>
#include <thread>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "utils/file.hpp"
#include "utils/string.hpp"

namespace fs = std::filesystem;

const std::vector<std::string> kDirsAll = {
    "existing_dir_777", "existing_dir_770", "existing_dir_700",
    "existing_dir_000", "symlink_dir_777",  "symlink_dir_770",
    "symlink_dir_700",  "symlink_dir_000"};

const std::vector<std::string> kFilesAll = {
    "existing_file_666", "existing_file_660", "existing_file_600",
    "existing_file_000", "symlink_file_666",  "symlink_file_660",
    "symlink_file_600",  "symlink_file_000"};

const std::map<std::string, fs::perms> kPermsAll = {
    {"777",
     fs::perms::owner_all | fs::perms::group_all | fs::perms::others_all},
    {"770", fs::perms::owner_all | fs::perms::group_all},
    {"700", fs::perms::owner_all},
    {"666", fs::perms::owner_read | fs::perms::owner_write |
                fs::perms::group_read | fs::perms::group_write |
                fs::perms::others_read | fs::perms::others_write},
    {"660", fs::perms::owner_read | fs::perms::owner_write |
                fs::perms::group_read | fs::perms::group_write},
    {"600", fs::perms::owner_read | fs::perms::owner_write},
    {"000", fs::perms::none},
};

fs::perms GetPermsFromFilename(const std::string &name) {
  auto split = utils::Split(name, "_");
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
                  fs::perms::owner_read | fs::perms::owner_write |
                      fs::perms::group_read | fs::perms::group_write |
                      fs::perms::others_read | fs::perms::others_write);

  CreateFile(path / "existing_file_660");
  fs::create_symlink(path / "existing_file_660", path / "symlink_file_660");
  fs::permissions(path / "existing_file_660",
                  fs::perms::owner_read | fs::perms::owner_write |
                      fs::perms::group_read | fs::perms::group_write);

  CreateFile(path / "existing_file_600");
  fs::create_symlink(path / "existing_file_600", path / "symlink_file_600");
  fs::permissions(path / "existing_file_600",
                  fs::perms::owner_read | fs::perms::owner_write);

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
    fs::create_directory_symlink(storage / "existing_dir_777",
                                 storage / "symlink_dir_777");
    CreateFiles(storage / "existing_dir_777");

    fs::create_directory(storage / "existing_dir_770");
    fs::create_directory_symlink(storage / "existing_dir_770",
                                 storage / "symlink_dir_770");
    CreateFiles(storage / "existing_dir_770");

    fs::create_directory(storage / "existing_dir_700");
    fs::create_directory_symlink(storage / "existing_dir_700",
                                 storage / "symlink_dir_700");
    CreateFiles(storage / "existing_dir_700");

    fs::create_directory(storage / "existing_dir_000");
    fs::create_directory_symlink(storage / "existing_dir_000",
                                 storage / "symlink_dir_000");
    CreateFiles(storage / "existing_dir_000");

    fs::permissions(storage / "existing_dir_777", fs::perms::owner_all |
                                                      fs::perms::group_all |
                                                      fs::perms::others_all);
    fs::permissions(storage / "existing_dir_770",
                    fs::perms::owner_all | fs::perms::group_all);
    fs::permissions(storage / "existing_dir_700", fs::perms::owner_all);
    fs::permissions(storage / "existing_dir_000", fs::perms::none);
  }

  void TearDown() override {
    // Validate that the test structure was left intact.
    for (const auto &dir : kDirsAll) {
      {
        ASSERT_TRUE(fs::exists(storage / dir));
        auto dir_status = fs::symlink_status(storage / dir);
        if (!utils::StartsWith(dir, "symlink")) {
          ASSERT_EQ(dir_status.permissions() & fs::perms::all,
                    GetPermsFromFilename(dir));
        }
        fs::permissions(storage / dir, fs::perms::owner_all,
                        fs::perm_options::add);
      }
      for (const auto &file : kFilesAll) {
        ASSERT_TRUE(fs::exists(storage / dir / file));
        auto file_status = fs::symlink_status(storage / dir / file);
        if (!utils::StartsWith(file, "symlink")) {
          ASSERT_EQ(file_status.permissions() & fs::perms::all,
                    GetPermsFromFilename(file));
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
        fs::permissions(file.path(), fs::perms::owner_all,
                        fs::perm_options::add, error_code);
      }
      fs::remove_all(storage);
    }
  }
};

TEST(UtilsFile, PermissionDenied) {
  auto ret = utils::ReadLines("/root/.bashrc");
  ASSERT_EQ(ret.size(), 0);
}

TEST_F(UtilsFileTest, ReadLines) {
  for (const auto &dir : kDirsAll) {
    for (const auto &file : kFilesAll) {
      auto ret = utils::ReadLines(storage / dir / file);
      if (utils::EndsWith(dir, "000") || utils::EndsWith(file, "000")) {
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
      ASSERT_FALSE(utils::EnsureDir(storage / dir / file));
      ASSERT_FALSE(utils::DeleteDir(storage / dir / file));
    }
    auto path = storage / dir / "test";
    auto ret = utils::EnsureDir(path);
    if (utils::EndsWith(dir, "000")) {
      ASSERT_FALSE(ret);
      ASSERT_FALSE(utils::DeleteDir(path));
    } else {
      ASSERT_TRUE(ret);
      ASSERT_TRUE(fs::exists(path));
      ASSERT_TRUE(fs::is_directory(path));
      CreateFile(path / "test");
      ASSERT_TRUE(utils::DeleteDir(path));
    }
  }
}

TEST_F(UtilsFileTest, EnsureDirOrDie) {
  for (const auto &dir : kDirsAll) {
    for (const auto &file : kFilesAll) {
      ASSERT_DEATH(utils::EnsureDirOrDie(storage / dir / file), "");
    }
    auto path = storage / dir / "test";
    if (utils::EndsWith(dir, "000")) {
      ASSERT_DEATH(utils::EnsureDirOrDie(path), "");
    } else {
      utils::EnsureDirOrDie(path);
    }
  }
}

TEST_F(UtilsFileTest, OutputFileExisting) {
  for (const auto &dir : kDirsAll) {
    for (const auto &file : kFilesAll) {
      utils::OutputFile handle;
      if (utils::EndsWith(dir, "000") || utils::EndsWith(file, "000")) {
        ASSERT_DEATH(handle.Open(storage / dir / file,
                                 utils::OutputFile::Mode::APPEND_TO_EXISTING),
                     "");
      } else {
        handle.Open(storage / dir / file,
                    utils::OutputFile::Mode::APPEND_TO_EXISTING);
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
    utils::OutputFile handle;
    auto path = storage / dir / "test";
    if (utils::EndsWith(dir, "000")) {
      ASSERT_DEATH(
          handle.Open(path, utils::OutputFile::Mode::APPEND_TO_EXISTING), "");
    } else {
      handle.Open(path, utils::OutputFile::Mode::APPEND_TO_EXISTING);
      ASSERT_TRUE(handle.IsOpen());
      ASSERT_EQ(handle.path(), path);
      handle.Write("hello world!\n");
      handle.Sync();
      handle.Close();
    }
  }
}

TEST_F(UtilsFileTest, OutputFileInvalidUsage) {
  utils::OutputFile handle;
  ASSERT_DEATH(handle.Write("hello!"), "");
  ASSERT_DEATH(handle.Sync(), "");
  ASSERT_DEATH(handle.Close(), "");
  handle.Open(storage / "existing_dir_777" / "existing_file_777",
              utils::OutputFile::Mode::APPEND_TO_EXISTING);
  ASSERT_DEATH(handle.Open(storage / "existing_dir_770" / "existing_file_770",
                           utils::OutputFile::Mode::APPEND_TO_EXISTING),
               "");
  handle.Write("hello!");
  handle.Sync();
  handle.Close();
}

TEST_F(UtilsFileTest, OutputFileMove) {
  utils::OutputFile original;
  original.Open(storage / "existing_dir_777" / "existing_file_777",
                utils::OutputFile::Mode::APPEND_TO_EXISTING);

  utils::OutputFile moved(std::move(original));

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
                utils::OutputFile::Mode::APPEND_TO_EXISTING);
  original.Close();
}

TEST_F(UtilsFileTest, OutputFileDescriptorLeackage) {
  for (int i = 0; i < 100000; ++i) {
    utils::OutputFile handle;
    handle.Open(storage / "existing_dir_777" / "existing_file_777",
                utils::OutputFile::Mode::APPEND_TO_EXISTING);
  }
}

TEST_F(UtilsFileTest, ConcurrentReadingAndWritting) {
  utils::OutputFile handle;
  handle.Open(storage / "existing_dir_777" / "existing_file_777",
              utils::OutputFile::Mode::OVERWRITE_EXISTING);

  std::thread writer_thread([&] {
    constexpr size_t number_of_writes = 1'000'000;
    uint8_t current_number = 0;
    for (size_t i = 0; i < number_of_writes; ++i) {
      handle.Write(&current_number, 1);
      ++current_number;
      handle.TryFlushing();
    }
  });

  constexpr size_t reader_threads_num = 7;
  std::vector<std::thread> reader_threads(reader_threads_num);
  for (size_t i = 0; i < reader_threads_num; ++i) {
    reader_threads.emplace_back([&] {
      constexpr size_t number_of_reads = 200;
      for (size_t i = 0; i < number_of_reads; ++i) {
        handle.DisableFlushing();

        auto [buffer, buffer_size] = handle.CurrentBuffer();
        utils::InputFile input_handle;
        // Read the file
        std::optional<uint8_t> previous_number;
        uint8_t current_number;
        while (input_handle.Read(&current_number, 1)) {
          if (previous_number) {
            const uint8_t expected_next = *previous_number + 1;
            ASSERT_TRUE(*buffer == expected_next);
          }
          previous_number = current_number;
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
        }
        handle.EnableFlushing();
        input_handle.Close();
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
}
