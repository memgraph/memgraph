#include <fstream>
#include <map>
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "utils/file.hpp"
#include "utils/string.hpp"

namespace fs = std::experimental::filesystem;

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
        fs::permissions(storage / dir,
                        fs::perms::add_perms | fs::perms::owner_all);
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
        fs::permissions(file.path(),
                        fs::perms::add_perms | fs::perms::owner_all,
                        error_code);
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

TEST_F(UtilsFileTest, LogFileExisting) {
  for (const auto &dir : kDirsAll) {
    for (const auto &file : kFilesAll) {
      utils::LogFile handle;
      if (utils::EndsWith(dir, "000") || utils::EndsWith(file, "000")) {
        ASSERT_DEATH(handle.Open(storage / dir / file), "");
      } else {
        handle.Open(storage / dir / file);
        ASSERT_TRUE(handle.IsOpen());
        ASSERT_EQ(handle.path(), storage / dir / file);
        handle.Write("hello world!\n", 13);
        handle.Sync();
        handle.Close();
      }
    }
  }
}

TEST_F(UtilsFileTest, LogFileNew) {
  for (const auto &dir : kDirsAll) {
    utils::LogFile handle;
    auto path = storage / dir / "test";
    if (utils::EndsWith(dir, "000")) {
      ASSERT_DEATH(handle.Open(path), "");
    } else {
      handle.Open(path);
      ASSERT_TRUE(handle.IsOpen());
      ASSERT_EQ(handle.path(), path);
      handle.Write("hello world!\n");
      handle.Sync();
      handle.Close();
    }
  }
}

TEST_F(UtilsFileTest, LogFileInvalidUsage) {
  utils::LogFile handle;
  ASSERT_DEATH(handle.Write("hello!"), "");
  ASSERT_DEATH(handle.Sync(), "");
  ASSERT_DEATH(handle.Close(), "");
  handle.Open(storage / "existing_dir_777" / "existing_file_777");
  ASSERT_DEATH(handle.Open(storage / "existing_dir_770" / "existing_file_770"),
               "");
  handle.Write("hello!");
  handle.Sync();
  handle.Close();
}

TEST_F(UtilsFileTest, LogFileMove) {
  utils::LogFile original;
  original.Open(storage / "existing_dir_777" / "existing_file_777");

  utils::LogFile moved(std::move(original));

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

  original.Open(storage / "existing_dir_770" / "existing_file_770");
  original.Close();
}

TEST_F(UtilsFileTest, LogFileDescriptorLeackage) {
  for (int i = 0; i < 100000; ++i) {
    utils::LogFile handle;
    handle.Open(storage / "existing_dir_777" / "existing_file_777");
  }
}
