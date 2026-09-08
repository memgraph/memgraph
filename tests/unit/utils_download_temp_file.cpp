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

#include <fcntl.h>
#include <limits.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <filesystem>
#include <string>
#include <system_error>

#include <gtest/gtest.h>

#include "utils/download_temp_file.hpp"
#include "utils/on_scope_exit.hpp"

namespace fs = std::filesystem;

using memgraph::utils::DownloadTempFile;

class DownloadTempFileTest : public testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = fs::temp_directory_path() / "download_temp_file_test";
    fs::create_directories(test_dir_);
  }

  void TearDown() override {
    if (fs::exists(test_dir_)) {
      fs::remove_all(test_dir_);
    }
  }

  static auto ReadAll(int fd) -> std::string {
    std::string content;
    std::array<char, 64> buffer{};
    while (auto const bytes = ::read(fd, buffer.data(), buffer.size())) {
      if (bytes < 0) break;
      content.append(buffer.data(), static_cast<std::size_t>(bytes));
    }
    return content;
  }

  static void Write(DownloadTempFile &file, std::string_view content) {
    auto stream = file.OpenStream();
    ASSERT_TRUE(stream.has_value()) << stream.error().message();
    ASSERT_EQ(std::fwrite(content.data(), 1, content.size(), stream->get()), content.size());
  }

  fs::path test_dir_;
};

TEST_F(DownloadTempFileTest, ContentWrittenThroughTheStreamIsReadableThroughTheDescriptor) {
  auto file = DownloadTempFile::Create(test_dir_);
  ASSERT_TRUE(file.has_value()) << file.error().message();

  Write(*file, "parquet bytes");

  auto fd = file->DupFd();
  ASSERT_TRUE(fd.has_value()) << fd.error().message();

  EXPECT_EQ(ReadAll(fd->Get()), "parquet bytes");
}

TEST_F(DownloadTempFileTest, NoDirectoryEntryIsLeftBehind) {
  auto file = DownloadTempFile::Create(test_dir_);
  ASSERT_TRUE(file.has_value()) << file.error().message();

  EXPECT_TRUE(fs::is_empty(test_dir_));
}

TEST_F(DownloadTempFileTest, TheFileIsAlreadyUnlinked) {
  auto file = DownloadTempFile::Create(test_dir_);
  ASSERT_TRUE(file.has_value()) << file.error().message();

  auto fd = file->DupFd();
  ASSERT_TRUE(fd.has_value()) << fd.error().message();

  struct stat info{};
  ASSERT_EQ(::fstat(fd->Get(), &info), 0);
  EXPECT_EQ(info.st_nlink, 0);
}

#ifdef __linux__
// The only way to recover the path of an unlinked file, and procfs is not mounted everywhere.
TEST_F(DownloadTempFileTest, FileIsCreatedInTheRequestedDirectoryUnderNameMax) {
  auto file = DownloadTempFile::Create(test_dir_);
  ASSERT_TRUE(file.has_value()) << file.error().message();

  auto fd = file->DupFd();
  ASSERT_TRUE(fd.has_value()) << fd.error().message();

  auto const target = fs::read_symlink(fs::path{"/proc/self/fd"} / std::to_string(fd->Get()));
  EXPECT_EQ(target.parent_path(), test_dir_);
  EXPECT_TRUE(target.filename().string().starts_with("memgraph_download_"));
  EXPECT_LT(target.filename().string().size(), NAME_MAX);
}
#endif

TEST_F(DownloadTempFileTest, DuplicatedDescriptorOutlivesTheOwner) {
  std::optional<memgraph::utils::OwnedFd> fd;
  {
    auto file = DownloadTempFile::Create(test_dir_);
    ASSERT_TRUE(file.has_value()) << file.error().message();
    Write(*file, "outlives the owner");

    auto duplicated = file->DupFd();
    ASSERT_TRUE(duplicated.has_value()) << duplicated.error().message();
    fd = std::move(*duplicated);
  }

  EXPECT_EQ(ReadAll(fd->Get()), "outlives the owner");
}

TEST_F(DownloadTempFileTest, AnOwnedDescriptorClosesWhenItGoesOutOfScope) {
  auto file = DownloadTempFile::Create(test_dir_);
  ASSERT_TRUE(file.has_value()) << file.error().message();

  int raw = -1;
  {
    auto fd = file->DupFd();
    ASSERT_TRUE(fd.has_value()) << fd.error().message();
    raw = fd->Get();
    ASSERT_NE(::fcntl(raw, F_GETFD), -1) << "descriptor should be open while owned";
  }

  EXPECT_EQ(::fcntl(raw, F_GETFD), -1) << "descriptor should be closed once the owner is gone";
}

TEST_F(DownloadTempFileTest, ReleasingAnOwnedDescriptorLeavesItOpen) {
  auto file = DownloadTempFile::Create(test_dir_);
  ASSERT_TRUE(file.has_value()) << file.error().message();

  int raw = -1;
  {
    auto fd = file->DupFd();
    ASSERT_TRUE(fd.has_value()) << fd.error().message();
    raw = fd->Release();
  }
  auto const close_raw = memgraph::utils::OnScopeExit{[&] { ::close(raw); }};

  EXPECT_NE(::fcntl(raw, F_GETFD), -1) << "a released descriptor belongs to the caller";
}

TEST_F(DownloadTempFileTest, AnUnwritableDirectoryIsReportedAsAnErrorCode) {
  if (::geteuid() == 0) {
    GTEST_SKIP() << "root bypasses the directory permissions this test relies on";
  }

  auto const unwritable = test_dir_ / "unwritable";
  fs::create_directories(unwritable);
  fs::permissions(unwritable, fs::perms::owner_read | fs::perms::owner_exec);
  auto const restore = memgraph::utils::OnScopeExit{[&] { fs::permissions(unwritable, fs::perms::owner_all); }};

  auto file = DownloadTempFile::Create(unwritable);

  ASSERT_FALSE(file.has_value());
  EXPECT_EQ(file.error(), std::errc::permission_denied);
}
