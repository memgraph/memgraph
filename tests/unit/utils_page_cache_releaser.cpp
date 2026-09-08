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
#include <cstdint>
#include <filesystem>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "page_cache_probe.hpp"
#include "utils/file.hpp"
#include "utils/page_cache_releaser.hpp"

namespace {

constexpr size_t kFileBytes = 8U << 20U;

class PageCacheReleaserTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::filesystem::create_directories(dir_);
    observable_ = memgraph::test::PageCacheEvictionObservable(dir_ / "probe");
  }

  void TearDown() override {
    std::error_code error_code;
    std::filesystem::remove_all(dir_, error_code);
  }

  // A file whose pages are resident and clean, which is the only state they can be evicted in.
  std::filesystem::path WriteResidentFile(std::string_view name) const {
    auto const path = dir_ / name;
    std::vector<uint8_t> const data(kFileBytes, 0xCD);
    memgraph::utils::NonConcurrentOutputFile file;
    file.Open(path, memgraph::utils::NonConcurrentOutputFile::Mode::OVERWRITE_EXISTING);
    file.Write(data.data(), data.size());
    file.Sync();
    file.Close();
    return path;
  }

  std::filesystem::path dir_{std::filesystem::temp_directory_path() / "MG_test_utils_page_cache_releaser"};
  bool observable_{false};
};

TEST_F(PageCacheReleaserTest, NoReleaserIsInstalledByDefault) {
  EXPECT_EQ(memgraph::utils::PageCacheReleaserHandle().lock(), nullptr);
}

TEST_F(PageCacheReleaserTest, TheHandleExpiresWhenItsOwnerLetsGo) {
  // Nothing keeps the releaser, and so its thread, alive beyond the handle its owner holds. This is
  // what lets an owner decide when the thread is joined.
  {
    auto const owner = memgraph::utils::InstallPageCacheReleaser();
    ASSERT_NE(memgraph::utils::PageCacheReleaserHandle().lock(), nullptr);
  }
  EXPECT_EQ(memgraph::utils::PageCacheReleaserHandle().lock(), nullptr);
}

TEST_F(PageCacheReleaserTest, AnInstalledReleaserEvictsTheFileItIsGiven) {
  if (!observable_) GTEST_SKIP() << "filesystem does not honour POSIX_FADV_DONTNEED";

  auto const path = WriteResidentFile("handed_over");
  memgraph::utils::InputFile file;
  ASSERT_TRUE(file.Open(path));
  ASSERT_GT(*memgraph::test::ResidentFraction(path), 0.9);

  auto const owner = memgraph::utils::InstallPageCacheReleaser();
  owner->Drop(std::move(file));

  // The drop is asynchronous by design, so the test waits for the effect rather than for the task:
  // waiting on the task would assert the mechanism, and residency is what the releaser exists for.
  auto const deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (std::chrono::steady_clock::now() < deadline) {
    if (*memgraph::test::ResidentFraction(path) < 0.05) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  EXPECT_LT(*memgraph::test::ResidentFraction(path), 0.05);
}

TEST_F(PageCacheReleaserTest, DroppingWithoutAReleaserStillEvicts) {
  if (!observable_) GTEST_SKIP() << "filesystem does not honour POSIX_FADV_DONTNEED";

  // The synchronous fallback a caller takes when it cannot lock the handle. Same outcome, and it
  // has already happened by the time the call returns.
  auto const path = WriteResidentFile("synchronous");
  memgraph::utils::InputFile file;
  ASSERT_TRUE(file.Open(path));
  ASSERT_GT(*memgraph::test::ResidentFraction(path), 0.9);

  ASSERT_EQ(memgraph::utils::PageCacheReleaserHandle().lock(), nullptr);
  file.DropCachedPages();
  EXPECT_LT(*memgraph::test::ResidentFraction(path), 0.05);
}

}  // namespace
