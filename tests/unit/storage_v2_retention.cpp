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

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <optional>
#include <ranges>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <unistd.h>

#include "gtest/gtest.h"

#include "storage/v2/durability/snapshot.hpp"

using memgraph::storage::durability::DeleteOldSnapshotFiles;
using memgraph::storage::durability::OldSnapshotFiles;
using memgraph::utils::FileRetainer;

TEST(RetentionTest, DeleteOldSnapshotFilesTest) {
  FileRetainer file_retainer;
  OldSnapshotFiles old_snapshot_files{{{10, std::filesystem::path{"/tmp/file1"}},
                                       {5, std::filesystem::path{"/tmp/file2"}},
                                       {7, std::filesystem::path{"/tmp/file3"}},
                                       {2, std::filesystem::path{"/tmp/file4"}},
                                       {6, std::filesystem::path{"/tmp/file5"}}}};

  for (auto const &path : std::views::values(old_snapshot_files)) {
    std::ofstream{path};
  }

  DeleteOldSnapshotFiles(old_snapshot_files, 3, &file_retainer);
  ASSERT_EQ(old_snapshot_files.size(), 2);
  ASSERT_EQ(old_snapshot_files[0].first, 7);
  ASSERT_EQ(old_snapshot_files[1].first, 10);

  for (auto const &path : std::views::values(old_snapshot_files)) {
    if (std::filesystem::exists(path)) {
      file_retainer.DeleteFile(path);
    }
  }
}

namespace {

// Verbatim copy of origin/master's DeleteOldSnapshotFiles (no retention floor); the behavioural oracle.
void MasterDeleteOldSnapshotFiles(OldSnapshotFiles &old_snapshot_files, uint64_t const snapshot_retention_count,
                                  FileRetainer *file_retainer) {
  std::ranges::sort(old_snapshot_files);

  if (old_snapshot_files.size() < snapshot_retention_count) return;

  // -1 because the current snapshot path has been skipped
  const uint32_t num_to_erase = old_snapshot_files.size() - (snapshot_retention_count - 1);
  for (size_t i = 0; i < num_to_erase; ++i) {
    const auto &[_, snapshot_path] = old_snapshot_files[i];
    file_retainer->DeleteFile(snapshot_path);
  }

  old_snapshot_files.erase(old_snapshot_files.begin(), old_snapshot_files.begin() + num_to_erase);
}

using Outcome = std::pair<std::vector<std::pair<uint64_t, std::string>>, std::set<std::string>>;

enum class TimestampShape : uint8_t { kAscending, kDescendingByPath, kEqualPair, kZeroEntries };

auto Timestamps(TimestampShape const shape, size_t const count) -> std::vector<uint64_t> {
  std::vector<uint64_t> ts(count);
  for (size_t i = 0; i < count; ++i) {
    switch (shape) {
      case TimestampShape::kAscending:
        ts[i] = 10 * (i + 1);
        break;
      case TimestampShape::kDescendingByPath:
        ts[i] = 10 * (count - i);
        break;
      case TimestampShape::kEqualPair:
        ts[i] = i < 2 ? 100 : 10 * (i + 1) + 100;
        break;
      case TimestampShape::kZeroEntries:
        ts[i] = (i == 0 || i == count / 2 || i + 1 == count) ? 0 : 10 * (i + 1);
        break;
    }
  }
  return ts;
}

class RetentionNoFloorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    root = std::filesystem::temp_directory_path() / ("MG_test_retention_nofloor_" + std::to_string(::getpid()));
    std::filesystem::remove_all(root);
  }

  void TearDown() override { std::filesystem::remove_all(root); }

  // Fresh directory holding one empty file per timestamp, run through `fn`; returns what `fn` handed back plus what
  // is left on disk.
  template <typename Fn>
  auto Run(std::vector<uint64_t> const &timestamps, Fn &&fn) -> Outcome {
    auto const dir = root / std::to_string(run_counter++);
    std::filesystem::create_directories(dir);
    OldSnapshotFiles files;
    for (size_t i = 0; i < timestamps.size(); ++i) {
      auto path = dir / ("snapshot_" + std::string(i < 10 ? "0" : "") + std::to_string(i));
      std::ofstream{path};
      files.emplace_back(timestamps[i], std::move(path));
    }
    FileRetainer file_retainer;
    fn(files, &file_retainer);

    Outcome outcome;
    for (auto const &[ts, path] : files) outcome.first.emplace_back(ts, path.filename().string());
    for (auto const &entry : std::filesystem::directory_iterator(dir)) {
      outcome.second.insert(entry.path().filename().string());
    }
    return outcome;
  }

  std::filesystem::path root;
  size_t run_counter{0};
};

}  // namespace

// Without a retention floor DeleteOldSnapshotFiles must behave exactly like origin/master's.
TEST_F(RetentionNoFloorTest, DeleteOldSnapshotFiles_NoFloor_MatchesMasterOracle) {
  for (uint64_t const n : {1U, 2U, 3U, 5U}) {
    std::set<size_t> const counts{0, 1, n - 1, n, n + 1, 3 * n, 10};
    for (size_t const s : counts) {
      for (auto const shape : {TimestampShape::kAscending,
                               TimestampShape::kDescendingByPath,
                               TimestampShape::kEqualPair,
                               TimestampShape::kZeroEntries}) {
        SCOPED_TRACE("N=" + std::to_string(n) + " S=" + std::to_string(s) +
                     " shape=" + std::to_string(static_cast<int>(shape)));
        auto const timestamps = Timestamps(shape, s);

        auto const master = Run(
            timestamps, [&](OldSnapshotFiles &files, FileRetainer *fr) { MasterDeleteOldSnapshotFiles(files, n, fr); });
        auto const defaulted =
            Run(timestamps, [&](OldSnapshotFiles &files, FileRetainer *fr) { DeleteOldSnapshotFiles(files, n, fr); });

        EXPECT_EQ(defaulted.first, master.first);
        EXPECT_EQ(defaulted.second, master.second);

#ifdef MG_ENTERPRISE
        auto const explicit_nullopt = Run(timestamps, [&](OldSnapshotFiles &files, FileRetainer *fr) {
          DeleteOldSnapshotFiles(files, n, fr, std::nullopt);
        });
        EXPECT_EQ(explicit_nullopt.first, master.first);
        EXPECT_EQ(explicit_nullopt.second, master.second);
#endif

        // Independent of the oracle: the oldest S-(N-1) entries (ties broken by path) go, the rest stay in order.
        std::vector<std::pair<uint64_t, std::string>> expected;
        for (size_t i = 0; i < s; ++i) {
          expected.emplace_back(timestamps[i], std::string("snapshot_") + (i < 10 ? "0" : "") + std::to_string(i));
        }
        std::ranges::sort(expected);
        if (s >= n) expected.erase(expected.begin(), expected.begin() + (s - (n - 1)));
        EXPECT_EQ(defaulted.first, expected);

        std::set<std::string> expected_on_disk;
        for (auto const &[ts, name] : expected) expected_on_disk.insert(name);
        EXPECT_EQ(defaulted.second, expected_on_disk);
      }
    }
  }
}

// The WAL cleanup gate in CreateSnapshot is `remaining >= N-1`, master's was `remaining == N-1`. They agree because
// the retention pass never leaves more than N-1 old snapshots.
TEST_F(RetentionNoFloorTest, WalCleanupGate_RemainingNeverExceedsRetentionMinusOne) {
  for (uint64_t const n : {1U, 3U}) {
    for (size_t s = 0; s <= 12; ++s) {
      SCOPED_TRACE("N=" + std::to_string(n) + " S=" + std::to_string(s));
      auto const outcome =
          Run(Timestamps(TimestampShape::kAscending, s), [&](OldSnapshotFiles &files, FileRetainer *fr) {
            DeleteOldSnapshotFiles(files, n, fr);
            EXPECT_LE(files.size(), n - 1);
            EXPECT_EQ(files.size(), std::min<size_t>(s, n - 1));
          });
      EXPECT_LE(outcome.first.size(), n - 1);
    }
  }
}
