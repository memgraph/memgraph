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

#include <gtest/gtest.h>

#include <unistd.h>

#include <fmt/format.h>
#include <algorithm>
#include <chrono>
#include <expected>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <string>
#include <system_error>
#include <vector>

#include "dbms/database.hpp"
#include "flags/general.hpp"
#include "memory/db_arena.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/crc_accumulator.hpp"
#include "utils/scheduler.hpp"
#include "utils/uuid.hpp"
#include "versioning/branch_engine.hpp"
#include "versioning/branch_ledger.hpp"
#include "versioning/version_store.hpp"

namespace fs = std::filesystem;
using memgraph::versioning::BranchLedger;
using memgraph::versioning::LedgerContents;
using memgraph::versioning::VersionStore;

namespace {
std::string Slurp(const fs::path &p) {
  std::ifstream in(p, std::ios::binary);
  return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

void Spit(const fs::path &p, const std::string &data) {
  std::ofstream out(p, std::ios::binary | std::ios::trunc);
  out << data;
}
}  // namespace

class BranchLedgerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ = fs::temp_directory_path() / ("mg_branch_ledger_" + memgraph::utils::GenerateUUID());
    file_ = dir_ / "branches" / "ledger";
  }

  void TearDown() override {
    std::error_code ec;
    fs::remove_all(dir_, ec);
  }

  std::unique_ptr<VersionStore> MakeStore() {
    auto store = std::make_unique<VersionStore>(
        [this] {
          const auto ts = next_ts_++;
          live_.push_back(ts);
          return ts;
        },
        [this](uint64_t ts) { std::erase(live_, ts); });
    store->AttachLedger(file_);
    return store;
  }

  fs::path dir_;
  fs::path file_;
  uint64_t next_ts_{100};
  std::vector<uint64_t> live_;
};

TEST_F(BranchLedgerTest, MissingFileReadsAsNullopt) {
  BranchLedger ledger{file_};
  auto res = ledger.Read();
  ASSERT_TRUE(res.has_value());
  EXPECT_FALSE(res->has_value());
}

TEST_F(BranchLedgerTest, RoundTrip) {
  LedgerContents in;
  in.next_number = 5;
  in.branches.push_back({"a", {.number = 2, .parent = "main", .fork_ts = 10}});
  in.branches[0].info.vertex_gid_floor = 11;
  in.branches[0].info.edge_gid_floor = 13;
  in.branches[0].info.created_at_us = 1'700'000'000'123'456;
  in.branches.push_back({"b", {.number = 4, .parent = "a", .fork_ts = 20}});

  BranchLedger ledger{file_};
  ASSERT_TRUE(ledger.Write(in).has_value());
  auto res = ledger.Read();
  ASSERT_TRUE(res.has_value());
  ASSERT_TRUE(res->has_value());
  const auto &out = **res;
  EXPECT_EQ(out.next_number, 5);
  ASSERT_EQ(out.branches.size(), 2);
  EXPECT_EQ(out.branches[0].name, "a");
  EXPECT_EQ(out.branches[0].info.vertex_gid_floor, 11);
  EXPECT_EQ(out.branches[0].info.edge_gid_floor, 13);
  EXPECT_EQ(out.branches[0].info.created_at_us, 1'700'000'000'123'456);
  EXPECT_EQ(out.branches[1].info.created_at_us, std::nullopt);
  EXPECT_EQ(out.branches[1].info.parent, "a");
  EXPECT_EQ(out.branches[1].info.vertex_gid_floor, std::nullopt);
}

TEST_F(BranchLedgerTest, ReadsALedgerWrittenWithADescription) {
  const std::string payload =
      R"({"next_number":3,"branches":[{"name":"a","number":2,"parent":"main","fork_ts":10,"description":"old"}]})";
  memgraph::utils::CrcAccumulator crc;
  crc.Update(reinterpret_cast<const unsigned char *>(payload.data()), static_cast<uint32_t>(payload.size()));
  fs::create_directories(file_.parent_path());
  Spit(file_, fmt::format("MGBL 1 {} {:08x}\n{}", payload.size(), crc.Value(), payload));

  auto res = BranchLedger{file_}.Read();
  ASSERT_TRUE(res.has_value() && res->has_value());
  ASSERT_EQ((*res)->branches.size(), 1);
  EXPECT_EQ((*res)->branches[0].name, "a");
  EXPECT_EQ((*res)->branches[0].info.fork_ts, 10);
}

TEST_F(BranchLedgerTest, DetectsTruncationAndCorruption) {
  LedgerContents in;
  in.next_number = 3;
  in.branches.push_back({"a", {.number = 2, .parent = "main", .fork_ts = 10}});
  BranchLedger ledger{file_};
  ASSERT_TRUE(ledger.Write(in).has_value());
  const auto good = Slurp(file_);

  Spit(file_, good.substr(0, good.size() - 3));
  EXPECT_FALSE(ledger.Read().has_value()) << "truncated payload";

  Spit(file_, good.substr(0, good.find('\n')));
  EXPECT_FALSE(ledger.Read().has_value()) << "header only";

  Spit(file_, "");
  EXPECT_FALSE(ledger.Read().has_value()) << "empty file";

  auto flipped = good;
  flipped[flipped.size() - 5] ^= 0x01;
  Spit(file_, flipped);
  EXPECT_FALSE(ledger.Read().has_value()) << "bit flip";

  Spit(file_, "XXXX" + good.substr(4));
  EXPECT_FALSE(ledger.Read().has_value()) << "bad magic";
}

TEST_F(BranchLedgerTest, WriteReplacesAtomicallyAndLeavesNoTempFile) {
  BranchLedger ledger{file_};
  LedgerContents v1;
  v1.next_number = 3;
  v1.branches.push_back({"a", {.number = 2, .parent = "main", .fork_ts = 1}});
  ASSERT_TRUE(ledger.Write(v1).has_value());

  LedgerContents v2;
  v2.next_number = 4;
  ASSERT_TRUE(ledger.Write(v2).has_value());

  auto res = ledger.Read();
  ASSERT_TRUE(res.has_value() && res->has_value());
  EXPECT_TRUE((*res)->branches.empty());
  EXPECT_EQ((*res)->next_number, 4);

  size_t files = 0;
  for (const auto &e : fs::directory_iterator(file_.parent_path())) {
    ++files;
    EXPECT_EQ(e.path(), file_);
  }
  EXPECT_EQ(files, 1);
}

TEST_F(BranchLedgerTest, FailedWriteKeepsPreviousLedger) {
  BranchLedger ledger{file_};
  LedgerContents v1;
  v1.next_number = 3;
  v1.branches.push_back({"a", {.number = 2, .parent = "main", .fork_ts = 1}});
  ASSERT_TRUE(ledger.Write(v1).has_value());

  // A directory squatting on the temp path makes the next write fail before the rename.
  auto tmp = file_;
  tmp += ".tmp";
  fs::create_directory(tmp);
  LedgerContents v2;
  v2.next_number = 9;
  EXPECT_FALSE(ledger.Write(v2).has_value());

  auto res = ledger.Read();
  ASSERT_TRUE(res.has_value() && res->has_value());
  EXPECT_EQ((*res)->next_number, 3);
}

TEST_F(BranchLedgerTest, StoreCreateAndDropUpdateLedger) {
  auto store = MakeStore();
  BranchLedger reader{file_};

  auto a = store->CreateBranch("a", "main");
  ASSERT_TRUE(a.has_value());
  auto b = store->CreateBranch("b", "a");
  ASSERT_TRUE(b.has_value());

  auto loaded = reader.Read();
  ASSERT_TRUE(loaded.has_value() && loaded->has_value());
  ASSERT_EQ((*loaded)->branches.size(), 2);
  EXPECT_EQ((*loaded)->next_number, 4);
  EXPECT_EQ((*loaded)->branches[0].name, "a");
  EXPECT_EQ((*loaded)->branches[0].info.fork_ts, a->fork_ts);
  EXPECT_EQ((*loaded)->branches[1].info.parent, "a");
  ASSERT_TRUE(a->created_at_us.has_value());
  EXPECT_GT(*a->created_at_us, 0U);
  EXPECT_EQ((*loaded)->branches[0].info.created_at_us, a->created_at_us);

  ASSERT_TRUE(store->RecordGidFloors(a->number, 7, 9).has_value());
  EXPECT_TRUE(store->RecordGidFloors(a->number, 99, 99).has_value()) << "second call is a no-op";
  EXPECT_EQ(store->Get("a")->vertex_gid_floor, 7);
  loaded = reader.Read();
  ASSERT_TRUE(loaded.has_value() && loaded->has_value());
  EXPECT_EQ((*loaded)->branches[0].info.vertex_gid_floor, 7);
  EXPECT_EQ((*loaded)->branches[0].info.edge_gid_floor, 9);

  auto dropped = store->DropBranch("a");
  ASSERT_TRUE(dropped.has_value());
  EXPECT_TRUE(*dropped);
  loaded = reader.Read();
  ASSERT_TRUE(loaded.has_value() && loaded->has_value());
  ASSERT_EQ((*loaded)->branches.size(), 1);
  EXPECT_EQ((*loaded)->branches[0].name, "b");
  EXPECT_EQ((*loaded)->next_number, 4) << "numbers are never reused";
}

TEST_F(BranchLedgerTest, LedgerWriteFailureLeavesNoPinOrBranch) {
  auto store = MakeStore();
  ASSERT_TRUE(store->CreateBranch("a", "main").has_value());
  const auto live_before = live_.size();

  auto tmp = file_;
  tmp += ".tmp";
  fs::create_directory(tmp);

  EXPECT_FALSE(store->CreateBranch("b", "main").has_value());
  EXPECT_FALSE(store->Exists("b"));
  EXPECT_EQ(live_.size(), live_before);

  auto dropped = store->DropBranch("a");
  EXPECT_FALSE(dropped.has_value());
  EXPECT_TRUE(store->Exists("a"));
  EXPECT_EQ(live_.size(), live_before);
  EXPECT_TRUE(store->TryAcquireCheckout("a").has_value()) << "a failed drop must not leave the branch unattachable";

  fs::remove(tmp);
  auto b = store->CreateBranch("b", "main");
  ASSERT_TRUE(b.has_value());
  EXPECT_EQ(b->number, 3) << "a failed create must not consume a number";
}

TEST_F(BranchLedgerTest, ExistsErrorIsNotReadAsNoLedger) {
  if (::geteuid() == 0) GTEST_SKIP() << "permission checks do not apply to root";
  fs::create_directories(file_.parent_path());
  BranchLedger writer{file_};
  LedgerContents contents;
  contents.next_number = 5;
  ASSERT_TRUE(writer.Write(contents).has_value());

  const auto branches_dir = file_.parent_path();
  fs::permissions(branches_dir, fs::perms::none, fs::perm_options::replace);
  auto res = BranchLedger{file_}.Read();
  fs::permissions(branches_dir, fs::perms::owner_all, fs::perm_options::replace);

  EXPECT_FALSE(res.has_value() && !res->has_value())
      << "an unreadable ledger location must be an error, not 'no ledger' (the next write would clobber the file)";
}

namespace {
namespace ms = memgraph::storage;
namespace mv = memgraph::versioning;

const fs::path kRestartDirectory{fs::temp_directory_path() / "versioning_ledger_restart"};

ms::Config MakeRestartConfig(bool recover_on_startup) {
  return ms::Config{
      .durability = {.storage_directory = kRestartDirectory,
                     .recover_on_startup = recover_on_startup,
                     .snapshot_wal_mode = ms::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                     .snapshot_interval = memgraph::utils::SchedulerInterval{std::chrono::hours(24)}},
  };
}

auto *Mem(memgraph::dbms::Database &db) { return static_cast<ms::InMemoryStorage *>(db.storage()); }

std::unique_ptr<mv::BranchContext> CheckoutBranch(memgraph::dbms::Database &db, std::string_view name) {
  auto *store = db.version_store();
  auto info = store->Get(name);
  EXPECT_TRUE(info.has_value());
  if (!info) return nullptr;
  auto core = store->GetOrBuildCore(info->number, [&]() -> std::expected<std::shared_ptr<mv::BranchCore>, std::string> {
    auto fresh = mv::BranchCore::Build(*Mem(db), info->fork_ts, info->number);
    if (!fresh) return std::unexpected(fresh.error().message);
    return std::move(*fresh);
  });
  EXPECT_TRUE(core.has_value());
  if (!core) return nullptr;
  EXPECT_TRUE(store->RecordGidFloors(info->number, (*core)->vertex_gid_floor(), (*core)->edge_gid_floor()).has_value());
  return std::make_unique<mv::BranchContext>(*core, std::string{name});
}

std::vector<uint64_t> BranchDirectoryNumbers() {
  std::vector<uint64_t> numbers;
  for (const auto &entry : fs::directory_iterator(kRestartDirectory / "branches")) {
    if (!entry.is_directory()) continue;
    numbers.push_back(std::stoull(entry.path().filename().string()));
  }
  return numbers;
}
}  // namespace

class BranchLedgerRestartTest : public ::testing::Test {
 protected:
  void SetUp() override { FLAGS_versioning_enabled = true; }

  void TearDown() override { FLAGS_versioning_enabled = false; }
};

TEST_F(BranchLedgerRestartTest, AnUnreadableLedgerNeverReusesABranchDirectory) {
  fs::remove_all(kRestartDirectory);
  const auto cleanup = [] {
    std::error_code ec;
    fs::remove_all(kRestartDirectory, ec);
  };

  uint64_t b_number = 0;
  {
    memgraph::dbms::Database db{MakeRestartConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    {
      auto acc = db.Access(ms::WRITE);
      acc->CreateVertex();
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    auto info = db.version_store()->CreateBranch("b", "main");
    ASSERT_TRUE(info.has_value());
    b_number = info->number;
    auto ctx = CheckoutBranch(db, "b");
    ASSERT_NE(ctx, nullptr);
    auto diff_acc = ctx->diff_engine().Access(ms::WRITE);
    ASSERT_TRUE(ctx->BeginTransaction(diff_acc.get()).has_value());
    ctx->CreateNativeVertex();
    ASSERT_TRUE(ctx->CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
    ctx->EndTransaction();
  }

  const auto wal_dir = kRestartDirectory / "branches" / std::to_string(b_number) / "wal";
  ASSERT_TRUE(fs::exists(wal_dir));
  ASSERT_TRUE(fs::directory_iterator(wal_dir) != fs::directory_iterator{}) << "the branch must have a WAL file";

  const auto ledger_file = kRestartDirectory / "branches" / "ledger";
  auto data = Slurp(ledger_file);
  ASSERT_FALSE(data.empty());
  data.back() = static_cast<char>(data.back() ^ 0x5a);
  Spit(ledger_file, data);
  ASSERT_FALSE(BranchLedger{ledger_file}.Read().has_value()) << "the ledger must be unreadable";

  uint64_t c_number = 0;
  {
    memgraph::dbms::Database db{MakeRestartConfig(/*recover_on_startup=*/true)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    ASSERT_FALSE(db.version_store()->Exists("b")) << "an unreadable ledger drops every branch";
    auto info = db.version_store()->CreateBranch("c", "main");
    ASSERT_TRUE(info.has_value());
    c_number = info->number;
    const auto numbers = BranchDirectoryNumbers();
    for (const auto n : numbers) {
      if (n == c_number) continue;
      EXPECT_LT(n, c_number) << "branch number " << c_number << " must be greater than every existing directory";
    }
    EXPECT_NE(c_number, b_number) << "the new branch reuses a stale branches/<n> directory";
  }

  {
    memgraph::dbms::Database db{MakeRestartConfig(/*recover_on_startup=*/true)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    EXPECT_TRUE(db.version_store()->Exists("c")) << "a new, legitimately empty branch must survive a restart";
  }
  cleanup();
}
