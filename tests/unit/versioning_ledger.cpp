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
#include <chrono>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <tuple>
#include <vector>

#include "dbms/database.hpp"
#include "memory/db_arena.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "tests/versioning_startup_refusal.hpp"
#include "utils/crc_accumulator.hpp"
#include "utils/scheduler.hpp"
#include "utils/uuid.hpp"
#include "versioning/branch_info.hpp"
#include "versioning/branch_ledger.hpp"
#include "versioning/version_store.hpp"

namespace fs = std::filesystem;
using memgraph::versioning::BranchLedger;
using memgraph::versioning::DropResult;
using memgraph::versioning::ForkPoint;
using memgraph::versioning::GidKind;
using memgraph::versioning::GidsHeldBelow;
using memgraph::versioning::LedgerContents;
using memgraph::versioning::ObjectCounts;
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

std::string Framed(const std::string &payload, uint32_t version = BranchLedger::kVersion) {
  memgraph::utils::CrcAccumulator crc;
  crc.Update(reinterpret_cast<const unsigned char *>(payload.data()), static_cast<uint32_t>(payload.size()));
  return fmt::format("MGBL {} {} {:08x}\n{}", version, payload.size(), crc.Value(), payload);
}

std::string OneBranch(const std::string &branch, uint64_t next_number = 3) {
  return fmt::format(R"({{"next_number":{},"branches":[{}]}})", next_number, branch);
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
          return ForkPoint{.fork_ts = ts, .counts = {.vertices = ts + 1, .edges = ts + 2}};
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
  in.branches[0].info.vertex_gid_ceiling = 70'011;
  in.branches[0].info.edge_gid_ceiling = 70'013;
  in.branches[0].info.created_at_us = 1'700'000'000'123'456;
  in.branches[0].info.fork_counts = ObjectCounts{.vertices = 17, .edges = 19};
  // The persisted fork_ts is the durable recorded one, not the runtime pin.
  in.branches.push_back({"b", {.number = 4, .parent = "a", .fork_ts = 100, .recorded_fork_ts = 20}});

  BranchLedger ledger{file_};
  ASSERT_TRUE(ledger.Write(in).has_value());
  auto res = ledger.Read();
  ASSERT_TRUE(res.has_value());
  ASSERT_TRUE(res->has_value());
  const auto &out = **res;
  EXPECT_EQ(out.next_number, 5);
  ASSERT_EQ(out.branches.size(), 2);
  EXPECT_EQ(out.branches[0].name, "a");
  EXPECT_EQ(out.branches[0].info.number, 2);
  EXPECT_EQ(out.branches[0].info.parent, "main");
  EXPECT_EQ(out.branches[0].info.fork_ts, 10);
  EXPECT_EQ(out.branches[0].info.recorded_fork_ts, 10);
  EXPECT_EQ(out.branches[1].info.fork_ts, 20);
  EXPECT_EQ(out.branches[1].info.recorded_fork_ts, 20);
  EXPECT_EQ(out.branches[0].info.vertex_gid_floor, 11);
  EXPECT_EQ(out.branches[0].info.edge_gid_floor, 13);
  EXPECT_EQ(out.branches[0].info.vertex_gid_ceiling, 70'011);
  EXPECT_EQ(out.branches[0].info.edge_gid_ceiling, 70'013);
  EXPECT_EQ(out.branches[0].info.created_at_us, 1'700'000'000'123'456);
  EXPECT_EQ(out.branches[0].info.fork_counts, (ObjectCounts{.vertices = 17, .edges = 19}));
  EXPECT_EQ(out.branches[1].info.created_at_us, std::nullopt);
  EXPECT_EQ(out.branches[1].info.fork_counts, std::nullopt);
  EXPECT_EQ(out.branches[1].info.parent, "a");
  EXPECT_EQ(out.branches[1].info.vertex_gid_ceiling, std::nullopt);
  EXPECT_EQ(out.branches[1].info.edge_gid_ceiling, std::nullopt);
  EXPECT_EQ(out.branches[1].info.vertex_gid_floor, std::nullopt);
  EXPECT_EQ(GidsHeldBelow(out.branches[0].info, GidKind::kVertex), 70'011);
  EXPECT_EQ(GidsHeldBelow(out.branches[0].info, GidKind::kEdge), 70'013);
  EXPECT_EQ(GidsHeldBelow(out.branches[1].info, GidKind::kVertex), 0);
  EXPECT_EQ(GidsHeldBelow(out.branches[1].info, GidKind::kEdge), 0);
}

// The floor stands in for the missing ceilings.
TEST_F(BranchLedgerTest, AnOldFormatLedgerReadsWithNoCeilingsAndIgnoresUnknownKeys) {
  fs::create_directories(file_.parent_path());
  Spit(
      file_,
      Framed(OneBranch(
          R"({"name":"a","number":2,"parent":"main","fork_ts":10,"vertex_gid_floor":7,"edge_gid_floor":9,"description":"old"})")));
  const auto res = BranchLedger{file_}.Read();
  ASSERT_TRUE(res.has_value() && res->has_value()) << (res ? "" : res.error());
  ASSERT_EQ((*res)->branches.size(), 1);
  EXPECT_EQ((*res)->branches[0].name, "a");
  const auto &info = (*res)->branches[0].info;
  EXPECT_EQ(info.fork_ts, 10);
  EXPECT_EQ(info.vertex_gid_ceiling, std::nullopt);
  EXPECT_EQ(info.edge_gid_ceiling, std::nullopt);
  EXPECT_EQ(GidsHeldBelow(info, GidKind::kVertex), 7);
  EXPECT_EQ(GidsHeldBelow(info, GidKind::kEdge), 9);
}

TEST_F(BranchLedgerTest, DetectsTruncationAndCorruption) {
  LedgerContents in;
  in.next_number = 3;
  in.branches.push_back({"a", {.number = 2, .parent = "main", .fork_ts = 10}});
  BranchLedger ledger{file_};
  ASSERT_TRUE(ledger.Write(in).has_value());
  const auto good = Slurp(file_);

  const auto expect_error = [&](std::string_view what, std::string_view text) {
    const auto res = ledger.Read();
    ASSERT_FALSE(res.has_value()) << what;
    EXPECT_NE(res.error().find(text), std::string::npos) << what << ": " << res.error();
  };

  Spit(file_, good.substr(0, good.size() - 3));
  expect_error("truncated payload", "does not match header size");

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

  // Same length and still valid JSON, header untouched: only the checksum can catch it.
  auto edited = good;
  const auto pos = edited.find(R"("fork_ts":10)");
  ASSERT_NE(pos, std::string::npos);
  edited.replace(pos, std::string_view{R"("fork_ts":10)"}.size(), R"("fork_ts":11)");
  Spit(file_, edited);
  expect_error("same-length edit", "checksum mismatch");

  // Correct checksum for the payload, but the header size is off by one.
  const auto payload = good.substr(good.find('\n') + 1);
  memgraph::utils::CrcAccumulator crc;
  crc.Update(reinterpret_cast<const unsigned char *>(payload.data()), static_cast<uint32_t>(payload.size()));
  Spit(file_, fmt::format("MGBL {} {} {:08x}\n{}", BranchLedger::kVersion, payload.size() + 1, crc.Value(), payload));
  expect_error("wrong header size, valid crc", "does not match header size");
}

// A directory where the ledger file belongs is damage like any other: Read reports it, naming the path and no stream
// internals, instead of throwing.
TEST_F(BranchLedgerTest, ADirectoryInPlaceOfTheLedgerIsAnErrorNotAnException) {
  fs::create_directories(file_);

  std::expected<std::optional<LedgerContents>, std::string> res;
  ASSERT_NO_THROW(res = BranchLedger{file_}.Read());
  ASSERT_FALSE(res.has_value());
  EXPECT_NE(res.error().find(file_.string()), std::string::npos) << res.error();
  EXPECT_NE(res.error().find("not a regular file"), std::string::npos) << res.error();
  EXPECT_EQ(res.error().find("basic_filebuf"), std::string::npos) << "no stream error text: " << res.error();
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

  // A non-empty directory at the ledger path lets the temp file be written but not renamed over it.
  const auto blocked = dir_ / "renamefail" / "ledger";
  fs::create_directories(blocked);
  Spit(blocked / "occupant", "x");
  const auto blocked_write = BranchLedger{blocked}.Write(v2);
  ASSERT_FALSE(blocked_write.has_value());
  EXPECT_NE(blocked_write.error().find("cannot rename into place"), std::string::npos) << blocked_write.error();
  auto blocked_tmp = blocked;
  blocked_tmp += ".tmp";
  EXPECT_FALSE(fs::exists(blocked_tmp));
}

TEST_F(BranchLedgerTest, RejectsHeadersWithGarbage) {
  const std::string payload = OneBranch(R"({"name":"a","number":2,"parent":"main","fork_ts":10})");
  const auto good = Framed(payload);
  fs::create_directories(file_.parent_path());
  Spit(file_, good);
  ASSERT_TRUE(BranchLedger{file_}.Read().has_value());

  const auto body = good.substr(good.find('\n'));
  const auto crc = good.substr(good.rfind(' ', good.find('\n')) + 1, 8);
  for (const auto &header : {fmt::format("MGBL 1 {}x {}", payload.size(), crc),
                             fmt::format("MGBL 1x {} {}", payload.size(), crc),
                             fmt::format("MGBL 1 {} {}zz", payload.size(), crc),
                             fmt::format("MGBL 1 {} {} extra", payload.size(), crc),
                             fmt::format("MGBL 1  {} {}", payload.size(), crc),
                             fmt::format("MGBL 1 {}", payload.size())}) {
    Spit(file_, header + body);
    EXPECT_FALSE(BranchLedger{file_}.Read().has_value()) << header;
  }
}

TEST_F(BranchLedgerTest, RejectsAnUnsupportedVersion) {
  fs::create_directories(file_.parent_path());
  for (const uint32_t version : {BranchLedger::kVersion + 1, uint32_t{0}}) {
    Spit(file_, Framed(OneBranch(R"({"name":"a","number":2,"parent":"main","fork_ts":10})"), version));
    const auto res = BranchLedger{file_}.Read();
    ASSERT_FALSE(res.has_value()) << version;
    EXPECT_NE(res.error().find(fmt::format("unsupported version {}", version)), std::string::npos) << res.error();
  }
}

TEST_F(BranchLedgerTest, RejectsAChecksummedButMalformedPayload) {
  fs::create_directories(file_.parent_path());
  for (const auto &payload : std::vector<std::string>{
           R"({"next_number":3)",
           R"({"next_number":3})",
           R"({"next_number":3,"branches":{}})",
           R"({"next_number":-3,"branches":[]})",
           OneBranch(R"({"name":"a","number":-2,"parent":"main","fork_ts":10})"),
           OneBranch(R"({"name":"a","number":2.5,"parent":"main","fork_ts":10})"),
           OneBranch(R"({"name":"a","number":"2","parent":"main","fork_ts":10})"),
           OneBranch(R"({"name":"a","number":2,"parent":"main","fork_ts":-1})"),
           OneBranch(R"({"name":"a","number":2,"parent":"main","fork_ts":10,"vertex_gid_floor":-1})"),
           OneBranch(R"({"name":"a","number":2,"parent":"main","fork_ts":10,"edge_gid_ceiling":-1})"),
           OneBranch(R"({"name":"a","number":2,"parent":"main","fork_ts":10,"created_at_us":1.5})"),
           OneBranch(R"({"name":"a","number":2,"fork_ts":10})"),
       }) {
    Spit(file_, Framed(payload));
    EXPECT_FALSE(BranchLedger{file_}.Read().has_value()) << payload;
  }
}

TEST_F(BranchLedgerTest, RejectsAChecksummedButInvalidRegistry) {
  fs::create_directories(file_.parent_path());
  const auto branch = [](std::string_view name, uint64_t number, std::string_view parent = "main") {
    return fmt::format(R"({{"name":"{}","number":{},"parent":"{}","fork_ts":10}})", name, number, parent);
  };
  for (const auto &payload : std::vector<std::string>{
           R"({"next_number":1,"branches":[]})",
           OneBranch(branch("a", 0)),
           OneBranch(branch("a", 1)),
           OneBranch(branch("a", 3)),
           OneBranch(
               R"({"name":"a","number":2,"parent":"main","fork_ts":10,"vertex_gid_floor":7,"vertex_gid_ceiling":6})"),
           OneBranch(R"({"name":"a","number":2,"parent":"main","fork_ts":10,"edge_gid_floor":9,"edge_gid_ceiling":8})"),
           OneBranch(branch("main", 2)),
           OneBranch(branch("a/b", 2)),
           OneBranch(branch("-a", 2)),
           OneBranch(branch("a", 2, "x@y")),
           OneBranch(branch("a", 2, "")),
           fmt::format(R"({{"next_number":5,"branches":[{},{}]}})", branch("a", 2), branch("a", 3)),
           fmt::format(R"({{"next_number":5,"branches":[{},{}]}})", branch("a", 2), branch("b", 2)),
       }) {
    Spit(file_, Framed(payload));
    EXPECT_FALSE(BranchLedger{file_}.Read().has_value()) << payload;
  }

  // A parent need only be a valid name, not a listed branch.
  Spit(file_, Framed(OneBranch(branch("a", 2, "gone"))));
  const auto res = BranchLedger{file_}.Read();
  ASSERT_TRUE(res.has_value() && res->has_value()) << (res ? "" : res.error());
  EXPECT_EQ((*res)->branches[0].info.parent, "gone");

  Spit(file_,
       Framed(OneBranch(
           R"({"name":"a","number":2,"parent":"main","fork_ts":10,"vertex_gid_floor":7,"vertex_gid_ceiling":7})")));
  const auto equal = BranchLedger{file_}.Read();
  ASSERT_TRUE(equal.has_value() && equal->has_value()) << (equal ? "" : equal.error());
  EXPECT_EQ((*equal)->branches[0].info.vertex_gid_ceiling, 7);
}

TEST_F(BranchLedgerTest, WriteRefusesWhatReadWouldReject) {
  BranchLedger ledger{file_};
  LedgerContents v1;
  v1.next_number = 3;
  v1.branches.push_back({"a", {.number = 2, .parent = "main", .fork_ts = 1}});
  ASSERT_TRUE(ledger.Write(v1).has_value());

  for (const auto &[name, number, parent] : {std::tuple{"\xff", 2, "main"},
                                             std::tuple{"a", 2, "\xff"},
                                             std::tuple{"main", 2, "main"},
                                             std::tuple{"a", 1, "main"},
                                             std::tuple{"a", 3, "main"}}) {
    LedgerContents bad;
    bad.next_number = 3;
    bad.branches.push_back({name, {.number = static_cast<uint64_t>(number), .parent = parent, .fork_ts = 1}});
    std::expected<void, std::string> res;
    ASSERT_NO_THROW(res = ledger.Write(bad));
    ASSERT_FALSE(res.has_value());
    EXPECT_NE(res.error().find("refusing to write an invalid registry"), std::string::npos) << res.error();
  }

  LedgerContents dup;
  dup.next_number = 4;
  dup.branches.push_back({"a", {.number = 2, .parent = "main", .fork_ts = 1}});
  dup.branches.push_back({"a", {.number = 3, .parent = "main", .fork_ts = 1}});
  const auto dup_res = ledger.Write(dup);
  ASSERT_FALSE(dup_res.has_value());
  EXPECT_NE(dup_res.error().find("refusing to write an invalid registry"), std::string::npos) << dup_res.error();

  const auto res = ledger.Read();
  ASSERT_TRUE(res.has_value() && res->has_value());
  EXPECT_EQ((*res)->branches.size(), 1);
}

// A crash before the rename leaves the old ledger readable; the next, shorter write must not inherit the longer
// stale temp file's tail (catches a missing O_TRUNC).
TEST_F(BranchLedgerTest, StaleTempFileLeavesTheOldLedgerAndDoesNotAffectTheNextWrite) {
  BranchLedger ledger{file_};
  LedgerContents v1;
  v1.next_number = 3;
  v1.branches.push_back({"a", {.number = 2, .parent = "main", .fork_ts = 5}});
  ASSERT_TRUE(ledger.Write(v1).has_value());

  const auto staged = dir_ / "staged" / "ledger";
  LedgerContents v2;
  v2.next_number = 4;
  v2.branches = v1.branches;
  v2.branches.push_back({"b", {.number = 3, .parent = "a", .fork_ts = 9}});
  ASSERT_TRUE(BranchLedger{staged}.Write(v2).has_value());
  auto tmp = file_;
  tmp += ".tmp";
  Spit(tmp, Slurp(staged));

  auto res = ledger.Read();
  ASSERT_TRUE(res.has_value() && res->has_value());
  EXPECT_EQ((*res)->next_number, 3);
  ASSERT_EQ((*res)->branches.size(), 1);
  EXPECT_EQ((*res)->branches[0].name, "a");

  LedgerContents v3;
  v3.next_number = 3;
  v3.branches.push_back({"a", {.number = 2, .parent = "main", .fork_ts = 7}});
  ASSERT_TRUE(ledger.Write(v3).has_value());
  EXPECT_FALSE(fs::exists(tmp));
  res = ledger.Read();
  ASSERT_TRUE(res.has_value() && res->has_value());
  ASSERT_EQ((*res)->branches.size(), 1);
  EXPECT_EQ((*res)->branches[0].info.fork_ts, 7);
}

class BranchLedgerPermissionTest : public BranchLedgerTest {
 protected:
  void SetUp() override {
    if (::geteuid() == 0) GTEST_SKIP() << "permission checks do not apply to root";
    BranchLedgerTest::SetUp();
    fs::create_directories(dir_);
  }

  void TearDown() override {
    std::error_code ec;
    for (const auto &d : {dir_, file_.parent_path()}) {
      fs::permissions(d, fs::perms::owner_all, ec);
    }
    BranchLedgerTest::TearDown();
  }

  // Write and search but no read: entries can be created inside, but the directory cannot be opened for fsync.
  static void MakeUnsyncable(const fs::path &d) { fs::permissions(d, fs::perms::owner_write | fs::perms::owner_exec); }

  LedgerContents Contents(uint64_t next_number) {
    LedgerContents c;
    c.next_number = next_number;
    return c;
  }
};

// The ledger directory's own entry is fsynced in its parent before anything is renamed into it.
TEST_F(BranchLedgerPermissionTest, WriteFailsWhenTheLedgerDirectoryEntryCannotBeSynced) {
  BranchLedger ledger{file_};
  ASSERT_TRUE(ledger.Write(Contents(3)).has_value());
  MakeUnsyncable(dir_);
  const auto res = ledger.Write(Contents(4));
  ASSERT_FALSE(res.has_value());
  EXPECT_NE(res.error().find("cannot open directory"), std::string::npos) << res.error();
  fs::permissions(dir_, fs::perms::owner_all);
  const auto read = ledger.Read();
  ASSERT_TRUE(read.has_value() && read->has_value());
  EXPECT_EQ((*read)->next_number, 3);
}

TEST_F(BranchLedgerPermissionTest, DirectoryFsyncFailureAfterRenameStillCountsAsWritten) {
  BranchLedger ledger{file_};
  ASSERT_TRUE(ledger.Write(Contents(3)).has_value());
  MakeUnsyncable(file_.parent_path());
  ASSERT_TRUE(ledger.Write(Contents(4)).has_value());
  const auto res = ledger.Read();
  ASSERT_TRUE(res.has_value() && res->has_value());
  EXPECT_EQ((*res)->next_number, 4);
}

TEST_F(BranchLedgerTest, StoreCreateAndDropUpdateLedger) {
  auto store = MakeStore();
  BranchLedger reader{file_};

  auto a = store->CreateBranch("a");
  ASSERT_TRUE(a.has_value());
  auto b = store->CreateBranch("b");
  ASSERT_TRUE(b.has_value());

  auto loaded = reader.Read();
  ASSERT_TRUE(loaded.has_value() && loaded->has_value());
  ASSERT_EQ((*loaded)->branches.size(), 2);
  EXPECT_EQ((*loaded)->next_number, 4);
  EXPECT_EQ((*loaded)->branches[0].name, "a");
  EXPECT_EQ((*loaded)->branches[0].info.fork_ts, a->fork_ts);
  EXPECT_EQ((*loaded)->branches[1].info.parent, "main");
  ASSERT_TRUE(a->created_at_us.has_value());
  EXPECT_GT(*a->created_at_us, 0U);
  EXPECT_EQ((*loaded)->branches[0].info.created_at_us, a->created_at_us);
  EXPECT_EQ(a->fork_counts, (ObjectCounts{.vertices = a->fork_ts + 1, .edges = a->fork_ts + 2}));
  EXPECT_EQ((*loaded)->branches[0].info.fork_counts, a->fork_counts);

  ASSERT_TRUE(store->RecordGidFloors(a->number, 7, 9).has_value());
  EXPECT_TRUE(store->RecordGidFloors(a->number, 99, 99).has_value()) << "second call is a no-op";
  EXPECT_EQ(store->Get("a")->vertex_gid_floor, 7);
  loaded = reader.Read();
  ASSERT_TRUE(loaded.has_value() && loaded->has_value());
  EXPECT_EQ((*loaded)->branches[0].info.vertex_gid_floor, 7);
  EXPECT_EQ((*loaded)->branches[0].info.edge_gid_floor, 9);

  auto dropped = store->DropBranch("a");
  EXPECT_EQ(dropped, DropResult::kDropped);
  loaded = reader.Read();
  ASSERT_TRUE(loaded.has_value() && loaded->has_value());
  ASSERT_EQ((*loaded)->branches.size(), 1);
  EXPECT_EQ((*loaded)->branches[0].name, "b");
  EXPECT_EQ((*loaded)->next_number, 4) << "numbers are never reused";
}

TEST_F(BranchLedgerTest, LedgerWriteFailureLeavesNoPinOrBranch) {
  auto store = MakeStore();
  ASSERT_TRUE(store->CreateBranch("a").has_value());
  const auto live_before = live_.size();

  auto tmp = file_;
  tmp += ".tmp";
  fs::create_directory(tmp);

  EXPECT_FALSE(store->CreateBranch("b").has_value());
  EXPECT_FALSE(store->Exists("b"));
  EXPECT_EQ(live_.size(), live_before);

  auto dropped = store->DropBranch("a");
  EXPECT_FALSE(dropped.has_value());
  EXPECT_TRUE(store->Exists("a"));
  EXPECT_EQ(live_.size(), live_before);
  auto attach = store->TryAcquireCheckout("a");
  ASSERT_TRUE(attach.has_value()) << "a failed drop must not leave the branch unattachable";
  store->ReleaseCheckout(attach->number);

  fs::remove(tmp);
  auto b = store->CreateBranch("b");
  ASSERT_TRUE(b.has_value());
  EXPECT_EQ(b->number, 3) << "a failed create must not consume a number";
  EXPECT_EQ(store->DropBranch("a"), DropResult::kDropped) << "a failed drop must leave the branch droppable";
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

TEST(BranchInfo, RecoverableBelowIsOneBelowTheRecordedForkClampedAtZero) {
  using memgraph::versioning::BranchInfo;
  using memgraph::versioning::RecoverableBelow;
  EXPECT_EQ(RecoverableBelow(BranchInfo{.number = 2, .parent = "main", .fork_ts = 0}), 0);
  EXPECT_EQ(RecoverableBelow(BranchInfo{.number = 2, .parent = "main", .fork_ts = 1}), 0);
  EXPECT_EQ(RecoverableBelow(BranchInfo{.number = 2, .parent = "main", .fork_ts = 42}), 41);
  EXPECT_EQ(RecoverableBelow(BranchInfo{.number = 2, .parent = "main", .fork_ts = 7, .recorded_fork_ts = 42}), 41)
      << "the recorded fork, not the runtime pin";
}

namespace {
namespace ms = memgraph::storage;

const fs::path kRestartDirectory{fs::temp_directory_path() / "versioning_ledger_restart"};

ms::Config MakeRestartConfig(bool recover_on_startup) {
  return ms::Config{
      .durability = {.storage_directory = kRestartDirectory,
                     .recover_on_startup = recover_on_startup,
                     .snapshot_wal_mode = ms::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                     .snapshot_interval = memgraph::utils::SchedulerInterval{std::chrono::hours(24)}},
      .experimental_versioning = true,
  };
}
}  // namespace

TEST(BranchLedgerRestartTest, AnUnreadableLedgerRefusesStartup) {
  fs::remove_all(kRestartDirectory);
  {
    memgraph::dbms::Database db{MakeRestartConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    {
      auto acc = db.Access(ms::WRITE);
      acc->CreateVertex();
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    ASSERT_TRUE(db.version_store()->CreateBranch("b").has_value());
  }

  const auto ledger_file = kRestartDirectory / "branches" / "ledger";
  auto data = Slurp(ledger_file);
  ASSERT_FALSE(data.empty());
  data.back() = static_cast<char>(data.back() ^ 0x5a);
  Spit(ledger_file, data);
  ASSERT_FALSE(BranchLedger{ledger_file}.Read().has_value()) << "the ledger must be unreadable";

  memgraph::tests::ExpectStartupRefused(
      kRestartDirectory, MakeRestartConfig(/*recover_on_startup=*/true), {"branch ledger", "cannot be read"});
  fs::remove_all(kRestartDirectory);
}
