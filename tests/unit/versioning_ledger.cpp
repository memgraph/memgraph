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
#include <cstdint>
#include <expected>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <string_view>
#include <system_error>
#include <tuple>
#include <vector>

#include "utils/crc_accumulator.hpp"
#include "utils/uuid.hpp"
#include "versioning/branch_info.hpp"
#include "versioning/branch_ledger.hpp"

namespace fs = std::filesystem;
using memgraph::versioning::BranchLedger;
using memgraph::versioning::LedgerContents;

namespace {
std::string Slurp(const fs::path &p) {
  std::ifstream in(p, std::ios::binary);
  return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

void Spit(const fs::path &p, const std::string &data) {
  std::ofstream out(p, std::ios::binary | std::ios::trunc);
  out << data;
}

// A ledger file with a valid header around `payload`.
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

  fs::path dir_;
  fs::path file_;
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
  EXPECT_EQ(out.branches[0].info.number, 2);
  EXPECT_EQ(out.branches[0].info.parent, "main");
  EXPECT_EQ(out.branches[0].info.fork_ts, 10);
  EXPECT_EQ(out.branches[0].info.recorded_fork_ts, 10);
  EXPECT_EQ(out.branches[1].info.fork_ts, 20);
  EXPECT_EQ(out.branches[1].info.recorded_fork_ts, 20);
  EXPECT_EQ(out.branches[0].info.vertex_gid_floor, 11);
  EXPECT_EQ(out.branches[0].info.edge_gid_floor, 13);
  EXPECT_EQ(out.branches[0].info.created_at_us, 1'700'000'000'123'456);
  EXPECT_EQ(out.branches[1].info.created_at_us, std::nullopt);
  EXPECT_EQ(out.branches[1].info.parent, "a");
  EXPECT_EQ(out.branches[1].info.vertex_gid_floor, std::nullopt);
}

TEST_F(BranchLedgerTest, PersistsRecordedForkTsNotTheRuntimePin) {
  // After a bounded-recovery restore the runtime pin (fork_ts) is re-derived and can differ from the
  // durable fork boundary; the ledger must persist the durable one.
  LedgerContents in;
  in.next_number = 3;
  in.branches.push_back({"a", {.number = 2, .parent = "main", .fork_ts = 100, .recorded_fork_ts = 10}});

  BranchLedger ledger{file_};
  ASSERT_TRUE(ledger.Write(in).has_value());
  const auto res = ledger.Read();
  ASSERT_TRUE(res.has_value() && res->has_value());
  ASSERT_EQ((*res)->branches.size(), 1);
  EXPECT_EQ((*res)->branches[0].info.fork_ts, 10);
  EXPECT_EQ((*res)->branches[0].info.recorded_fork_ts, 10);
}

TEST_F(BranchLedgerTest, ReadsALedgerWrittenWithADescription) {
  fs::create_directories(file_.parent_path());
  Spit(file_, Framed(OneBranch(R"({"name":"a","number":2,"parent":"main","fork_ts":10,"description":"old"})")));

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
  Spit(file_, Framed(OneBranch(R"({"name":"a","number":2,"parent":"main","fork_ts":10})"), BranchLedger::kVersion + 1));
  const auto res = BranchLedger{file_}.Read();
  ASSERT_FALSE(res.has_value());
  EXPECT_NE(res.error().find("unsupported version 2"), std::string::npos) << res.error();
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

  // A dropped parent leaves its children in place, so an unknown parent is valid.
  Spit(file_, Framed(OneBranch(branch("a", 2, "gone"))));
  const auto res = BranchLedger{file_}.Read();
  ASSERT_TRUE(res.has_value() && res->has_value()) << (res ? "" : res.error());
  EXPECT_EQ((*res)->branches[0].info.parent, "gone");
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
    EXPECT_FALSE(res.has_value());
  }

  LedgerContents dup;
  dup.next_number = 4;
  dup.branches.push_back({"a", {.number = 2, .parent = "main", .fork_ts = 1}});
  dup.branches.push_back({"a", {.number = 3, .parent = "main", .fork_ts = 1}});
  EXPECT_FALSE(ledger.Write(dup).has_value());

  const auto res = ledger.Read();
  ASSERT_TRUE(res.has_value() && res->has_value());
  EXPECT_EQ((*res)->branches.size(), 1);
}

TEST_F(BranchLedgerTest, StaleTempFileDoesNotAffectTheNextWrite) {
  fs::create_directories(file_.parent_path());
  auto tmp = file_;
  tmp += ".tmp";
  Spit(tmp, std::string(4096, 'x'));

  LedgerContents in;
  in.next_number = 3;
  in.branches.push_back({"a", {.number = 2, .parent = "main", .fork_ts = 7}});
  BranchLedger ledger{file_};
  ASSERT_TRUE(ledger.Write(in).has_value());
  EXPECT_FALSE(fs::exists(tmp));
  const auto res = ledger.Read();
  ASSERT_TRUE(res.has_value() && res->has_value());
  EXPECT_EQ((*res)->branches[0].info.fork_ts, 7);
}

// A crash between writing the temp file and renaming it leaves the new contents only in the temp file.
TEST_F(BranchLedgerTest, CrashBeforeRenameKeepsTheOldLedger) {
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

  const auto res = ledger.Read();
  ASSERT_TRUE(res.has_value() && res->has_value());
  EXPECT_EQ((*res)->next_number, 3);
  ASSERT_EQ((*res)->branches.size(), 1);
  EXPECT_EQ((*res)->branches[0].name, "a");
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
