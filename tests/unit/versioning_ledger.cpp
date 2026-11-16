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

#include <fmt/format.h>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <tuple>
#include <vector>

#include "kvstore/kvstore.hpp"
#include "utils/uuid.hpp"
#include "versioning/branch_info.hpp"
#include "versioning/branch_ledger.hpp"

namespace fs = std::filesystem;
using memgraph::versioning::BranchLedger;
using memgraph::versioning::GidKind;
using memgraph::versioning::GidsHeldBelow;
using memgraph::versioning::kBranchLedgerKeyPrefix;
using memgraph::versioning::LedgerContents;

namespace {
std::string OneBranch(const std::string &branch, uint64_t next_number = 3) {
  return fmt::format(R"({{"next_number":{},"branches":[{}]}})", next_number, branch);
}

// Prepends the current schema version to a JSON object body.
std::string Versioned(const std::string &object, uint32_t version = BranchLedger::kVersion) {
  return fmt::format(R"({{"version":{},{})", version, object.substr(1));
}

std::string KeyOf(const memgraph::utils::UUID &uuid) { return std::string{kBranchLedgerKeyPrefix} + std::string{uuid}; }
}  // namespace

class BranchLedgerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ = fs::temp_directory_path() / ("mg_branch_ledger_" + memgraph::utils::GenerateUUID());
    store_ = std::make_unique<memgraph::kvstore::KVStore>(dir_);
  }

  void TearDown() override {
    store_.reset();
    std::error_code ec;
    fs::remove_all(dir_, ec);
  }

  void Put(const std::string &value) { ASSERT_TRUE(store_->Put(KeyOf(uuid_), value)); }

  std::expected<std::optional<LedgerContents>, std::string> Read() const { return BranchLedger{*store_, uuid_}.Read(); }

  fs::path dir_;
  std::unique_ptr<memgraph::kvstore::KVStore> store_;
  memgraph::utils::UUID uuid_;
};

TEST_F(BranchLedgerTest, MissingKeyReadsAsNullopt) {
  auto res = Read();
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
  // The persisted fork_ts is the durable recorded one, not the runtime pin.
  in.branches.push_back({"b", {.number = 4, .parent = "a", .fork_ts = 100, .recorded_fork_ts = 20}});

  BranchLedger ledger{*store_, uuid_};
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
  EXPECT_EQ(out.branches[1].info.created_at_us, std::nullopt);
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
  Put(Versioned(OneBranch(
      R"({"name":"a","number":2,"parent":"main","fork_ts":10,"vertex_gid_floor":7,"edge_gid_floor":9,"description":"old"})")));
  const auto res = Read();
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

TEST_F(BranchLedgerTest, RejectsUnparsableValues) {
  const auto good = Versioned(OneBranch(R"({"name":"a","number":2,"parent":"main","fork_ts":10})"));
  for (const auto &value : {good.substr(0, good.size() - 3), std::string{}, std::string{"not json"}}) {
    Put(value);
    EXPECT_FALSE(Read().has_value()) << value;
  }
}

TEST_F(BranchLedgerTest, RejectsAMissingOrUnsupportedVersion) {
  const auto body = OneBranch(R"({"name":"a","number":2,"parent":"main","fork_ts":10})");
  Put(body);
  EXPECT_FALSE(Read().has_value()) << "missing version";
  for (const uint32_t version : {BranchLedger::kVersion + 1, uint32_t{0}}) {
    Put(Versioned(body, version));
    const auto res = Read();
    ASSERT_FALSE(res.has_value()) << version;
    EXPECT_NE(res.error().find(fmt::format("unsupported version {}", version)), std::string::npos) << res.error();
  }
}

TEST_F(BranchLedgerTest, RejectsAMalformedPayload) {
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
    Put(Versioned(payload));
    EXPECT_FALSE(Read().has_value()) << payload;
  }
}

TEST_F(BranchLedgerTest, RejectsAnInvalidRegistry) {
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
    Put(Versioned(payload));
    EXPECT_FALSE(Read().has_value()) << payload;
  }

  // A parent need only be a valid name, not a listed branch.
  Put(Versioned(OneBranch(branch("a", 2, "gone"))));
  const auto res = Read();
  ASSERT_TRUE(res.has_value() && res->has_value()) << (res ? "" : res.error());
  EXPECT_EQ((*res)->branches[0].info.parent, "gone");

  Put(Versioned(OneBranch(
      R"({"name":"a","number":2,"parent":"main","fork_ts":10,"vertex_gid_floor":7,"vertex_gid_ceiling":7})")));
  const auto equal = Read();
  ASSERT_TRUE(equal.has_value() && equal->has_value()) << (equal ? "" : equal.error());
  EXPECT_EQ((*equal)->branches[0].info.vertex_gid_ceiling, 7);
}

TEST_F(BranchLedgerTest, WriteRefusesWhatReadWouldReject) {
  BranchLedger ledger{*store_, uuid_};
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

TEST_F(BranchLedgerTest, DatabasesInOneStoreAreIndependent) {
  const memgraph::utils::UUID other;
  LedgerContents a;
  a.next_number = 3;
  a.branches.push_back({"a", {.number = 2, .parent = "main", .fork_ts = 1}});
  LedgerContents b;
  b.next_number = 4;
  b.branches.push_back({"b", {.number = 3, .parent = "main", .fork_ts = 2}});
  ASSERT_TRUE((BranchLedger{*store_, uuid_}.Write(a).has_value()));
  ASSERT_TRUE((BranchLedger{*store_, other}.Write(b).has_value()));

  ASSERT_TRUE(store_->Put(KeyOf(uuid_), "garbage"));
  EXPECT_FALSE(Read().has_value());
  const auto res = BranchLedger{*store_, other}.Read();
  ASSERT_TRUE(res.has_value() && res->has_value()) << (res ? "" : res.error());
  EXPECT_EQ((*res)->branches[0].name, "b");
}
