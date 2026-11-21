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
#include <array>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <memory>
#include <nlohmann/json.hpp>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "kvstore/kvstore.hpp"
#include "utils/uuid.hpp"
#include "versioning/branch_info.hpp"
#include "versioning/branch_ledger.hpp"

namespace fs = std::filesystem;
using memgraph::versioning::BranchInfo;
using memgraph::versioning::BranchLedger;
using memgraph::versioning::ForkPoint;
using memgraph::versioning::kBranchLedgerKeyPrefix;
using memgraph::versioning::LedgerContents;
using memgraph::versioning::ObjectCounts;
using nlohmann::json;

namespace {
// Every field distinct and derived from `seed`, so a mixed-up field shows.
ForkPoint ForkAt(uint64_t seed) {
  return {.durable_fork_ts = seed,
          .main_durable_ts = seed + 1,
          .vertex_gid_floor = seed + 2,
          .edge_gid_floor = seed + 3,
          .counts = {.vertices = seed + 4, .edges = seed + 5}};
}

BranchInfo InfoOf(uint64_t id, uint64_t seed) {
  return {.id = id, .fork = ForkAt(seed), .created_at_us = 1'700'000'000'000'000 + seed};
}

// A complete ledger branch, as the ledger writes it.
json BranchJson(std::string_view name, uint64_t id) {
  return {{"name", name},
          {"id", id},
          {"durable_fork_ts", 10},
          {"main_durable_ts", 11},
          {"vertex_gid_floor", 12},
          {"edge_gid_floor", 13},
          {"fork_vertex_count", 14},
          {"fork_edge_count", 15},
          {"created_at_us", 16}};
}

constexpr std::array kBranchKeys{"name",
                                 "id",
                                 "durable_fork_ts",
                                 "main_durable_ts",
                                 "vertex_gid_floor",
                                 "edge_gid_floor",
                                 "fork_vertex_count",
                                 "fork_edge_count",
                                 "created_at_us"};

std::string Payload(json branches, uint64_t next_branch_id = 3, uint32_t version = BranchLedger::kVersion) {
  return json{{"version", version}, {"next_branch_id", next_branch_id}, {"branches", std::move(branches)}}.dump();
}

std::string OneBranch(const json &branch) { return Payload(json::array({branch})); }

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
  const LedgerContents in{.next_branch_id = 5, .branches = {{"a", InfoOf(2, 10)}, {"b", InfoOf(4, 20)}}};

  BranchLedger ledger{*store_, uuid_};
  ASSERT_TRUE(ledger.Write(in).has_value());
  auto res = ledger.Read();
  ASSERT_TRUE(res.has_value() && res->has_value());
  const auto &out = **res;
  EXPECT_EQ(out.next_branch_id, 5);
  ASSERT_EQ(out.branches.size(), 2);
  for (size_t i = 0; i < 2; ++i) {
    EXPECT_EQ(out.branches[i].name, in.branches[i].name);
    EXPECT_EQ(out.branches[i].info.id, in.branches[i].info.id);
    EXPECT_EQ(out.branches[i].info.fork, in.branches[i].info.fork);
    EXPECT_EQ(out.branches[i].info.created_at_us, in.branches[i].info.created_at_us);
  }
}

TEST_F(BranchLedgerTest, EmptyLedgerKeepsNextBranchId) {
  BranchLedger ledger{*store_, uuid_};
  ASSERT_TRUE(ledger.Write({.next_branch_id = 7, .branches = {}}).has_value());
  const auto res = ledger.Read();
  ASSERT_TRUE(res.has_value() && res->has_value());
  EXPECT_EQ((*res)->next_branch_id, 7);
  EXPECT_TRUE((*res)->branches.empty());
}

TEST_F(BranchLedgerTest, RejectsUnparsableValues) {
  const auto good = OneBranch(BranchJson("a", 2));
  for (const auto &value : {good.substr(0, good.size() - 3),
                            std::string{},
                            std::string{"not json"},
                            std::string{"[]"},
                            std::string{"5"},
                            std::string{"null"},
                            std::string{R"("x")"}}) {
    Put(value);
    EXPECT_FALSE(Read().has_value()) << value;
  }
}

TEST_F(BranchLedgerTest, RejectsAMissingOrUnsupportedVersion) {
  auto root = json::parse(OneBranch(BranchJson("a", 2)));
  root.erase("version");
  Put(root.dump());
  EXPECT_FALSE(Read().has_value()) << "missing version";
  for (const uint32_t version : {BranchLedger::kVersion + 1, uint32_t{0}}) {
    Put(Payload(json::array({BranchJson("a", 2)}), 3, version));
    const auto res = Read();
    ASSERT_FALSE(res.has_value()) << version;
    EXPECT_NE(res.error().find(fmt::format("unsupported version {}", version)), std::string::npos) << res.error();
  }
}

TEST_F(BranchLedgerTest, RejectsAMalformedPayload) {
  const auto good = json::parse(OneBranch(BranchJson("a", 2)));
  for (const char *key : {"version", "next_branch_id", "branches"}) {
    auto root = good;
    root.erase(key);
    Put(root.dump());
    EXPECT_FALSE(Read().has_value()) << "missing " << key;
  }
  for (const auto &bad : {json::object(), json(-3), json("3"), json(1.5)}) {
    auto root = good;
    root["next_branch_id"] = bad;
    Put(root.dump());
    EXPECT_FALSE(Read().has_value()) << root;
  }
  auto not_array = good;
  not_array["branches"] = json::object();
  Put(not_array.dump());
  EXPECT_FALSE(Read().has_value()) << not_array;

  // Every branch key is required and typed.
  for (const char *key : kBranchKeys) {
    auto branch = BranchJson("a", 2);
    branch.erase(key);
    Put(OneBranch(branch));
    EXPECT_FALSE(Read().has_value()) << "missing " << key;

    for (const auto &bad : {json(-1), json(1.5), json("2"), json(nullptr)}) {
      // A string is the valid type for the name.
      if (std::string_view{key} == "name" && bad.is_string()) continue;
      branch = BranchJson("a", 2);
      branch[key] = bad;
      Put(OneBranch(branch));
      EXPECT_FALSE(Read().has_value()) << key << " = " << bad;
    }
  }
}

TEST_F(BranchLedgerTest, RejectsAnInvalidRegistry) {
  for (const auto &payload : {
           Payload(json::array(), 1),
           OneBranch(BranchJson("a", 0)),
           OneBranch(BranchJson("a", 1)),
           OneBranch(BranchJson("a", 3)),
           OneBranch(BranchJson("main", 2)),
           OneBranch(BranchJson("a/b", 2)),
           OneBranch(BranchJson("-a", 2)),
           Payload(json::array({BranchJson("a", 2), BranchJson("a", 3)}), 5),
           Payload(json::array({BranchJson("a", 2), BranchJson("b", 2)}), 5),
       }) {
    Put(payload);
    EXPECT_FALSE(Read().has_value()) << payload;
  }
}

TEST_F(BranchLedgerTest, WriteRefusesWhatReadWouldReject) {
  BranchLedger ledger{*store_, uuid_};
  ASSERT_TRUE(ledger.Write({.next_branch_id = 3, .branches = {{"a", InfoOf(2, 1)}}}).has_value());

  for (const auto &[name, branch_id] :
       {std::pair{"\xff", 2}, std::pair{"main", 2}, std::pair{"a", 1}, std::pair{"a", 3}}) {
    std::expected<void, std::string> res;
    ASSERT_NO_THROW(
        res = ledger.Write({.next_branch_id = 3, .branches = {{name, InfoOf(static_cast<uint64_t>(branch_id), 1)}}}));
    ASSERT_FALSE(res.has_value());
    EXPECT_NE(res.error().find("refusing to write an invalid registry"), std::string::npos) << res.error();
  }

  const auto dup_res = ledger.Write({.next_branch_id = 4, .branches = {{"a", InfoOf(2, 1)}, {"a", InfoOf(3, 1)}}});
  ASSERT_FALSE(dup_res.has_value());
  EXPECT_NE(dup_res.error().find("refusing to write an invalid registry"), std::string::npos) << dup_res.error();

  const auto res = ledger.Read();
  ASSERT_TRUE(res.has_value() && res->has_value());
  EXPECT_EQ((*res)->branches.size(), 1);
}

TEST_F(BranchLedgerTest, DatabasesInOneStoreAreIndependent) {
  const memgraph::utils::UUID other;
  ASSERT_TRUE(
      (BranchLedger{*store_, uuid_}.Write({.next_branch_id = 3, .branches = {{"a", InfoOf(2, 1)}}}).has_value()));
  ASSERT_TRUE(
      (BranchLedger{*store_, other}.Write({.next_branch_id = 4, .branches = {{"b", InfoOf(3, 2)}}}).has_value()));

  ASSERT_TRUE(store_->Put(KeyOf(uuid_), "garbage"));
  EXPECT_FALSE(Read().has_value());
  const auto res = BranchLedger{*store_, other}.Read();
  ASSERT_TRUE(res.has_value() && res->has_value()) << (res ? "" : res.error());
  EXPECT_EQ((*res)->branches[0].name, "b");
}
