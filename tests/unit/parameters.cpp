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

#include <unistd.h>

#include <algorithm>
#include <filesystem>
#include <iterator>
#include <ranges>
#include <string>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>

#include "parameters/parameters.hpp"
#include "utils/file.hpp"

namespace fs = std::filesystem;
using memgraph::parameters::kGlobalScope;
using memgraph::parameters::ParameterInfo;
using memgraph::parameters::Parameters;
using memgraph::parameters::SetParameterResult;

namespace {

// Database scopes are database uuids; the values only have to be distinct.
constexpr std::string_view kDbScope = "801d75e3-0beb-4810-9b10-d31223169cba";
constexpr std::string_view kOtherDbScope = "66dde0d0-2eeb-4209-86c9-43e5a442048f";

auto SortedKeys(std::vector<ParameterInfo> const &params) -> std::vector<std::string> {
  std::vector<std::string> keys;
  keys.reserve(params.size());
  std::ranges::transform(
      params, std::back_inserter(keys), [](auto const &p) { return p.scope_context + "/" + p.name; });
  std::ranges::sort(keys);
  return keys;
}

}  // namespace

class ParametersTest : public ::testing::Test {
 protected:
  void SetUp() override { memgraph::utils::EnsureDir(test_folder_); }

  void TearDown() override { fs::remove_all(test_folder_); }

  auto MakeParameters(std::string_view name) -> Parameters { return Parameters{test_folder_ / name}; }

  fs::path test_folder_{fs::temp_directory_path() /
                        ("unit_parameters_test_" + std::to_string(static_cast<int>(getpid())))};
};

TEST_F(ParametersTest, SetGetUnset) {
  auto parameters = MakeParameters("SetGetUnset");

  ASSERT_EQ(parameters.SetParameter("threshold", "100", kGlobalScope), SetParameterResult::Success);
  EXPECT_EQ(parameters.GetParameter("threshold", kGlobalScope), "100");

  EXPECT_TRUE(parameters.UnsetParameter("threshold", kGlobalScope));
  EXPECT_FALSE(parameters.GetParameter("threshold", kGlobalScope).has_value());
}

TEST_F(ParametersTest, ScopesAreIndependent) {
  auto parameters = MakeParameters("ScopesAreIndependent");

  ASSERT_EQ(parameters.SetParameter("tier", R"("global")", kGlobalScope), SetParameterResult::Success);
  ASSERT_EQ(parameters.SetParameter("tier", R"("db")", kDbScope), SetParameterResult::Success);

  EXPECT_EQ(parameters.GetParameter("tier", kGlobalScope), R"("global")");
  EXPECT_EQ(parameters.GetParameter("tier", kDbScope), R"("db")");

  EXPECT_TRUE(parameters.UnsetParameter("tier", kDbScope));
  EXPECT_EQ(parameters.GetParameter("tier", kGlobalScope), R"("global")");
}

TEST_F(ParametersTest, GetParametersReturnsGlobalPlusRequestedScope) {
  auto parameters = MakeParameters("GetParametersReturnsGlobalPlusRequestedScope");

  ASSERT_EQ(parameters.SetParameter("g", "1", kGlobalScope), SetParameterResult::Success);
  ASSERT_EQ(parameters.SetParameter("mine", "2", kDbScope), SetParameterResult::Success);
  ASSERT_EQ(parameters.SetParameter("theirs", "3", kOtherDbScope), SetParameterResult::Success);

  EXPECT_EQ(SortedKeys(parameters.GetParameters(kDbScope)),
            (std::vector<std::string>{std::string{kDbScope} + "/mine", "global/g"}));
}

TEST_F(ParametersTest, SnapshotRoundTrips) {
  auto source = MakeParameters("SnapshotRoundTripsSource");
  ASSERT_EQ(source.SetParameter("g", R"("gv")", kGlobalScope), SetParameterResult::Success);
  ASSERT_EQ(source.SetParameter("d", R"("dv")", kDbScope), SetParameterResult::Success);

  auto target = MakeParameters("SnapshotRoundTripsTarget");
  ASSERT_TRUE(target.ApplyRecovery(source.GetSnapshotForRecovery()));

  EXPECT_EQ(SortedKeys(target.GetSnapshotForRecovery()), SortedKeys(source.GetSnapshotForRecovery()));
  EXPECT_EQ(target.GetParameter("d", kDbScope), R"("dv")");
}

// Recovering a snapshot the store already holds must leave every value in place. Only keys the
// snapshot does not carry are deleted, so the incoming and stale sets are disjoint and no key is
// ever both written and dropped.
TEST_F(ParametersTest, RecoveryOfAnIdenticalSnapshotKeepsEveryValue) {
  auto parameters = MakeParameters("RecoveryOfAnIdenticalSnapshotKeepsEveryValue");
  ASSERT_EQ(parameters.SetParameter("g", R"("gv")", kGlobalScope), SetParameterResult::Success);
  ASSERT_EQ(parameters.SetParameter("d", R"("dv")", kDbScope), SetParameterResult::Success);

  ASSERT_TRUE(parameters.ApplyRecovery(parameters.GetSnapshotForRecovery()));

  EXPECT_EQ(parameters.CountParameters(), 2);
  EXPECT_EQ(parameters.GetParameter("g", kGlobalScope), R"("gv")");
  EXPECT_EQ(parameters.GetParameter("d", kDbScope), R"("dv")");
}

// A replica joining a cluster must end up with main's parameters and nothing else. Recovery
// replaces local state for databases and auth (system_replication.cpp), so parameters must too:
// a parameter set while the instance was standalone has to go, or the replica silently resolves
// a $placeholder to a value that exists nowhere in the cluster.
TEST_F(ParametersTest, RecoveryDiscardsPreExistingParameters) {
  auto main = MakeParameters("RecoveryDiscardsPreExistingParametersMain");
  ASSERT_EQ(main.SetParameter("on_main", R"("MAIN-only")", kGlobalScope), SetParameterResult::Success);
  ASSERT_EQ(main.SetParameter("shared", R"("from-MAIN")", kGlobalScope), SetParameterResult::Success);
  ASSERT_EQ(main.SetParameter("db_on_main", R"("MAIN-db")", kDbScope), SetParameterResult::Success);

  auto replica = MakeParameters("RecoveryDiscardsPreExistingParametersReplica");
  ASSERT_EQ(replica.SetParameter("on_replica", R"("REPLICA-only")", kGlobalScope), SetParameterResult::Success);
  ASSERT_EQ(replica.SetParameter("shared", R"("from-REPLICA")", kGlobalScope), SetParameterResult::Success);
  ASSERT_EQ(replica.SetParameter("db_on_replica", R"("REPLICA-db")", kOtherDbScope), SetParameterResult::Success);

  ASSERT_TRUE(replica.ApplyRecovery(main.GetSnapshotForRecovery()));

  EXPECT_EQ(SortedKeys(replica.GetSnapshotForRecovery()), SortedKeys(main.GetSnapshotForRecovery()));
  EXPECT_EQ(replica.CountParameters(), main.CountParameters());
  EXPECT_FALSE(replica.GetParameter("on_replica", kGlobalScope).has_value());
  EXPECT_FALSE(replica.GetParameter("db_on_replica", kOtherDbScope).has_value());
  EXPECT_EQ(replica.GetParameter("shared", kGlobalScope), R"("from-MAIN")");
}
