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

// Interpreter-level fixture for Graph Versioning: an in-process Database driven through Interpreter::Prepare/Pull, with
// sessions attached to branches through the C++ API (the branch statements are not in the grammar yet).
#pragma once

#include <errno.h>  // program_invocation_short_name

#include <cctype>
#include <cstdint>
#include <exception>
#include <expected>
#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>

#include "dbms/database.hpp"
#include "interpreter_faker.hpp"
#include "license/license.hpp"
#include "query/interpreter.hpp"
#include "query/interpreter_context.hpp"
#include "replication/state.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "utils/gatekeeper.hpp"
#include "utils/logging.hpp"
#include "utils/synchronized.hpp"
#include "versioning/branch_engine.hpp"
#include "versioning/version_store.hpp"

namespace memgraph::tests::versioning_fixture {

constexpr auto kNoHandler = nullptr;
constexpr int64_t kNullInt = std::numeric_limits<int64_t>::min();

using Rows = std::vector<std::vector<int64_t>>;

inline Rows IntRows(InterpreterFaker &session, const std::string &query) {
  auto stream = session.Interpret(query);
  Rows rows;
  for (const auto &row : stream.GetResults()) {
    auto &out = rows.emplace_back();
    for (const auto &value : row) {
      out.push_back(value.type() == memgraph::communication::bolt::Value::Type::Null ? kNullInt : value.ValueInt());
    }
  }
  return rows;
}

inline std::string ExplainText(InterpreterFaker &session, const std::string &query) {
  auto stream = session.Interpret("EXPLAIN " + query);
  std::string plan;
  for (const auto &row : stream.GetResults()) {
    plan += row[0].ValueString();
    plan += '\n';
  }
  return plan;
}

template <typename Fn>
std::string ThrownMessage(Fn &&fn) {
  try {
    fn();
  } catch (const std::exception &e) {
    return e.what();
  }
  return "<no exception>";
}

// True when `plan` contains the operator named exactly `op`, not merely as the prefix of a longer operator name
// (ScanAllByLabel inside ScanAllByLabelProperties).
inline bool PlanHasOperator(std::string_view plan, std::string_view op) {
  auto const is_identifier_char = [](char c) { return std::isalnum(static_cast<unsigned char>(c)) || c == '_'; };
  for (auto pos = plan.find(op); pos != std::string_view::npos; pos = plan.find(op, pos + 1)) {
    auto const end = pos + op.size();
    if ((pos == 0 || !is_identifier_char(plan[pos - 1])) && (end == plan.size() || !is_identifier_char(plan[end]))) {
      return true;
    }
  }
  return false;
}

}  // namespace memgraph::tests::versioning_fixture

// `kPropertiesOnEdges = false` builds the storage with reference (light) edges; `kNarrowing` turns
// experimental_commit_lock_narrowing on for main.
template <bool kPropertiesOnEdges, bool kWal = true, bool kRecover = true, bool kNarrowing = false>
class VersioningFixture : public ::testing::Test {
 protected:
  // Per test binary: ctest runs the versioning suites in parallel and they all share this fixture.
  std::filesystem::path data_directory =
      std::filesystem::temp_directory_path() /
      (std::string{program_invocation_short_name} + (!kWal                ? "_interpreter_no_wal"
                                                     : !kRecover          ? "_interpreter_no_recovery"
                                                     : kNarrowing         ? "_interpreter_narrowing"
                                                     : kPropertiesOnEdges ? "_interpreter"
                                                                          : "_interpreter_reference_edge"));

  memgraph::storage::Config config{[&] {
    memgraph::storage::Config config{};
    config.durability.storage_directory = data_directory;
    config.experimental_versioning = true;
    config.experimental_commit_lock_narrowing = kNarrowing;
    config.durability.recover_on_startup = kRecover;
    config.durability.snapshot_wal_mode =
        kWal ? memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL
             : memgraph::storage::Config::Durability::SnapshotWalMode::DISABLED;
    config.disk.main_storage_directory = data_directory / "disk";
    config.salient.items.properties_on_edges = kPropertiesOnEdges;
    return config;
  }()};

  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state{
      memgraph::storage::ReplicationStateRootPath(config)};
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{config};
  memgraph::dbms::DatabaseAccess db{[&] {
    auto db_acc_opt = db_gk.access();
    MG_ASSERT(db_acc_opt, "Failed to access db");
    auto &db_acc = *db_acc_opt;
    MG_ASSERT(db_acc->GetStorageMode() == memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL,
              "Wrong storage mode!");
    return db_acc;
  }()};

  memgraph::system::System system_state;
  memgraph::query::InterpreterContext interpreter_context{{},
                                                          nullptr,
                                                          nullptr,
                                                          memgraph::tests::versioning_fixture::kNoHandler,
                                                          &repl_state,
                                                          system_state,
                                                          nullptr,
                                                          nullptr,
                                                          nullptr};

  InterpreterFaker faker{&interpreter_context, db};

  void SetUp() override { SetVersioningFlag(false); }

  // The flag is a startup setting; flipping it on the live config is how these tests exercise the gate.
  void SetVersioningFlag(bool enabled) { db->storage()->config_.experimental_versioning = enabled; }

  void TearDown() override {
    memgraph::license::global_license_checker.DisableTesting();
    std::filesystem::remove_all(data_directory);
  }

  void SatisfyGate() {
    SetVersioningFlag(true);
    memgraph::license::global_license_checker.EnableTesting();
  }

  // Attaches `session` to the existing branch `name` the way a session checkout does.
  void Attach(InterpreterFaker &session, const std::string &name) {
    auto *version_store = db->version_store();
    auto info = version_store->TryAcquireCheckout(name);
    ASSERT_TRUE(info) << info.error();
    auto *main = static_cast<memgraph::storage::InMemoryStorage *>(db->storage());
    auto core = version_store->GetOrBuildCore(
        info->number, [&]() -> std::expected<std::shared_ptr<memgraph::versioning::BranchCore>, std::string> {
          auto built = memgraph::versioning::BranchCore::BuildForCheckout(*main, *info);
          if (!built) return std::unexpected(std::move(built.error().message));
          (*built)->SetGidReserver(version_store->GidReserverOf(info->number));
          return std::move(*built);
        });
    if (!core) version_store->ReleaseCheckout(info->number);
    ASSERT_TRUE(core) << core.error();
    auto recorded =
        version_store->RecordGidFloors(info->number, (*core)->vertex_gid_floor(), (*core)->edge_gid_floor());
    if (!recorded) version_store->ReleaseCheckout(info->number);
    ASSERT_TRUE(recorded) << recorded.error();
    session.interpreter.current_db_.SetBranchContext(
        std::make_unique<memgraph::versioning::BranchContext>(std::move(*core), name), version_store, info->number);
  }

  void Fork(InterpreterFaker &session, const std::string &name = "b") {
    auto created = db->version_store()->CreateBranch(name);
    ASSERT_TRUE(created) << created.error();
    session.interpreter.current_db_.LeaveBranch();
    Attach(session, name);
    ASSERT_EQ(session.interpreter.current_db_.CurrentVersion(), name);
    ASSERT_NE(session.interpreter.current_db_.branch_context(), nullptr);
  }

  void Fork(const std::string &name = "b") { Fork(faker, name); }
};

using VersioningInterpreterTest = VersioningFixture<true>;

class VersioningGatedTest : public VersioningInterpreterTest {
 protected:
  void SetUp() override {
    VersioningInterpreterTest::SetUp();
    SatisfyGate();
  }
};

class VersioningNarrowingGatedTest : public VersioningFixture<true, true, true, true> {
 protected:
  void SetUp() override {
    VersioningFixture<true, true, true, true>::SetUp();
    SatisfyGate();
  }
};
