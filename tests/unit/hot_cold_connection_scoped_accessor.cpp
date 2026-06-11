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

// Unit tests for the connection-scoped DatabaseAccess model under HOT_COLD_TENANTS.
//
// Design: a session's db_acc_ is CONNECTION-scoped — acquired at SetCurrentDB/USE/connect and held
// for the whole session, exactly like the flag-OFF (master) behaviour. The held accessor increments
// the gatekeeper use-count, which is the suspend-safety mechanism: a tenant is suspendable only when
// no session (or background task) holds an accessor (sole-accessor state). A COLD tenant is reheated
// synchronously via block-and-resume at SetCurrentDB/USE time (Resume-instead-of-Get), not via a
// per-query "resuming" exception.
//
// Contrast with the deleted transaction-scoped model, which released db_acc_ between queries.

#include "gtest/gtest.h"

#ifdef MG_ENTERPRISE

#include <chrono>
#include <filesystem>
#include <optional>
#include <string>
#include <thread>

#include "auth/auth.hpp"
#include "communication/result_stream_faker.hpp"
#include "dbms/constants.hpp"
#include "dbms/dbms_handler.hpp"
#include "flags/experimental.hpp"
#include "flags/general.hpp"
#include "flags/run_time_configurable.hpp"
#include "interpreter_faker.hpp"
#include "license/license.hpp"
#include "parameters/parameters.hpp"
#include "query/auth_checker.hpp"
#include "query/config.hpp"
#include "query/exceptions.hpp"
#include "query/interpreter.hpp"
#include "query/interpreter_context.hpp"
#include "query/typed_value.hpp"
#include "replication/state.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/gatekeeper.hpp"
#include "utils/logging.hpp"
#include "utils/synchronized.hpp"

namespace {

memgraph::storage::Config MakeConfig(const std::filesystem::path &dir) {
  memgraph::storage::Config cfg{};
  memgraph::storage::UpdatePaths(cfg, dir);
  cfg.durability.snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
  cfg.durability.recover_on_startup = false;
  cfg.durability.snapshot_on_exit = false;
  return cfg;
}

// Minimal harness — mirrors hot_cold_suspend.cpp MinMemgraph.
struct MinMemgraph {
  explicit MinMemgraph(const memgraph::storage::Config &conf)
      : settings{conf.durability.storage_directory / "settings"},
        auth{conf.durability.storage_directory / "auth", memgraph::auth::Auth::Config{}},
        parameters{conf.durability.storage_directory},
        repl_state{ReplicationStateRootPath(conf)},
        dbms{conf},
        interpreter_context{{}, &settings, &parameters, &dbms, &repl_state, system, nullptr, nullptr, nullptr} {
    memgraph::license::RegisterLicenseSettings(memgraph::license::global_license_checker, settings);
    memgraph::flags::run_time::Initialize(settings);
    memgraph::license::global_license_checker.CheckEnvLicense(settings);
  }

  auto NewInterpreter() { return InterpreterFaker{&interpreter_context, dbms.Get()}; }

  memgraph::utils::Settings settings;
  memgraph::auth::SynchedAuth auth;
  memgraph::system::System system;
  memgraph::parameters::Parameters parameters;
  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state;
  memgraph::dbms::DbmsHandler dbms;
  memgraph::query::InterpreterContext interpreter_context;
};

}  // namespace

class HotColdConnectionScopedAccessorTest : public ::testing::Test {
 public:
  std::filesystem::path data_directory =
      std::filesystem::temp_directory_path() / "MG_tests_unit_hot_cold_connection_scoped_accessor";

  void SetUp() override {
    TearDown();
    FLAGS_storage_hot_cold_min_hot_residency_sec = 0;
    min_mg.emplace(MakeConfig(data_directory));
  }

  void TearDown() override {
    min_mg.reset();
    // Reset experiments so a test that enabled HOT_COLD_TENANTS does not leak the
    // flag into any subsequent test (the experiment state is process-global).
    memgraph::flags::SetExperimental(memgraph::flags::Experiments{});
    if (std::filesystem::exists(data_directory)) std::filesystem::remove_all(data_directory);
  }

  auto &DBMS() { return min_mg->dbms; }

  // Create a tenant with SNAPSHOT+WAL durability and write N nodes so suspend can succeed.
  void CreateAndPopulate(const std::string &name, int n) {
    ASSERT_TRUE(DBMS().New(name).has_value());
    auto db_acc = DBMS().Get(name);
    auto storage_acc = db_acc->Access(memgraph::storage::WRITE);
    for (int i = 0; i < n; ++i) {
      storage_acc->CreateVertex();
    }
    ASSERT_TRUE(storage_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  std::optional<MinMemgraph> min_mg;
};

// ---------------------------------------------------------------------------
// C1 (flag ON): SetCurrentDB acquires and HOLDS the accessor for the session.
//               While a session holds it, the tenant is NOT suspendable
//               (gatekeeper count > sole-accessor => ACTIVE_CONNECTIONS).
// ---------------------------------------------------------------------------

TEST_F(HotColdConnectionScopedAccessorTest, SetCurrentDBAcquiresAndHoldsAccessor) {
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::HOT_COLD_TENANTS);

  const std::string db_name = "tenant_c1";
  CreateAndPopulate(db_name, 2);

  auto interpreter = min_mg->NewInterpreter();
  interpreter.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);

  // Connection-scoped: the accessor is engaged immediately after SetCurrentDB.
  EXPECT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value())
      << "flag ON connection-scoped: db_acc_ must be held right after SetCurrentDB";

  // Run an autocommit query — the accessor must remain engaged afterwards.
  {
    auto [stream, qid] = interpreter.Prepare("RETURN 1 AS x");
    EXPECT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value()) << "db_acc_ must be engaged during Prepare";
    interpreter.Pull(&stream);
    EXPECT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value())
        << "db_acc_ must STILL be held after an autocommit Pull (connection-scoped)";
  }

  // A held session accessor blocks suspend.
  auto suspend_res = DBMS().Suspend(db_name);
  ASSERT_FALSE(suspend_res.has_value()) << "Suspend must fail while a session holds the accessor";
  EXPECT_EQ(suspend_res.error(), memgraph::dbms::DbmsHandler::SuspendError::ACTIVE_CONNECTIONS);
}

// ---------------------------------------------------------------------------
// C2 (flag ON): the accessor persists across multiple queries and reads data
//               correctly; current_db_name_ stays in sync.
// ---------------------------------------------------------------------------

TEST_F(HotColdConnectionScopedAccessorTest, AccessorPersistsAcrossQueries) {
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::HOT_COLD_TENANTS);

  const std::string db_name = "tenant_c2";
  CreateAndPopulate(db_name, 3);

  auto interpreter = min_mg->NewInterpreter();
  interpreter.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);

  // First query.
  {
    auto [stream, qid] = interpreter.Prepare("MATCH (n) RETURN count(n) AS c");
    interpreter.Pull(&stream);
    const auto &results = stream.GetResults();
    ASSERT_EQ(results.size(), 1U);
    EXPECT_EQ(results[0][0].ValueInt(), 3) << "first query must read the 3 nodes";
  }

  // db_acc_ still held after the first query (connection-scoped).
  EXPECT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value())
      << "db_acc_ must remain engaged after the first query";
  ASSERT_TRUE(interpreter.interpreter.current_db_.current_db_name_.has_value())
      << "current_db_name_ must persist across queries";
  EXPECT_EQ(*interpreter.interpreter.current_db_.current_db_name_, db_name);

  // Second query on the same session — same held accessor.
  {
    auto [stream, qid] = interpreter.Prepare("MATCH (n) RETURN count(n) AS c");
    EXPECT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value())
        << "db_acc_ must still be held for the second query";
    interpreter.Pull(&stream);
    const auto &results = stream.GetResults();
    ASSERT_EQ(results.size(), 1U);
    EXPECT_EQ(results[0][0].ValueInt(), 3) << "second query must still read the 3 nodes";
  }

  EXPECT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value())
      << "db_acc_ must remain held after the second query";
}

// ---------------------------------------------------------------------------
// C3 (flag OFF regression): byte-identical to master — db_acc_ persists across
//                           queries and the tenant is not suspendable while held.
//                           (Under connection-scoped, flag ON behaves the same.)
// ---------------------------------------------------------------------------

TEST_F(HotColdConnectionScopedAccessorTest, FlagOffAccessorPersistsAcrossQueries) {
  // Do NOT enable the experiment — TearDown() guarantees the state is clear.
  ASSERT_FALSE(memgraph::flags::AreExperimentsEnabled(memgraph::flags::Experiments::HOT_COLD_TENANTS));

  const std::string db_name = "tenant_c3";
  CreateAndPopulate(db_name, 1);

  auto interpreter = min_mg->NewInterpreter();
  interpreter.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);

  EXPECT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value())
      << "flag OFF: db_acc_ must be pinned immediately after SetCurrentDB";
  // Negative assertion proving the flag-OFF branch (not the connection-scoped flag-ON branch) was
  // taken: the flag-OFF SetCurrentDB path uses Get() and never populates current_db_name_, whereas
  // the flag-ON path sets it (see C2). If the flag-OFF path accidentally took the flag-ON branch,
  // current_db_name_ would be engaged here. This is what makes C3 a meaningful byte-identity check.
  EXPECT_FALSE(interpreter.interpreter.current_db_.current_db_name_.has_value())
      << "flag OFF: current_db_name_ must stay unset (flag-ON-only session identity)";

  {
    auto [stream, qid] = interpreter.Prepare("RETURN 42");
    interpreter.Pull(&stream);
  }

  EXPECT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value())
      << "flag OFF: db_acc_ must persist across queries (byte-identical to master)";
  EXPECT_FALSE(interpreter.interpreter.current_db_.current_db_name_.has_value())
      << "flag OFF: current_db_name_ must remain unset across queries";

  auto suspend_res = DBMS().Suspend(db_name);
  ASSERT_FALSE(suspend_res.has_value()) << "flag OFF: Suspend must fail while a session accessor is held";
  EXPECT_EQ(suspend_res.error(), memgraph::dbms::DbmsHandler::SuspendError::ACTIVE_CONNECTIONS);
}

// ---------------------------------------------------------------------------
// C4 (flag ON): releasing the session (destroying the interpreter) drops the
//               accessor, dropping the gatekeeper count to sole-accessor — only
//               then is the tenant suspendable. This is the connection-scoped
//               eviction reach: a tenant is reclaimed when it has zero sessions.
// ---------------------------------------------------------------------------

TEST_F(HotColdConnectionScopedAccessorTest, ReleasingSessionMakesTenantSuspendable) {
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::HOT_COLD_TENANTS);

  const std::string db_name = "tenant_c4";
  CreateAndPopulate(db_name, 2);

  // A live session pins the tenant HOT.
  std::optional<InterpreterFaker> session{std::in_place, &min_mg->interpreter_context, DBMS().Get()};
  session->interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);
  ASSERT_TRUE(session->interpreter.current_db_.db_acc_.has_value());

  {
    auto suspend_held = DBMS().Suspend(db_name);
    ASSERT_FALSE(suspend_held.has_value()) << "Suspend must fail while the session holds the accessor";
    EXPECT_EQ(suspend_held.error(), memgraph::dbms::DbmsHandler::SuspendError::ACTIVE_CONNECTIONS);
  }

  // Release the session: deregister it, then destroy it (drops db_acc_).
  min_mg->interpreter_context.interpreters.WithLock(
      [&](auto &interpreters) { interpreters.erase(&session->interpreter); });
  session.reset();

  // With zero connections, the tenant is now suspendable.
  auto suspend_free = DBMS().Suspend(db_name);
  EXPECT_TRUE(suspend_free.has_value()) << "Suspend must succeed once no session holds the accessor";
}

// ---------------------------------------------------------------------------
// C5 (flag ON): explicit BEGIN..COMMIT keeps the accessor engaged throughout and
//               after COMMIT (connection-scoped); suspend fails while connected.
// ---------------------------------------------------------------------------

TEST_F(HotColdConnectionScopedAccessorTest, ExplicitTransactionHoldsAccessorThroughCommit) {
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::HOT_COLD_TENANTS);

  const std::string db_name = "tenant_c5";
  CreateAndPopulate(db_name, 2);

  auto interpreter = min_mg->NewInterpreter();
  interpreter.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);

  {
    auto [stream, qid] = interpreter.Prepare("BEGIN");
    interpreter.Pull(&stream);
  }
  EXPECT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value()) << "db_acc_ must be engaged after BEGIN";

  {
    auto [stream, qid] = interpreter.Prepare("RETURN 1");
    interpreter.Pull(&stream, {}, qid);
  }
  EXPECT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value())
      << "db_acc_ must remain engaged during the explicit transaction";

  {
    auto [stream, qid] = interpreter.Prepare("COMMIT");
    interpreter.Pull(&stream);
  }
  EXPECT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value())
      << "db_acc_ must STILL be held after COMMIT (connection-scoped)";

  // Suspend must fail — the session still holds the accessor after COMMIT.
  auto suspend_res = DBMS().Suspend(db_name);
  ASSERT_FALSE(suspend_res.has_value()) << "Suspend must fail while the session is connected";
  EXPECT_EQ(suspend_res.error(), memgraph::dbms::DbmsHandler::SuspendError::ACTIVE_CONNECTIONS);
}

// ---------------------------------------------------------------------------
// C6 (flag ON): explicit BEGIN..ROLLBACK rolls data back; the accessor stays
//               engaged (connection-scoped) and the ROLLBACK does not crash
//               (Fix 2 null-guard on the ROLLBACK metric path).
// ---------------------------------------------------------------------------

TEST_F(HotColdConnectionScopedAccessorTest, ExplicitTransactionRollbackHoldsAccessorAndRollsBack) {
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::HOT_COLD_TENANTS);

  const std::string db_name = "tenant_c6";
  CreateAndPopulate(db_name, 2);

  auto interpreter = min_mg->NewInterpreter();
  interpreter.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);

  {
    auto [stream, qid] = interpreter.Prepare("BEGIN");
    interpreter.Pull(&stream);
  }
  {
    auto [stream, qid] = interpreter.Prepare("CREATE (n:RollbackMe)");
    interpreter.Pull(&stream, {}, qid);
  }
  EXPECT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value())
      << "db_acc_ must remain engaged during the explicit transaction";

  // ROLLBACK must not throw/crash (Fix 2 null-guard). Hoisted into a lambda because the
  // structured-binding comma would otherwise be parsed as a macro-argument separator.
  auto do_rollback = [&]() {
    auto [stream, qid] = interpreter.Prepare("ROLLBACK");
    interpreter.Pull(&stream);
  };
  ASSERT_NO_THROW(do_rollback()) << "ROLLBACK must not throw/crash (Fix 2 null-guard)";

  // Accessor still held (connection-scoped).
  EXPECT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value())
      << "db_acc_ must remain held after ROLLBACK (connection-scoped)";

  // The rolled-back CREATE must not be visible — query on the same (HOT) session.
  {
    auto [stream, qid] = interpreter.Prepare("MATCH (n:RollbackMe) RETURN count(n) AS c");
    interpreter.Pull(&stream);
    const auto &results = stream.GetResults();
    ASSERT_EQ(results.size(), 1U);
    EXPECT_EQ(results[0][0].ValueInt(), 0) << "Rolled-back CREATE must not be visible after ROLLBACK";
  }
}

// ---------------------------------------------------------------------------
// C7 (flag ON): block-and-resume. A COLD (suspended) tenant is reheated
//               synchronously when a new session calls SetCurrentDB — no
//               "resuming" exception. The query then reads the original data.
// ---------------------------------------------------------------------------

TEST_F(HotColdConnectionScopedAccessorTest, SetCurrentDBBlockResumesColdTenant) {
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::HOT_COLD_TENANTS);

  const std::string db_name = "tenant_c7";
  CreateAndPopulate(db_name, 4);

  // No session holds the freshly-created tenant -> suspendable.
  auto suspend_res = DBMS().Suspend(db_name);
  ASSERT_TRUE(suspend_res.has_value()) << "Suspend must succeed for a tenant with no sessions";

  // A new session attaches to the COLD tenant — SetCurrentDB block-and-resumes it (no exception).
  auto interpreter = min_mg->NewInterpreter();
  ASSERT_NO_THROW(interpreter.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false))
      << "SetCurrentDB on a COLD tenant must block-and-resume, not throw";
  EXPECT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value())
      << "db_acc_ must be held (HOT) after block-and-resume";

  // The resumed tenant must expose the original 4 nodes.
  {
    auto [stream, qid] = interpreter.Prepare("MATCH (n) RETURN count(n) AS c");
    interpreter.Pull(&stream);
    const auto &results = stream.GetResults();
    ASSERT_EQ(results.size(), 1U);
    EXPECT_EQ(results[0][0].ValueInt(), 4) << "Post-resume query must read the original 4 nodes";
  }
}

// ---------------------------------------------------------------------------
// C8 (flag ON): nested BEGIN (BEGIN while already in an explicit txn) throws
//               ExplicitTransactionUsageException at Pull and leaves the
//               original transaction intact, with the accessor still held.
// ---------------------------------------------------------------------------

TEST_F(HotColdConnectionScopedAccessorTest, NestedBeginThrowsAndPreservesTransaction) {
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::HOT_COLD_TENANTS);

  const std::string db_name = "tenant_c8";
  CreateAndPopulate(db_name, 2);

  auto interpreter = min_mg->NewInterpreter();
  interpreter.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);

  {
    auto [stream, qid] = interpreter.Prepare("BEGIN");
    interpreter.Pull(&stream);
  }
  EXPECT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value()) << "db_acc_ must be engaged after outer BEGIN";
  EXPECT_TRUE(interpreter.interpreter.in_explicit_transaction_)
      << "in_explicit_transaction_ must be true after outer BEGIN";

  {
    auto [stream, qid] = interpreter.Prepare("BEGIN");
    EXPECT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value())
        << "db_acc_ must still be held through the nested-BEGIN Prepare path";
    EXPECT_THROW(interpreter.Pull(&stream), memgraph::query::ExplicitTransactionUsageException)
        << "Nested BEGIN must throw ExplicitTransactionUsageException at Pull";
  }

  EXPECT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value())
      << "db_acc_ must still be held after a failed nested BEGIN";
  EXPECT_TRUE(interpreter.interpreter.in_explicit_transaction_)
      << "in_explicit_transaction_ must remain true after a failed nested BEGIN";

  // The original transaction must still be usable.
  {
    auto [stream, qid] = interpreter.Prepare("RETURN 1 AS alive");
    interpreter.Pull(&stream, {}, qid);
    const auto &results = stream.GetResults();
    ASSERT_EQ(results.size(), 1U);
    EXPECT_EQ(results[0][0].ValueInt(), 1)
        << "Original explicit transaction must remain usable after nested-BEGIN failure";
  }

  {
    auto [stream, qid] = interpreter.Prepare("COMMIT");
    interpreter.Pull(&stream);
  }
  EXPECT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value())
      << "db_acc_ must remain held after COMMIT (connection-scoped)";
}

#endif  // MG_ENTERPRISE
