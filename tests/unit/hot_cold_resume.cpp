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

// Unit tests for the hot/cold RESUME engine (DbmsHandler::Resume_).
//
// C5 is the NODE-LOCAL synchronous resume engine: single-flight via the gatekeeper, off-lock
// recovery, pre-publish (on_resume_) arm, and the COLD-access query seam (Get() on a suspended
// tenant errors with an actionable message). Resume is exercised directly here; the interpreter
// query-seam caller is wired in the same commit but verified via the e2e suite later.
//
// Coverage:
//   ResumeNonExistentRejected           — resuming an absent tenant returns NON_EXISTENT
//   ResumeAlreadyHotReturnsAccessor     — resuming a HOT tenant is a no-op that shares its accessor
//   SuspendResumeRoundTripRecoversData  — data survives a HOT -> COLD -> HOT cycle (WAL replay)
//   ColdAccessThrowsSuspendedError      — Get() on a COLD tenant throws the suspended-seam message
//   OnResumePrePublishArmRuns           — the pre-publish arm fires exactly once on a real resume
//   OnResumeFailureStaysColdRetriable   — a throwing pre-publish arm rolls back to COLD; retriable
//   ConcurrentResumeSingleFlight        — concurrent resumers rebuild once; all observe HOT

#ifdef MG_ENTERPRISE

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "dbms/constants.hpp"
#include "dbms/dbms_handler.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"

namespace fs = std::filesystem;
using memgraph::dbms::DbmsHandler;

// Per-test-binary storage root.
static fs::path g_storage_root{fs::temp_directory_path() / "MG_test_unit_hot_cold_resume"};

class HotColdResume : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = g_storage_root / ::testing::UnitTest::GetInstance()->current_test_info()->name();
    fs::remove_all(test_dir_);
    fs::create_directories(test_dir_);

    memgraph::storage::Config conf;
    memgraph::storage::UpdatePaths(conf, test_dir_);
    conf.durability.snapshot_wal_mode =
        memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
    conf.durability.recover_on_startup = true;
    handler_ = std::make_unique<DbmsHandler>(conf);
  }

  void TearDown() override {
    handler_.reset();
    fs::remove_all(test_dir_);
  }

  std::string CreateTenant(std::string name) {
    auto result = handler_->New(name);
    EXPECT_TRUE(result.has_value()) << "Failed to create tenant: " << name;
    return name;
  }

  // Create a tenant and write `n` vertices through a real storage accessor, so the suspend teardown
  // persists content the resume must replay.
  std::string CreateAndPopulate(std::string name, int n) {
    CreateTenant(name);
    auto db_acc = handler_->Get(name);
    auto storage_acc = db_acc->Access(memgraph::storage::WRITE);
    for (int i = 0; i < n; ++i) storage_acc->CreateVertex();
    EXPECT_TRUE(storage_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    return name;
  }

  static int64_t CountNodes(memgraph::dbms::DatabaseAccess &db_acc) {
    auto storage_acc = db_acc->Access(memgraph::storage::READ);
    int64_t count = 0;
    for ([[maybe_unused]] auto _ : storage_acc->Vertices(memgraph::storage::View::OLD)) ++count;
    return count;
  }

  bool InAll(std::string_view name) {
    auto all = handler_->All();
    return std::find(all.begin(), all.end(), name) != all.end();
  }

  fs::path test_dir_;
  std::unique_ptr<DbmsHandler> handler_;
};

// Resuming a tenant that was never created (nor suspended) returns NON_EXISTENT.
TEST_F(HotColdResume, ResumeNonExistentRejected) {
  auto result = handler_->Resume("never_existed");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error(), DbmsHandler::ResumeError::NON_EXISTENT);
}

// Resuming an already-HOT tenant is a no-op: Resume_ races to Get() and returns its live accessor.
TEST_F(HotColdResume, ResumeAlreadyHotReturnsAccessor) {
  auto name = CreateTenant("already_hot");
  auto result = handler_->Resume(name);
  ASSERT_TRUE(result.has_value()) << "Resuming a HOT tenant must return its accessor, not error";
  EXPECT_EQ(result.value()->name(), name);
  EXPECT_TRUE(InAll(name));
}

// The core round-trip: data written before a suspend must be present after the resume (WAL replay).
TEST_F(HotColdResume, SuspendResumeRoundTripRecoversData) {
  constexpr int kNodes = 7;
  auto name = CreateAndPopulate("round_trip", kNodes);

  ASSERT_TRUE(handler_->Suspend(name).has_value());
  EXPECT_FALSE(InAll(name)) << "Tenant must be COLD after suspend";

  auto result = handler_->Resume(name);
  ASSERT_TRUE(result.has_value()) << "Resume of a populated COLD tenant must succeed";
  EXPECT_TRUE(InAll(name)) << "Tenant must be HOT again after resume";

  auto acc = result.value();
  EXPECT_EQ(CountNodes(acc), kNodes) << "All vertices must survive the HOT -> COLD -> HOT cycle";

  // The fresh accessor is independently usable via Get().
  auto via_get = handler_->Get(name);
  EXPECT_EQ(CountNodes(via_get), kNodes);
}

// Cold-access query seam: once suspended, Get() throws (it is no longer HOT). The message must point
// the user at RESUME rather than claim the database is unknown.
TEST_F(HotColdResume, ColdAccessThrowsSuspendedError) {
  auto name = CreateTenant("cold_access");
  ASSERT_TRUE(handler_->Suspend(name).has_value());

  EXPECT_THROW(
      {
        try {
          handler_->Get(name);
        } catch (const std::exception &e) {
          EXPECT_THAT(e.what(), ::testing::HasSubstr("suspended"));
          throw;
        }
      },
      std::exception);
}

// The pre-publish arm (on_resume_) runs exactly once on a real COLD -> HOT resume.
TEST_F(HotColdResume, OnResumePrePublishArmRuns) {
  auto name = CreateTenant("pre_publish");
  ASSERT_TRUE(handler_->Suspend(name).has_value());

  std::atomic<int> arm_calls{0};
  handler_->SetOnResume([&arm_calls](memgraph::dbms::DatabaseAccess) { ++arm_calls; });

  auto result = handler_->Resume(name);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(arm_calls.load(), 1) << "The pre-publish arm must fire exactly once on a real resume";

  handler_->SetOnResume({});
}

// A throwing pre-publish arm aborts the resume (RESUMING -> COLD); the tenant stays COLD and a later
// (non-throwing) resume succeeds.
TEST_F(HotColdResume, OnResumeFailureStaysColdRetriable) {
  auto name = CreateTenant("retriable");
  ASSERT_TRUE(handler_->Suspend(name).has_value());

  std::atomic<bool> should_throw{true};
  handler_->SetOnResume([&should_throw](memgraph::dbms::DatabaseAccess) {
    if (should_throw.load()) throw std::runtime_error("injected pre-publish failure");
  });

  auto failed = handler_->Resume(name);
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error(), DbmsHandler::ResumeError::RECOVERY_FAILED);
  EXPECT_FALSE(InAll(name)) << "A failed resume must leave the tenant COLD";

  // Retry without the injected failure: the tenant must recover.
  should_throw.store(false);
  auto ok = handler_->Resume(name);
  ASSERT_TRUE(ok.has_value()) << "A COLD tenant must be resumable after a transient pre-publish failure";
  EXPECT_TRUE(InAll(name));

  handler_->SetOnResume({});
}

// Concurrent resumers single-flight through the gatekeeper: exactly one thread rebuilds (the
// on_resume_ arm fires once), and every caller observes the published HOT tenant.
TEST_F(HotColdResume, ConcurrentResumeSingleFlight) {
  auto name = CreateTenant("single_flight");
  ASSERT_TRUE(handler_->Suspend(name).has_value());

  std::atomic<int> rebuilds{0};
  handler_->SetOnResume([&rebuilds](memgraph::dbms::DatabaseAccess) {
    // Hold briefly so the losers are guaranteed to enter the poll path while the winner builds.
    ++rebuilds;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  });

  constexpr int kThreads = 8;
  std::atomic<int> successes{0};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&] {
      auto res = handler_->Resume(name);
      if (res.has_value()) ++successes;
    });
  }
  for (auto &t : threads) t.join();

  EXPECT_EQ(rebuilds.load(), 1) << "Exactly one thread must rebuild the storage (single-flight)";
  EXPECT_EQ(successes.load(), kThreads) << "Every concurrent resumer must observe the published HOT tenant";
  EXPECT_TRUE(InAll(name));

  handler_->SetOnResume({});
}

#else

#include <gtest/gtest.h>

TEST(HotColdResume, NotApplicableInCommunity) { GTEST_SKIP() << "hot/cold resume is an enterprise-only feature"; }

#endif  // MG_ENTERPRISE
