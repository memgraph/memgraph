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

#include <filesystem>
#include <optional>
#include <string>

#include "auth/auth.hpp"
#include "auth/auth_layer.hpp"
#include "license/license.hpp"
#include "utils/file.hpp"
#include "utils/resource_monitoring.hpp"

namespace fs = std::filesystem;
using memgraph::auth::Auth;
using memgraph::auth::AuthLayer;
using memgraph::auth::SynchedAuth;

class AuthLayerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    memgraph::utils::EnsureDir(test_folder_);
    memgraph::license::global_license_checker.EnableTesting();
#ifdef MG_ENTERPRISE
    auth_.emplace(test_folder_ / "auth", Auth::Config{}, &resources_);
#else
    auth_.emplace(test_folder_ / "auth", Auth::Config{});
#endif
    layer_.emplace(*auth_);
  }

  void TearDown() override {
    layer_.reset();
    auth_.reset();
    fs::remove_all(test_folder_);
  }

  fs::path test_folder_{fs::temp_directory_path() / "MG_tests_unit_auth_layer"};
#ifdef MG_ENTERPRISE
  memgraph::utils::ResourceMonitoring resources_;
#endif
  std::optional<SynchedAuth> auth_;
  std::optional<AuthLayer> layer_;
};

TEST_F(AuthLayerTest, WritesOutsideATransactionAreImmediatelyDurable) {
  {
    ASSERT_TRUE(layer_->Lock()->AddUser("alice").has_value());
  }
  EXPECT_TRUE(layer_->Lock()->HasUser("alice"));
}

TEST_F(AuthLayerTest, TransactionalWritesAreInvisibleUntilCommit) {
  memgraph::auth::AuthTransaction tx;
  {
    ASSERT_TRUE(layer_->Lock(&tx)->AddUser("alice").has_value());
  }

  // A separate, non-transactional read must not see the buffered user.
  EXPECT_FALSE(layer_->Lock()->HasUser("alice"));

  ASSERT_TRUE(layer_->Commit(tx));
  EXPECT_TRUE(layer_->Lock()->HasUser("alice"));
}

TEST_F(AuthLayerTest, TransactionSeesItsOwnWrites) {
  memgraph::auth::AuthTransaction tx;
  {
    ASSERT_TRUE(layer_->Lock(&tx)->AddUser("alice").has_value());
  }
  EXPECT_TRUE(layer_->Lock(&tx)->HasUser("alice"));
}

TEST_F(AuthLayerTest, AbandonedTransactionLeavesNothingBehind) {
  {
    memgraph::auth::AuthTransaction tx;
    ASSERT_TRUE(layer_->Lock(&tx)->AddUser("alice").has_value());
  }
  EXPECT_FALSE(layer_->Lock()->HasUser("alice"));
}

TEST_F(AuthLayerTest, StorageIsRestoredAfterEachTransactionalCall) {
  memgraph::auth::AuthTransaction tx;
  {
    ASSERT_TRUE(layer_->Lock(&tx)->AddUser("alice").has_value());
  }

  // Auth must be back on durable storage between calls, so an unrelated write lands on disk immediately.
  {
    ASSERT_TRUE(layer_->Lock()->AddUser("bob").has_value());
  }
  EXPECT_TRUE(layer_->Lock()->HasUser("bob"));

  ASSERT_TRUE(layer_->Commit(tx));
  EXPECT_TRUE(layer_->Lock()->HasUser("alice"));
}

TEST_F(AuthLayerTest, ConflictingCommitLeavesStorageUntouched) {
  {
    ASSERT_TRUE(layer_->Lock()->AddUser("alice").has_value());
  }

  memgraph::auth::AuthTransaction tx;
  // Read alice into the read-set, then modify her outside the transaction.
  {
    ASSERT_TRUE(layer_->Lock(&tx)->GetUser("alice").has_value());
  }
  {
    auto locked = layer_->Lock();
    auto user = locked->GetUser("alice");
    ASSERT_TRUE(user);
    locked->UpdatePassword(*user, "changed");
    locked->SaveUser(*user);
  }
  {
    ASSERT_TRUE(layer_->Lock(&tx)->AddUser("bob").has_value());
  }

  EXPECT_FALSE(layer_->Commit(tx));
  EXPECT_FALSE(layer_->Lock()->HasUser("bob"));
}

TEST_F(AuthLayerTest, TransactionalWritesDoNotMoveTheEpochUntilCommit) {
  // UpToDate reports whether the epoch still matches, and syncs the caller's copy. A session holding this would
  // otherwise refresh its permission cache against state that is not durable.
  Auth::Epoch seen;
  layer_->Lock()->UpToDate(seen);

  memgraph::auth::AuthTransaction tx;
  {
    ASSERT_TRUE(layer_->Lock(&tx)->AddUser("alice").has_value());
  }
  {
    ASSERT_TRUE(layer_->Lock(&tx)->AddUser("bob").has_value());
  }
  EXPECT_TRUE(layer_->Lock()->UpToDate(seen));

  ASSERT_TRUE(layer_->Commit(tx));
  EXPECT_FALSE(layer_->Lock()->UpToDate(seen));
}

TEST_F(AuthLayerTest, TransactionalWritesCollectActionsInsteadOfReplicatingImmediately) {
  // Without a transaction there is nowhere to put the action and no system transaction to take it, so it is
  // dropped. Inside one it is held until COMMIT drains it.
  {
    ASSERT_TRUE(layer_->Lock()->AddUser("alice").has_value());
  }

  memgraph::auth::AuthTransaction tx;
  EXPECT_TRUE(tx.pending_actions().empty());

  {
    ASSERT_TRUE(layer_->Lock(&tx)->AddUser("bob").has_value());
  }
  EXPECT_EQ(tx.pending_actions().size(), 1);

  {
    ASSERT_TRUE(layer_->Lock(&tx)->AddUser("carol").has_value());
  }
  EXPECT_EQ(tx.pending_actions().size(), 2);
}

TEST_F(AuthLayerTest, TheSinkIsUnboundOutsideTheTransactionsOwnCalls) {
  // A write on another session while a transaction is open must not land in that transaction's list.
  memgraph::auth::AuthTransaction tx;
  {
    ASSERT_TRUE(layer_->Lock(&tx)->AddUser("bob").has_value());
  }
  ASSERT_EQ(tx.pending_actions().size(), 1);

  {
    ASSERT_TRUE(layer_->Lock()->AddUser("alice").has_value());
  }
  EXPECT_EQ(tx.pending_actions().size(), 1);
}

#ifdef MG_ENTERPRISE
TEST_F(AuthLayerTest, DroppingAUserInATransactionHoldsItsResourcesUntilCommit) {
  // ResourceMonitoring is process-wide and has no rollback, so the release waits for the flush. GetUser creates on
  // miss, so presence is observed through the map's own reference rather than by looking the user up again.
  {
    ASSERT_TRUE(layer_->Lock()->AddUser("alice").has_value());
  }
  auto held = resources_.GetUser("alice");
  ASSERT_EQ(held.use_count(), 2);  // ours, and the map's

  memgraph::auth::AuthTransaction tx;
  {
    ASSERT_TRUE(layer_->Lock(&tx)->RemoveUser("alice"));
  }
  EXPECT_EQ(held.use_count(), 2) << "resources released before COMMIT";
  EXPECT_EQ(tx.dropped_users().size(), 1);

  ASSERT_TRUE(layer_->Commit(tx));
  EXPECT_EQ(held.use_count(), 1) << "resources still held after COMMIT";
}

TEST_F(AuthLayerTest, AbandoningATransactionLeavesDroppedUsersResourcesIntact) {
  {
    ASSERT_TRUE(layer_->Lock()->AddUser("alice").has_value());
  }
  auto held = resources_.GetUser("alice");
  ASSERT_EQ(held.use_count(), 2);

  {
    memgraph::auth::AuthTransaction tx;
    ASSERT_TRUE(layer_->Lock(&tx)->RemoveUser("alice"));
  }  // never committed

  EXPECT_EQ(held.use_count(), 2);
  EXPECT_TRUE(layer_->Lock()->HasUser("alice"));
}
#endif
