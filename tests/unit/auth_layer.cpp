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

namespace fs = std::filesystem;
using memgraph::auth::Auth;
using memgraph::auth::AuthLayer;
using memgraph::auth::SynchedAuth;

class AuthLayerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    memgraph::utils::EnsureDir(test_folder_);
    memgraph::license::global_license_checker.EnableTesting();
    auth_.emplace(test_folder_ / "auth", Auth::Config{});
    layer_.emplace(*auth_);
  }

  void TearDown() override {
    layer_.reset();
    auth_.reset();
    fs::remove_all(test_folder_);
  }

  fs::path test_folder_{fs::temp_directory_path() / "MG_tests_unit_auth_layer"};
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
  AuthLayer::Transaction tx;
  {
    ASSERT_TRUE(layer_->Lock(tx)->AddUser("alice").has_value());
  }

  // A separate, non-transactional read must not see the buffered user.
  EXPECT_FALSE(layer_->Lock()->HasUser("alice"));

  ASSERT_TRUE(layer_->Commit(tx));
  EXPECT_TRUE(layer_->Lock()->HasUser("alice"));
}

TEST_F(AuthLayerTest, TransactionSeesItsOwnWrites) {
  AuthLayer::Transaction tx;
  {
    ASSERT_TRUE(layer_->Lock(tx)->AddUser("alice").has_value());
  }
  EXPECT_TRUE(layer_->Lock(tx)->HasUser("alice"));
}

TEST_F(AuthLayerTest, AbandonedTransactionLeavesNothingBehind) {
  {
    AuthLayer::Transaction tx;
    ASSERT_TRUE(layer_->Lock(tx)->AddUser("alice").has_value());
  }
  EXPECT_FALSE(layer_->Lock()->HasUser("alice"));
}

TEST_F(AuthLayerTest, StorageIsRestoredAfterEachTransactionalCall) {
  AuthLayer::Transaction tx;
  {
    ASSERT_TRUE(layer_->Lock(tx)->AddUser("alice").has_value());
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

  AuthLayer::Transaction tx;
  // Read alice into the read-set, then modify her outside the transaction.
  {
    ASSERT_TRUE(layer_->Lock(tx)->GetUser("alice").has_value());
  }
  {
    auto locked = layer_->Lock();
    auto user = locked->GetUser("alice");
    ASSERT_TRUE(user);
    locked->UpdatePassword(*user, "changed");
    locked->SaveUser(*user);
  }
  {
    ASSERT_TRUE(layer_->Lock(tx)->AddUser("bob").has_value());
  }

  EXPECT_FALSE(layer_->Commit(tx));
  EXPECT_FALSE(layer_->Lock()->HasUser("bob"));
}

TEST_F(AuthLayerTest, TransactionalWritesDoNotMoveTheEpochUntilCommit) {
  // UpToDate reports whether the epoch still matches, and syncs the caller's copy. A session holding this would
  // otherwise refresh its permission cache against state that is not durable.
  Auth::Epoch seen;
  layer_->Lock()->UpToDate(seen);

  AuthLayer::Transaction tx;
  {
    ASSERT_TRUE(layer_->Lock(tx)->AddUser("alice").has_value());
  }
  {
    ASSERT_TRUE(layer_->Lock(tx)->AddUser("bob").has_value());
  }
  EXPECT_TRUE(layer_->Lock()->UpToDate(seen));

  ASSERT_TRUE(layer_->Commit(tx));
  EXPECT_FALSE(layer_->Lock()->UpToDate(seen));
}
