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
#include "auth/rpc.hpp"
#include "license/license.hpp"
#include "system/system.hpp"
#include "system/transaction.hpp"
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

  ASSERT_TRUE(layer_->Commit(tx, nullptr));
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

  ASSERT_TRUE(layer_->Commit(tx, nullptr));
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

  EXPECT_FALSE(layer_->Commit(tx, nullptr));
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

  ASSERT_TRUE(layer_->Commit(tx, nullptr));
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

  ASSERT_TRUE(layer_->Commit(tx, nullptr));
  EXPECT_EQ(held.use_count(), 1) << "resources still held after COMMIT";
}

#ifdef MG_ENTERPRISE
// A replica applies a whole auth transaction or none of it. These drive the same entry point the replication
// handler uses, so the batch either lands complete or leaves the store as it was.
TEST_F(AuthLayerTest, ApplyingABatchWritesEveryOperation) {
  std::vector<memgraph::replication::AuthOp> ops;
  ops.emplace_back(memgraph::replication::AuthUpdateOp{memgraph::auth::User{"alice"}});
  ops.emplace_back(memgraph::replication::AuthUpdateOp{memgraph::auth::User{"bob"}});

  ASSERT_TRUE(layer_->ApplyBatch(ops));

  auto locked = layer_->Lock();
  EXPECT_TRUE(locked->HasUser("alice"));
  EXPECT_TRUE(locked->HasUser("bob"));
}

// The ordering case the batch exists for: a transaction may drop a name and recreate it, and the operations
// have to be applied in the order the transaction made them or the record is lost.
TEST_F(AuthLayerTest, ApplyingABatchKeepsOperationOrder) {
  {
    ASSERT_TRUE(layer_->Lock()->AddUser("alice").has_value());
  }

  std::vector<memgraph::replication::AuthOp> ops;
  ops.emplace_back(memgraph::replication::AuthDropOp{memgraph::replication::AuthDataType::USER, "alice"});
  ops.emplace_back(memgraph::replication::AuthUpdateOp{memgraph::auth::User{"alice"}});

  ASSERT_TRUE(layer_->ApplyBatch(ops));
  EXPECT_TRUE(layer_->Lock()->HasUser("alice")) << "the recreate must win, as it came last";

  // And the other way round: created then dropped leaves nothing.
  std::vector<memgraph::replication::AuthOp> reversed;
  reversed.emplace_back(memgraph::replication::AuthUpdateOp{memgraph::auth::User{"carol"}});
  reversed.emplace_back(memgraph::replication::AuthDropOp{memgraph::replication::AuthDataType::USER, "carol"});

  ASSERT_TRUE(layer_->ApplyBatch(reversed));
  EXPECT_FALSE(layer_->Lock()->HasUser("carol")) << "the drop must win, as it came last";
}

// A drop naming something the replica does not have is ordinary, not a failure: the main may have created and
// dropped a record between snapshots. Failing the batch here would force needless snapshot recoveries.
TEST_F(AuthLayerTest, ABatchToleratesADropThatNamesNothing) {
  std::vector<memgraph::replication::AuthOp> ops;
  ops.emplace_back(memgraph::replication::AuthUpdateOp{memgraph::auth::User{"alice"}});
  ops.emplace_back(memgraph::replication::AuthDropOp{memgraph::replication::AuthDataType::ROLE, "no_such_role"});

  EXPECT_TRUE(layer_->ApplyBatch(ops));
  EXPECT_TRUE(layer_->Lock()->HasUser("alice")) << "the rest of the batch must still apply";
}

// The same tolerance for a profile, which takes a different path: profiles are applied outside the overlay, and
// the applier distinguishes a profile that was never there from a delete that failed. Only the second is a
// failure.
TEST_F(AuthLayerTest, ABatchToleratesAProfileDropThatNamesNothing) {
  std::vector<memgraph::replication::AuthOp> ops;
  ops.emplace_back(memgraph::replication::AuthUpdateOp{memgraph::auth::User{"alice"}});
  ops.emplace_back(memgraph::replication::AuthDropOp{memgraph::replication::AuthDataType::PROFILE, "no_such_profile"});

  EXPECT_TRUE(layer_->ApplyBatch(ops));
  EXPECT_TRUE(layer_->Lock()->HasUser("alice")) << "the rest of the batch must still apply";
}

// The point of batching: an operation that throws part-way leaves the store exactly as it was, so a replica
// never holds a prefix of a transaction. An empty username is rejected by User construction downstream.
TEST_F(AuthLayerTest, AThrowingBatchLeavesTheStoreUntouched) {
  {
    ASSERT_TRUE(layer_->Lock()->AddUser("existing").has_value());
  }

  std::vector<memgraph::replication::AuthOp> ops;
  ops.emplace_back(memgraph::replication::AuthUpdateOp{memgraph::auth::User{"alice"}});
  ops.emplace_back(memgraph::replication::AuthUpdateOp{});  // carries no record: the applier cannot honour it

  EXPECT_FALSE(layer_->ApplyBatch(ops)) << "the batch must report failure so the main marks the replica behind";

  auto locked = layer_->Lock();
  EXPECT_FALSE(locked->HasUser("alice")) << "a failed batch must not leave its earlier operations behind";
  EXPECT_TRUE(locked->HasUser("existing")) << "a failed batch must not disturb what was already there";
}
#endif

TEST_F(AuthLayerTest, RecreatingADroppedUserKeepsItsResources) {
  {
    ASSERT_TRUE(layer_->Lock()->AddUser("alice").has_value());
  }
  auto held = resources_.GetUser("alice");
  ASSERT_EQ(held.use_count(), 2);

  memgraph::auth::AuthTransaction tx;
  {
    auto locked = layer_->Lock(&tx);
    ASSERT_TRUE(locked->RemoveUser("alice"));
    ASSERT_TRUE(locked->AddUser("alice").has_value());
  }

  ASSERT_TRUE(layer_->Commit(tx, nullptr));
  EXPECT_TRUE(layer_->Lock()->HasUser("alice"));
  EXPECT_EQ(held.use_count(), 2) << "resources released for a user the transaction recreated";
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

TEST_F(AuthLayerTest, CommitBatchesCollectedOperationsIntoOneAction) {
  memgraph::system::System system;
  auto system_tx = system.TryCreateTransaction();
  ASSERT_TRUE(system_tx);

  memgraph::auth::AuthTransaction tx;
  {
    ASSERT_TRUE(layer_->Lock(&tx)->AddUser("alice").has_value());
  }
  {
    ASSERT_TRUE(layer_->Lock(&tx)->AddUser("bob").has_value());
  }
  ASSERT_EQ(tx.pending_actions().size(), 2);

  ASSERT_TRUE(layer_->Commit(tx, &*system_tx));
  EXPECT_TRUE(tx.pending_actions().empty()) << "actions left behind after the drain";

  // Commit reports AllCommitsConfirmed and aborts when it holds nothing, so a transaction that received the
  // action is distinguishable from one that did not. Counting them also pins the batching: two statements
  // must arrive as one action, or a replica could apply the first without the second.
  struct NoopHandler {
    memgraph::system::AllSyncReplicaStatus ApplyAction(memgraph::system::ISystemAction const & /*action*/,
                                                       memgraph::system::Transaction const & /*txn*/) {
      ++applied;
      return memgraph::system::AllSyncReplicaStatus::AllCommitsConfirmed;
    }

    memgraph::system::AllSyncReplicaStatus FinalizeTransaction(memgraph::system::Transaction const & /*txn*/) {
      return memgraph::system::AllSyncReplicaStatus::AllCommitsConfirmed;
    }

    int &applied;
  };

  int applied = 0;
  system_tx->Commit(NoopHandler{applied});
  EXPECT_EQ(applied, 1) << "a transaction replicates as one batched action, however many statements it ran";
}

TEST_F(AuthLayerTest, AConflictingCommitLeavesTheSystemTransactionEmpty) {
  {
    ASSERT_TRUE(layer_->Lock()->AddUser("alice").has_value());
  }

  memgraph::auth::AuthTransaction tx;
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
  ASSERT_FALSE(tx.pending_actions().empty());

  memgraph::system::System system;
  auto system_tx = system.TryCreateTransaction();
  ASSERT_TRUE(system_tx);

  EXPECT_FALSE(layer_->Commit(tx, &*system_tx));
  EXPECT_FALSE(tx.pending_actions().empty()) << "a conflicted transaction must keep its actions undrained";
}

TEST_F(AuthLayerTest, AReadGuardKeepsTheOverlayInstalled) {
  // ReadLock moves its ScopedOverlay into a variant. A defaulted move would leave the source's saved storage
  // engaged, so the temporary's destructor would restore durable storage while the surviving guard still reads
  // through it, and every transactional read would silently miss the transaction's own writes.
  memgraph::auth::AuthTransaction tx;
  {
    ASSERT_TRUE(layer_->Lock(&tx)->AddUser("alice").has_value());
  }
  {
    auto reader = layer_->ReadLock(&tx);
    EXPECT_TRUE(reader->GetUser("alice").has_value()) << "read guard lost the overlay";
  }
  EXPECT_FALSE(layer_->Lock()->HasUser("alice")) << "the write escaped the transaction";
}

TEST_F(AuthLayerTest, AListedUserSetIsNotInvalidatedByAConcurrentCreate) {
  // The end-to-end sequence, through the calls a real session makes rather than the repository directly:
  // SHOW USERS lists the users, another session creates one, then CREATE USER runs HasUsers() on the way in.
  // The transaction concluded something from the list it read, so a new user appearing under that prefix has to
  // conflict it.
  {
    ASSERT_TRUE(layer_->Lock()->AddUser("bob").has_value());
  }

  memgraph::auth::AuthTransaction tx;
  {
    auto listed = layer_->ReadLock(&tx)->AllUsers();
    ASSERT_EQ(listed.size(), 1);
  }

  // Another session, outside the transaction.
  {
    ASSERT_TRUE(layer_->Lock()->AddUser("alice").has_value());
  }

  {
    // HasUsers() is what CreateUser calls first, and it re-scans the same prefix.
    auto locked = layer_->Lock(&tx);
    ASSERT_TRUE(locked->HasUsers());
    ASSERT_TRUE(locked->AddUser("carol").has_value());
  }

  EXPECT_FALSE(layer_->Commit(tx, nullptr)) << "committed on a user list that had already changed";
  EXPECT_FALSE(layer_->Lock()->HasUser("carol"));
}

TEST_F(AuthLayerTest, AReadOnlyTransactionDoesNotSpendTheEpoch) {
  // The epoch is what tells every session its cached permissions are stale. A transaction that only read has
  // published nothing for them to re-read, so moving it would cost every session a refresh for no change.
  {
    ASSERT_TRUE(layer_->Lock()->AddUser("alice").has_value());
  }

  Auth::Epoch seen;
  layer_->Lock()->UpToDate(seen);

  memgraph::auth::AuthTransaction reader;
  {
    EXPECT_TRUE(layer_->ReadLock(&reader)->HasUsers());
  }
  ASSERT_TRUE(layer_->Commit(reader, nullptr));
  EXPECT_TRUE(layer_->Lock()->UpToDate(seen)) << "a read-only transaction invalidated every session's cache";

  // A transaction that did write still moves it.
  memgraph::auth::AuthTransaction writer;
  {
    ASSERT_TRUE(layer_->Lock(&writer)->AddUser("bob").has_value());
  }
  ASSERT_TRUE(layer_->Commit(writer, nullptr));
  EXPECT_FALSE(layer_->Lock()->UpToDate(seen));
}

TEST_F(AuthLayerTest, ARepeatedListingDoesNotAdoptAConcurrentCreate) {
  // Listing twice must not launder a user created in between into the set the transaction is held to. The first
  // exhaustive scan fixes what it depends on; a later one can only confirm a subset of it.
  {
    ASSERT_TRUE(layer_->Lock()->AddUser("bob").has_value());
  }

  memgraph::auth::AuthTransaction tx;
  {
    ASSERT_EQ(layer_->ReadLock(&tx)->AllUsers().size(), 1);
  }

  {
    ASSERT_TRUE(layer_->Lock()->AddUser("alice").has_value());
  }

  {
    // The transaction lists again, now seeing alice, and then writes on the strength of what it has read.
    auto locked = layer_->Lock(&tx);
    ASSERT_EQ(locked->AllUsers().size(), 2);
    ASSERT_TRUE(locked->AddUser("carol").has_value());
  }

  EXPECT_FALSE(layer_->Commit(tx, nullptr)) << "committed on a user list that changed under it";
  EXPECT_FALSE(layer_->Lock()->HasUser("carol"));
}
