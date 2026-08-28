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

#include "storage/v2/storage_error.hpp"

#include <gtest/gtest.h>

using memgraph::storage::FormatReplicationError;
using memgraph::storage::ReplicaFailureReason;
using memgraph::storage::ReplicaFailureReasonToString;
using memgraph::storage::ReplicationError;

// --- ReplicaFailureReasonToString ---

TEST(ReplicaFailureReasonToStringTest, AllReasonsProduceNonEmptyStrings) {
  for (auto reason : {ReplicaFailureReason::NOT_IN_SYNC,
                      ReplicaFailureReason::FAILED_TO_GET_LOCK,
                      ReplicaFailureReason::RPC_ERROR,
                      ReplicaFailureReason::DIVERGED,
                      ReplicaFailureReason::TIMEOUT}) {
    EXPECT_FALSE(ReplicaFailureReasonToString(reason).empty()) << "Reason: " << static_cast<int>(reason);
  }
}

// --- FormatReplicationError: single replica ---

TEST(FormatReplicationErrorTest, SingleSyncReplicaNotInSync) {
  ReplicationError error{
      .failures = {{"replica_1", "SYNC", ReplicaFailureReason::NOT_IN_SYNC}},
      .transaction_committed = true,
  };
  auto msg = FormatReplicationError(error);
  EXPECT_NE(msg.find("Failed to replicate to SYNC replica 'replica_1'"), std::string::npos);
  EXPECT_NE(msg.find("not reachable or not in sync"), std::string::npos);
  EXPECT_NE(msg.find("Replica will be recovered automatically."), std::string::npos);
  EXPECT_NE(msg.find("Transaction is still committed on the main"), std::string::npos);
}

TEST(FormatReplicationErrorTest, SingleStrictSyncReplicaAborted) {
  ReplicationError error{
      .failures = {{"replica_1", "STRICT_SYNC", ReplicaFailureReason::RPC_ERROR}},
      .transaction_committed = false,
  };
  auto msg = FormatReplicationError(error);
  EXPECT_NE(msg.find("Failed to replicate to STRICT_SYNC replica 'replica_1'"), std::string::npos);
  EXPECT_NE(msg.find("RPC communication error"), std::string::npos);
  EXPECT_NE(msg.find("Transaction was aborted on all instances."), std::string::npos);
}

TEST(FormatReplicationErrorTest, SingleReplicaDiverged) {
  ReplicationError error{
      .failures = {{"replica_1", "SYNC", ReplicaFailureReason::DIVERGED}},
      .transaction_committed = true,
  };
  auto msg = FormatReplicationError(error);
  EXPECT_NE(msg.find("replica has diverged from main"), std::string::npos);
}

TEST(FormatReplicationErrorTest, SingleReplicaFailedToGetLock) {
  ReplicationError error{
      .failures = {{"replica_1", "ASYNC", ReplicaFailureReason::FAILED_TO_GET_LOCK}},
      .transaction_committed = true,
  };
  auto msg = FormatReplicationError(error);
  EXPECT_NE(msg.find("failed to obtain RPC lock"), std::string::npos);
}

// --- FormatReplicationError: timeout-specific suffix ---

TEST(FormatReplicationErrorTest, SingleTimeoutAppendsTimeoutMessage) {
  ReplicationError error{
      .failures = {{"replica_1", "SYNC", ReplicaFailureReason::TIMEOUT}},
      .transaction_committed = true,
  };
  auto msg = FormatReplicationError(error);
  EXPECT_NE(msg.find("RPC timeout while replicating"), std::string::npos);
  EXPECT_NE(msg.find("Main reached an RPC timeout while replicating to [replica_1]"), std::string::npos);
  EXPECT_NE(msg.find("deltas_batch_progress_size"), std::string::npos);
}

TEST(FormatReplicationErrorTest, NoTimeoutSuffixWhenNoTimeoutFailure) {
  ReplicationError error{
      .failures = {{"replica_1", "SYNC", ReplicaFailureReason::NOT_IN_SYNC}},
      .transaction_committed = true,
  };
  auto msg = FormatReplicationError(error);
  EXPECT_EQ(msg.find("Main reached an RPC timeout"), std::string::npos);
}

// --- FormatReplicationError: multiple replicas ---

TEST(FormatReplicationErrorTest, TwoSyncReplicasSameReasonGrouped) {
  ReplicationError error{
      .failures =
          {
              {"replica_1", "SYNC", ReplicaFailureReason::NOT_IN_SYNC},
              {"replica_2", "SYNC", ReplicaFailureReason::NOT_IN_SYNC},
          },
      .transaction_committed = true,
  };
  auto msg = FormatReplicationError(error);
  EXPECT_NE(msg.find("Failed to replicate to SYNC replicas 'replica_1' and 'replica_2'"), std::string::npos);
  // Plural form
  EXPECT_NE(msg.find("replicas are not reachable"), std::string::npos);
  EXPECT_NE(msg.find("Replicas will be recovered automatically."), std::string::npos);
}

TEST(FormatReplicationErrorTest, ThreeReplicasSameReasonGrouped) {
  ReplicationError error{
      .failures =
          {
              {"r1", "SYNC", ReplicaFailureReason::NOT_IN_SYNC},
              {"r2", "SYNC", ReplicaFailureReason::NOT_IN_SYNC},
              {"r3", "SYNC", ReplicaFailureReason::NOT_IN_SYNC},
          },
      .transaction_committed = true,
  };
  auto msg = FormatReplicationError(error);
  EXPECT_NE(msg.find("'r1', 'r2' and 'r3'"), std::string::npos);
}

TEST(FormatReplicationErrorTest, DifferentReasonsProduceSeparateGroups) {
  ReplicationError error{
      .failures =
          {
              {"replica_1", "SYNC", ReplicaFailureReason::NOT_IN_SYNC},
              {"replica_2", "SYNC", ReplicaFailureReason::TIMEOUT},
          },
      .transaction_committed = true,
  };
  auto msg = FormatReplicationError(error);
  EXPECT_NE(msg.find("Failed to replicate to SYNC replica 'replica_1'"), std::string::npos);
  EXPECT_NE(msg.find("Failed to replicate to SYNC replica 'replica_2'"), std::string::npos);
}

TEST(FormatReplicationErrorTest, DifferentModesProduceSeparateGroups) {
  ReplicationError error{
      .failures =
          {
              {"replica_1", "ASYNC", ReplicaFailureReason::NOT_IN_SYNC},
              {"replica_2", "STRICT_SYNC", ReplicaFailureReason::NOT_IN_SYNC},
          },
      .transaction_committed = false,
  };
  auto msg = FormatReplicationError(error);
  EXPECT_NE(msg.find("ASYNC replica 'replica_1'"), std::string::npos);
  EXPECT_NE(msg.find("STRICT_SYNC replica 'replica_2'"), std::string::npos);
}

// --- FormatReplicationError: plural form for "diverged" ---

TEST(FormatReplicationErrorTest, PluralFormForDiverged) {
  ReplicationError error{
      .failures =
          {
              {"r1", "SYNC", ReplicaFailureReason::DIVERGED},
              {"r2", "SYNC", ReplicaFailureReason::DIVERGED},
          },
      .transaction_committed = true,
  };
  auto msg = FormatReplicationError(error);
  EXPECT_NE(msg.find("replicas have diverged from main"), std::string::npos);
}

// --- FormatReplicationError: multiple timeouts in suffix ---

TEST(FormatReplicationErrorTest, MultipleTimeoutsListedInSuffix) {
  ReplicationError error{
      .failures =
          {
              {"r1", "SYNC", ReplicaFailureReason::TIMEOUT},
              {"r2", "SYNC", ReplicaFailureReason::TIMEOUT},
          },
      .transaction_committed = true,
  };
  auto msg = FormatReplicationError(error);
  EXPECT_NE(msg.find("Main reached an RPC timeout while replicating to [r1, r2]"), std::string::npos);
}
