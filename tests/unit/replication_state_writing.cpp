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

#include "gtest/gtest.h"

#include "io/network/endpoint.hpp"
#include "replication/config.hpp"
#include "replication/state.hpp"

using memgraph::io::network::Endpoint;
using memgraph::replication::ReplicationServerConfig;
using memgraph::replication::ReplicationState;

// A main-role instance that is part of an HA cluster keys write acceptance off the writing_enabled_ bit, which is
// toggled through Enable/DisableWritingOnMain.
TEST(ReplicationStateWriting, MainWritingToggle) {
  ReplicationState repl_state{/*durability_dir=*/std::nullopt, /*ha_cluster=*/true};

  // A freshly constructed main comes up non-writeable until the coordinator enables writing.
  ASSERT_TRUE(repl_state.IsMain());
  EXPECT_FALSE(repl_state.IsMainWriteable());

  EXPECT_TRUE(repl_state.EnableWritingOnMain());
  EXPECT_TRUE(repl_state.IsMainWriteable());

  EXPECT_TRUE(repl_state.DisableWritingOnMain());
  EXPECT_FALSE(repl_state.IsMainWriteable());

  // Toggling is idempotent and reversible.
  EXPECT_TRUE(repl_state.EnableWritingOnMain());
  EXPECT_TRUE(repl_state.IsMainWriteable());
}

// Outside of an HA cluster the writing_enabled_ bit is irrelevant: a standalone main is always writeable.
TEST(ReplicationStateWriting, StandaloneMainAlwaysWriteable) {
  ReplicationState repl_state{/*durability_dir=*/std::nullopt, /*ha_cluster=*/false};

  ASSERT_TRUE(repl_state.IsMain());
  EXPECT_TRUE(repl_state.IsMainWriteable());

  EXPECT_TRUE(repl_state.DisableWritingOnMain());
  EXPECT_TRUE(repl_state.IsMainWriteable());
}

// On a replica there is no writing state to toggle; the operations are no-ops and the instance is never writeable.
TEST(ReplicationStateWriting, ReplicaNeverWriteable) {
  ReplicationState repl_state{/*durability_dir=*/std::nullopt, /*ha_cluster=*/true};

  ASSERT_TRUE(repl_state.SetReplicationRoleReplica(ReplicationServerConfig{.repl_server = Endpoint("127.0.0.1", 30112)},
                                                   /*maybe_main_uuid=*/std::nullopt));
  ASSERT_TRUE(repl_state.IsReplica());

  EXPECT_FALSE(repl_state.IsMainWriteable());
  EXPECT_FALSE(repl_state.EnableWritingOnMain());
  EXPECT_FALSE(repl_state.DisableWritingOnMain());
  EXPECT_FALSE(repl_state.IsMainWriteable());
}
