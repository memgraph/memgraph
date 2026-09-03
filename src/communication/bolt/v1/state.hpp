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

#pragma once

#include <cstdint>

namespace memgraph::communication::bolt {

/**
 * This class represents states in execution of the Bolt protocol.
 * It is used only internally in the Session. All functions that run
 * these states can be found in the states/ subdirectory.
 */
enum class State : uint8_t {
  /**
   * This state negotiates a handshake with the client.
   */
  Handshake,

  /**
   * This state initializes the Bolt session.
   */
  Init,

  /**
   * This state waits for next query (RUN command).
   */
  Idle,

  /**
   * Query has been parsed (ast tree and priority), but not prepared (actual plan and storage accessors).
   * Going to Result once query has been prepared.
   */
  Parsed,

  /**
   * This state holds results of RUN command and waits for either PULL_ALL or
   * DISCARD_ALL command.
   */
  Result,

  /**
   * This state handles errors, if client handles error response correctly next
   * state is Idle.
   */
  Error,

  /**
   * This is a 'virtual' state (it doesn't have a run function) which tells
   * the session that the client has sent malformed data and that the
   * session should be closed.
   */
  Close,

  /**
   * A PULL (or DISCARD) whose autocommit Commit() step returned
   * CommitWouldBlockException — commit_mutex_ is held by another writer.
   * The bolt driver parks this pool worker under WaitResource::CommitLock and
   * retries Commit() on wake. Nothing is sent to the client until the commit
   * completes or fails permanently.
   */
  PendingCommit,
};

// Outcome of a single FinishPendingCommit_ attempt: the commit acquired commit_mutex_ and the SUCCESS
// response was sent (Done), must be re-tried (Reschedule), or the connection should be torn down
// (ClientError — encoder write failure or unrecoverable exception). Lives here (not session.hpp) so the
// v2 session driver can name it without pulling in the whole bolt Session header.
enum class PendingCommitOutcome : uint8_t { Reschedule, Done, ClientError };
}  // namespace memgraph::communication::bolt
