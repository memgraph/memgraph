// Copyright 2025 Memgraph Ltd.
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
   * A BEGIN whose engine-lock acquire would block has been decoded and stashed; its completion is
   * being run out-of-band on a pool worker. Execute_ returns to the dechunk loop's caller without
   * touching any message buffered behind the BEGIN, so ordering is preserved until the BEGIN finishes.
   */
  PendingBegin,

  /**
   * A PREPARE (the Parsed->Result step) whose engine-lock acquire would block has been decoded and parsed; its
   * completion is being run out-of-band on a pool worker. Execute_ returns without touching any message buffered
   * behind the PREPARE, so ordering is preserved until the PREPARE finishes. The parse itself is held in
   * SessionHL::parsed_res_ (re-runnable); the bolt layer only tracks the pending flag + retry count.
   */
  PendingPrepare,

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
};

// Outcome of the pool-side completion of a would-block BEGIN (FinishPendingBegin_).
// Done        - the BEGIN completed and SUCCESS was sent.
// ClientError - send failed or the begin threw; state moved to Close/Error.
// Reschedule  - the bounded-try engine-lock acquire lost the race; extras stay stashed, re-post to the pool.
enum class PendingBeginOutcome : uint8_t { Done, ClientError, Reschedule };

// Outcome of the pool-side completion of a would-block PREPARE (FinishPendingPrepare_).
// Done        - the PREPARE completed and the header SUCCESS was sent.
// ClientError - send failed or the prepare threw; state moved to Close/Error.
// Reschedule  - the bounded-try engine-lock acquire lost the race; parsed_res_ stays intact, re-post to the pool.
enum class PendingPrepareOutcome : uint8_t { Done, ClientError, Reschedule };
}  // namespace memgraph::communication::bolt
