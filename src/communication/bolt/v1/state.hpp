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
   * A coroutine cursor yielded cooperatively inside a PULL handler
   * (COROUTINE_CURSORS ON only). The query is alive; the worker should be
   * rescheduled (pinned resumable task) without sending any response to the
   * client and without waiting for a new client message.  Under
   * COROUTINE_CURSORS OFF this state is never set, so the flag-OFF path is
   * byte-identical to the pre-B5 code.
   */
  Yielding,
};

/**
 * Three-way result of a single Execute_() / Execute() call (Batch B5).
 *
 * NoMoreData  — maps to the old `return false` (no Bolt chunk ready; wait for
 *               the next async read). Used by both flag-ON and flag-OFF paths.
 * MoreData    — maps to the old `return true` (State::Parsed reached;
 *               reschedule with the now-known query priority). Both paths.
 * Yielding    — COROUTINE_CURSORS ON only: a coroutine cursor yielded inside
 *               PULL.  The query is still alive; the DoWork resumable task
 *               must return true (same-worker pinned reschedule) WITHOUT
 *               calling DoRead().  Under COROUTINE_CURSORS OFF this variant
 *               is never produced, so the flag-OFF path is byte-identical.
 */
enum class ExecuteResult : uint8_t {
  NoMoreData,
  MoreData,
  Yielding,
};

}  // namespace memgraph::communication::bolt
