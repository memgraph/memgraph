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

/**
 * Tri-state outcome of Session::Execute_ (Session-surgery Stage B, IP-1 design doc
 * opencode-work/resource-lock-starvation/coro-prepare/ip1-design.md REVISION 3 §R3.3 / REVISION 4
 * §R4.7). `kNoMoreData`/`kMoreData` are exactly today's `false`/`true` (kept as an enum, not a bool,
 * so the new third state can't be silently misread as either). `kNeedsCoroPrepare` is new: it means
 * the RUN currently being processed (state_ == State::Parsed) needs to be driven via the coroutine
 * Prepare chain (communication::bolt::HandlePrepareCoro) because that path can park a contended
 * accessor acquire -- Execute_ deliberately does NOT invoke HandlePrepare synchronously in that case;
 * the caller (communication::v2::Session::RunLoop) drives HandlePrepareCoro and reacts to whether it
 * parked. Only ever returned when --experimental-coro-prepare-accessor-yield is on AND the caller is
 * running on a PriorityThreadPool (LP) worker -- flag-off, or any other execution context (e.g. the
 * ASIO scheduler's OnReadAsio loop, which never runs on a pool worker), never sees it.
 */
enum class ExecuteResult : uint8_t {
  kNoMoreData,        // no more data to process; caller should arm a fresh read (today's `false`)
  kMoreData,          // more data ready to process now; caller should call Execute_ again (today's `true`)
  kNeedsCoroPrepare,  // state_ == Parsed and this Prepare must be driven via the coroutine chain
};
}  // namespace memgraph::communication::bolt
