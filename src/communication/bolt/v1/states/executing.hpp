// Copyright 2023 Memgraph Ltd.
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

#include <map>
#include <memory>
#include <new>
#include <string>

#include "communication/bolt/v1/codes.hpp"
#include "communication/bolt/v1/constants.hpp"
#include "communication/bolt/v1/exceptions.hpp"
#include "communication/bolt/v1/state.hpp"
#include "communication/bolt/v1/states/handlers.hpp"
#include "communication/bolt/v1/value.hpp"
#include "utils/event_counter.hpp"
#include "utils/likely.hpp"
#include "utils/logging.hpp"
#include "utils/message.hpp"

namespace memgraph::metrics {
extern const Event BoltMessages;
}  // namespace memgraph::metrics

namespace memgraph::communication::bolt {

template <typename TSession>
State RunHandlerV1(Signature signature, TSession &session, State state, Marker marker) {
  switch (signature) {
    case Signature::Run:
      return HandleRunV1<TSession>(session, state, marker);
    case Signature::Pull:
      return HandlePullV1<TSession>(session, state, marker);
    case Signature::Discard:
      return HandleDiscardV1<TSession>(session, state, marker);
    case Signature::Reset:
      return HandleReset<TSession>(session, marker);
    default:
      spdlog::trace("Unrecognized signature received (0x{:02X})!", utils::UnderlyingCast(signature));
      return State::Close;
  }
}

template <typename TSession, int bolt_minor = 0>
State RunHandlerV4(Signature signature, TSession &session, State state, Marker marker) {
  switch (signature) {
    case Signature::Run:
      return HandleRunV4<TSession>(session, state, marker);
    case Signature::Pull:
      return HandlePullV4<TSession>(session, state, marker);
    case Signature::Discard:
      return HandleDiscardV4<TSession>(session, state, marker);
    case Signature::Reset:
      return HandleReset<TSession>(session, marker);
    case Signature::Begin:
      return HandleBegin<TSession>(session, state, marker);
    case Signature::Commit:
      return HandleCommit<TSession>(session, state, marker);
    case Signature::Goodbye:
      return HandleGoodbye<TSession>();
    case Signature::Rollback:
      return HandleRollback<TSession>(session, state, marker);
    case Signature::Noop: {
      if constexpr (bolt_minor >= 1) {
        return HandleNoop<TSession>(state);
      } else {
        spdlog::trace("Supported only in bolt v4.1");
        return State::Close;
      }
    }
    case Signature::Route: {
      if constexpr (bolt_minor >= 3) {
        if (signature == Signature::Route) return HandleRoute<TSession>(session, marker);
      } else {
        spdlog::trace("Supported only in bolt v4.3");
        return State::Close;
      }
    }
    default:
      spdlog::trace("Unrecognized signature received (0x{:02X})!", utils::UnderlyingCast(signature));
      return State::Close;
  }
}

template <typename TSession>
State RunHandlerV5(Signature signature, TSession &session, State state, Marker marker) {
  switch (signature) {
    case Signature::Run:
      return HandleRunV5<TSession>(session, state, marker);
    case Signature::Pull:
      return HandlePullV5<TSession>(session, state, marker);
    case Signature::Discard:
      return HandleDiscardV5<TSession>(session, state, marker);
    case Signature::Reset:
      return HandleReset<TSession>(session, marker);
    case Signature::Begin:
      return HandleBegin<TSession>(session, state, marker);
    case Signature::Commit:
      return HandleCommit<TSession>(session, state, marker);
    case Signature::Goodbye:
      return HandleGoodbye<TSession>();
    case Signature::Rollback:
      return HandleRollback<TSession>(session, state, marker);
    case Signature::Noop:
      return HandleNoop<TSession>(state);
    case Signature::Route:
      return HandleRoute<TSession>(session, marker);
    case Signature::LogOff:
      return HandleLogOff<TSession>();
    default:
      spdlog::trace("Unrecognized signature received (0x{:02X})!", utils::UnderlyingCast(signature));
      return State::Close;
  }
}

/**
 * Executor state run function
 * This function executes an initialized Bolt session.
 * It executes: RUN, PULL_ALL, DISCARD_ALL & RESET.
 * @param session the session that should be used for the run
 */
template <typename TSession>
State StateExecutingRun(TSession &session, State state) {
  Marker marker;
  Signature signature;
  if (!session.decoder_.ReadMessageHeader(&signature, &marker)) {
    spdlog::trace("Missing header data!");
    return State::Close;
  }

  switch (session.version_.major) {
    case 1:
      memgraph::metrics::IncrementCounter(memgraph::metrics::BoltMessages);
      return RunHandlerV1(signature, session, state, marker);
    case 4: {
      memgraph::metrics::IncrementCounter(memgraph::metrics::BoltMessages);
      if (session.version_.minor >= 3) {
        return RunHandlerV4<TSession, 3>(signature, session, state, marker);
      }
      if (session.version_.minor >= 1) {
        return RunHandlerV4<TSession, 1>(signature, session, state, marker);
      }
      return RunHandlerV4<TSession>(signature, session, state, marker);
    }
    case 5:
      memgraph::metrics::IncrementCounter(memgraph::metrics::BoltMessages);
      return RunHandlerV5<TSession>(signature, session, state, marker);
    default:
      spdlog::trace("Unsupported bolt version:{}.{})!", session.version_.major, session.version_.minor);
      return State::Close;
  }
}
}  // namespace memgraph::communication::bolt
