// Copyright 2021 Memgraph Ltd.
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

#include <functional>
#include <map>
#include <memory>
#include <new>
#include <string>
#include <unordered_map>

#include "communication/bolt/v1/codes.hpp"
#include "communication/bolt/v1/constants.hpp"
#include "communication/bolt/v1/exceptions.hpp"
#include "communication/bolt/v1/state.hpp"
#include "communication/bolt/v1/states/handlers.hpp"
#include "communication/bolt/v1/value.hpp"
#include "utils/likely.hpp"
#include "utils/logging.hpp"
#include "utils/message.hpp"

namespace communication::bolt {

template <typename TSession>
class BoltHandlersV1 {
 public:
  static State RunHandler(Signature signature, TSession &session, State state, Marker marker) {
    switch (signature) {
      case Signature::Run:
        return std::invoke(HandleRunV1<TSession>, session, state, marker);
      case Signature::Pull:
        return std::invoke(HandlePullV1<TSession>, session, state, marker);
      case Signature::Discard:
        return std::invoke(HandleDiscardV1<TSession>, session, state, marker);
      case Signature::Reset:
        return std::invoke(HandleReset<TSession>, session, state, marker);
      default:
        spdlog::trace("Unrecognized signature received (0x{:02X})!", utils::UnderlyingCast(signature));
        return State::Close;
    }
  }
};

template <typename TSession, int bolt_minor = 0>
class BoltHandlersV4 {
 public:
  static State RunHandler(Signature signature, TSession &session, State state, Marker marker) {
    if constexpr (bolt_minor >= 1) {
      if (signature == Signature::Noop) return std::invoke(HandleNoop<TSession>, session, state, marker);
    }
    if constexpr (bolt_minor >= 3) {
      if (signature == Signature::Route) return std::invoke(HandleRoute<TSession>, session, state, marker);
    }
    switch (signature) {
      case Signature::Run:
        return std::invoke(HandleRunV2<TSession>, session, state, marker);
      case Signature::Pull:
        return std::invoke(HandlePullV4<TSession>, session, state, marker);
      case Signature::Discard:
        return std::invoke(HandleDiscardV4<TSession>, session, state, marker);
      case Signature::Reset:
        return std::invoke(HandleReset<TSession>, session, state, marker);
      case Signature::Begin:
        return std::invoke(HandleBegin<TSession>, session, state, marker);
      case Signature::Commit:
        return std::invoke(HandleCommit<TSession>, session, state, marker);
      case Signature::Goodbye:
        return std::invoke(HandleGoodbye<TSession>, session, state, marker);
      case Signature::Rollback:
        return std::invoke(HandleRollback<TSession>, session, state, marker);
      default:
        spdlog::trace("Unrecognized signature received (0x{:02X})!", utils::UnderlyingCast(signature));
        return State::Close;
    }
  }
};

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
      return BoltHandlersV1<TSession>::RunHandler(signature, session, state, marker);
    case 4:
      if (session.version_.minor >= 3) {
        return BoltHandlersV4<TSession, 3>::RunHandler(signature, session, state, marker);
      }
      if (session.version_.minor >= 1) {
        return BoltHandlersV4<TSession, 1>::RunHandler(signature, session, state, marker);
      }
      return BoltHandlersV4<TSession>::RunHandler(signature, session, state, marker);
      break;
    default:
      spdlog::trace("Unsupported bolt version:{}.{})!", session.version_.major, session.version_.minor);
      return State::Close;
  }
}
}  // namespace communication::bolt
