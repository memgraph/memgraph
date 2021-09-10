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

template <typename Session>
class BoltHandlers {
 public:
  using BoltHandler = std::function<State(Session &, State, Marker)>;

  void AddHandler(const Signature signature, BoltHandler handler) { handlers[signature] = handler; }

  void RemoveHandler(const Signature signature) { handlers.erase(signature); }

  State RunHandler(const Signature signature, Session &session, State state, Marker marker) {
    try {
      return std::invoke(handlers.at(signature), session, state, marker);
    } catch (const std::out_of_range &exp) {
      spdlog::trace("Unrecognized signature received (0x{:02X})!", utils::UnderlyingCast(signature));
      return State::Close;
    }
  }

 private:
  std::unordered_map<Signature, BoltHandler, EnumClassHash> handlers{
      {Signature::Run, HandleRunV2<Session>},         {Signature::Pull, HandlePullV1<Session>},
      {Signature::Discard, HandleDiscardV1<Session>}, {Signature::Begin, HandleBegin<Session>},
      {Signature::Commit, HandleCommit<Session>},     {Signature::Reset, HandleReset<Session>},
      {Signature::Goodbye, HandleGoodbye<Session>},
  };
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
  BoltHandlers<TSession> bolt_handlers;

  switch (session.version_.major) {
    case 1:
      bolt_handlers.AddHandler(Signature::Run, HandleRunV1<TSession>);
      bolt_handlers.RemoveHandler(Signature::Goodbye);
      bolt_handlers.RemoveHandler(Signature::Begin);
      bolt_handlers.RemoveHandler(Signature::Commit);
      bolt_handlers.RemoveHandler(Signature::Rollback);
      break;
    case 2:
    case 3:
      bolt_handlers.AddHandler(Signature::Pull, HandlePullV2<TSession>);
      bolt_handlers.AddHandler(Signature::Discard, HandleDiscardV2<TSession>);
      break;
    case 4:
      bolt_handlers.AddHandler(Signature::Pull, HandlePullV4<TSession>);
      bolt_handlers.AddHandler(Signature::Discard, HandleDiscardV4<TSession>);
      if (session.version_.minor >= 1) {
        bolt_handlers.AddHandler(Signature::Noop, HandleNoop<TSession>);
      }
      if (session.version_.minor >= 3) {
        bolt_handlers.AddHandler(Signature::Route, HandleRoute<TSession>);
      }
      break;
    default:
      spdlog::trace("Unsupported bolt version:{}.{})!", session.version_.major, session.version_.minor);
      return State::Close;
  }
  return bolt_handlers.RunHandler(signature, session, state, marker);
}
}  // namespace communication::bolt
