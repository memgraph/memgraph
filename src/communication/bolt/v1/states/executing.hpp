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
class BoltHandlers {
 public:
  using BoltHandler = State (*)(TSession &, State, Marker);

  explicit BoltHandlers(std::unordered_map<Signature, BoltHandler, EnumClassHash> handlers)
      : handlers(std::move(handlers)) {}

  virtual ~BoltHandlers(){};
  BoltHandlers(const BoltHandlers &) = delete;
  BoltHandlers(const BoltHandlers &&) = delete;
  BoltHandlers &operator=(const BoltHandlers &) = delete;
  BoltHandlers &operator=(const BoltHandlers &&) = delete;

  State RunHandler(const Signature signature, TSession &session, State state, Marker marker) {
    if (handlers.contains(signature)) {
      return std::invoke(handlers[signature], session, state, marker);
    }
    spdlog::trace("Unrecognized signature received (0x{:02X})!", utils::UnderlyingCast(signature));
    return State::Close;
  }

 private:
  std::unordered_map<Signature, BoltHandler, EnumClassHash> handlers;
};

template <typename TSession>
class BoltHandlersV1 : public BoltHandlers<TSession> {
 public:
  using typename BoltHandlers<TSession>::BoltHandler;

  BoltHandlersV1()
      : BoltHandlers<TSession>(std::move(std::unordered_map<Signature, BoltHandler, EnumClassHash>{
            {Signature::Run, HandleRunV1<TSession>},
            {Signature::Pull, HandlePullV1<TSession>},
            {Signature::Discard, HandleDiscardV1<TSession>},
            {Signature::Reset, HandleReset<TSession>},
        })) {}
};

template <typename TSession>
class BoltHandlersV4 : public BoltHandlers<TSession> {
 public:
  using typename BoltHandlers<TSession>::BoltHandler;

  BoltHandlersV4()
      : BoltHandlers<TSession>{std::move(std::unordered_map<Signature, BoltHandler, EnumClassHash>{
            {Signature::Run, HandleRunV2<TSession>},
            {Signature::Pull, HandlePullV4<TSession>},
            {Signature::Discard, HandleDiscardV4<TSession>},
            {Signature::Begin, HandleBegin<TSession>},
            {Signature::Commit, HandleCommit<TSession>},
            {Signature::Reset, HandleReset<TSession>},
            {Signature::Goodbye, HandleGoodbye<TSession>},
            {Signature::Rollback, HandleRollback<TSession>},
        })} {}
};

template <typename TSession>
class BoltHandlersV41 : public BoltHandlers<TSession> {
 public:
  using typename BoltHandlers<TSession>::BoltHandler;

  BoltHandlersV41()
      : BoltHandlers<TSession>{std::move(std::unordered_map<Signature, BoltHandler, EnumClassHash>{
            {Signature::Run, HandleRunV2<TSession>},
            {Signature::Pull, HandlePullV4<TSession>},
            {Signature::Discard, HandleDiscardV4<TSession>},
            {Signature::Begin, HandleBegin<TSession>},
            {Signature::Commit, HandleCommit<TSession>},
            {Signature::Reset, HandleReset<TSession>},
            {Signature::Goodbye, HandleGoodbye<TSession>},
            {Signature::Rollback, HandleRollback<TSession>},
            {Signature::Noop, HandleNoop<TSession>},
        })} {}
};

template <typename TSession>
class BoltHandlersV43 : public BoltHandlers<TSession> {
 public:
  using typename BoltHandlers<TSession>::BoltHandler;

  BoltHandlersV43()
      : BoltHandlers<TSession>{std::move(std::unordered_map<Signature, BoltHandler, EnumClassHash>{
            {Signature::Run, HandleRunV2<TSession>},
            {Signature::Pull, HandlePullV4<TSession>},
            {Signature::Discard, HandleDiscardV4<TSession>},
            {Signature::Begin, HandleBegin<TSession>},
            {Signature::Commit, HandleCommit<TSession>},
            {Signature::Reset, HandleReset<TSession>},
            {Signature::Goodbye, HandleGoodbye<TSession>},
            {Signature::Rollback, HandleRollback<TSession>},
            {Signature::Noop, HandleNoop<TSession>},
            {Signature::Route, HandleRoute<TSession>},

        })} {}
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
  std::unique_ptr<BoltHandlers<TSession>> bolt_handlers;

  switch (session.version_.major) {
    case 1:
      bolt_handlers = std::make_unique<BoltHandlersV1<TSession>>();
      break;
    case 4:
      bolt_handlers = std::make_unique<BoltHandlersV4<TSession>>();
      if (session.version_.minor >= 1) {
        bolt_handlers = std::make_unique<BoltHandlersV41<TSession>>();
      }
      if (session.version_.minor >= 3) {
        bolt_handlers = std::make_unique<BoltHandlersV43<TSession>>();
      }
      break;
    default:
      spdlog::trace("Unsupported bolt version:{}.{})!", session.version_.major, session.version_.minor);
      return State::Close;
  }
  return bolt_handlers->RunHandler(signature, session, state, marker);
}
}  // namespace communication::bolt
