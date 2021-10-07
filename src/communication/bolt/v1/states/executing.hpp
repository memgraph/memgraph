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

#include <map>
#include <new>
#include <string>

#include "communication/bolt/v1/codes.hpp"
#include "communication/bolt/v1/constants.hpp"
#include "communication/bolt/v1/exceptions.hpp"
#include "communication/bolt/v1/state.hpp"
#include "communication/bolt/v1/value.hpp"
#include "communication/exceptions.hpp"
#include "utils/likely.hpp"
#include "utils/logging.hpp"
#include "utils/message.hpp"

namespace communication::bolt {

// TODO (mferencevic): revise these error messages
inline std::pair<std::string, std::string> ExceptionToErrorMessage(const std::exception &e) {
  if (auto *verbose = dynamic_cast<const VerboseError *>(&e)) {
    return {verbose->code(), verbose->what()};
  }
  if (dynamic_cast<const ClientError *>(&e)) {
    // Clients expect 4 strings separated by dots. First being database name
    // (for example: Neo, Memgraph...), second being either ClientError,
    // TransientError or DatabaseError (or ClientNotification for warnings).
    // ClientError means wrong query, do not retry. DatabaseError means
    // something wrong in database, do not retry. TransientError means query
    // failed, but if retried it may succeed, retry it.
    //
    // Third and fourth strings being namespace and specific error name.
    // It is not really important what we put there since we don't expect
    // any special handling of specific exceptions on client side, but we
    // need to make sure that we don't accidentally return some exception
    // name which clients handle in a special way. For example, if client
    // receives *.TransientError.Transaction.Terminate it will not rerun
    // query even though TransientError was returned, because of Neo's
    // semantics of that error.
    return {"Memgraph.ClientError.MemgraphError.MemgraphError", e.what()};
  }
  if (dynamic_cast<const utils::BasicException *>(&e)) {
    // Exception not derived from QueryException was thrown which means that
    // database probably aborted transaction because of some timeout,
    // deadlock, serialization error or something similar. We return
    // TransientError since retry of same transaction could succeed.
    return {"Memgraph.TransientError.MemgraphError.MemgraphError", e.what()};
  }
  if (dynamic_cast<const std::bad_alloc *>(&e)) {
    // std::bad_alloc was thrown, God knows in which state is database ->
    // terminate.
    LOG_FATAL("Memgraph is out of memory");
  }
  // All exceptions used in memgraph are derived from BasicException. Since
  // we caught some other exception we don't know what is going on. Return
  // DatabaseError, log real message and return generic string.
  spdlog::error(
      utils::MessageWithLink("Unknown exception occurred during query execution {}.", e.what(), "https://memgr.ph/unknown"));
  return {"Memgraph.DatabaseError.MemgraphError.MemgraphError",
          "An unknown exception occurred, this is unexpected. Real message "
          "should be in database logs."};
}

template <typename TSession>
inline State HandleFailure(TSession &session, const std::exception &e) {
  spdlog::trace("Error message: {}", e.what());
  if (const auto *p = dynamic_cast<const utils::StacktraceException *>(&e)) {
    spdlog::trace("Error trace: {}", p->trace());
  }
  session.encoder_buffer_.Clear();
  auto code_message = ExceptionToErrorMessage(e);
  bool fail_sent = session.encoder_.MessageFailure({{"code", code_message.first}, {"message", code_message.second}});
  if (!fail_sent) {
    spdlog::trace("Couldn't send failure message!");
    return State::Close;
  }
  return State::Error;
}

template <typename TSession>
State HandleRun(TSession &session, State state, Marker marker) {
  const std::map<std::string, Value> kEmptyFields = {{"fields", std::vector<Value>{}}};

  const auto expected_marker = session.version_.major == 1 ? Marker::TinyStruct2 : Marker::TinyStruct3;
  if (marker != expected_marker) {
    spdlog::trace("Expected {} marker, but received 0x{:02X}!",
                  session.version_.major == 1 ? "TinyStruct2" : "TinyStruct3", utils::UnderlyingCast(marker));
    return State::Close;
  }

  Value query, params, extra;
  if (!session.decoder_.ReadValue(&query, Value::Type::String)) {
    spdlog::trace("Couldn't read query string!");
    return State::Close;
  }

  if (!session.decoder_.ReadValue(&params, Value::Type::Map)) {
    spdlog::trace("Couldn't read parameters!");
    return State::Close;
  }

  if (session.version_.major == 4) {
    if (!session.decoder_.ReadValue(&extra, Value::Type::Map)) {
      spdlog::trace("Couldn't read extra field!");
    }
  }

  if (state != State::Idle) {
    // Client could potentially recover if we move to error state, but there is
    // no legitimate situation in which well working client would end up in this
    // situation.
    spdlog::trace("Unexpected RUN command!");
    return State::Close;
  }

  DMG_ASSERT(!session.encoder_buffer_.HasData(), "There should be no data to write in this state");

  spdlog::debug("[Run] '{}'", query.ValueString());

  try {
    // Interpret can throw.
    auto [header, qid] = session.Interpret(query.ValueString(), params.ValueMap());
    // Convert std::string to Value
    std::vector<Value> vec;
    std::map<std::string, Value> data;
    vec.reserve(header.size());
    for (auto &i : header) vec.emplace_back(std::move(i));
    data.emplace("fields", std::move(vec));
    // Send the header.
    if (!session.encoder_.MessageSuccess(data)) {
      spdlog::trace("Couldn't send query header!");
      return State::Close;
    }
    return State::Result;
  } catch (const std::exception &e) {
    return HandleFailure(session, e);
  }
}

namespace detail {
template <bool is_pull, typename TSession>
State HandlePullDiscard(TSession &session, State state, Marker marker) {
  const auto expected_marker = session.version_.major == 1 ? Marker::TinyStruct : Marker::TinyStruct1;
  if (marker != expected_marker) {
    spdlog::trace("Expected {} marker, but received 0x{:02X}!",
                  session.version_.major == 1 ? "TinyStruct" : "TinyStruct1", utils::UnderlyingCast(marker));
    return State::Close;
  }

  if (state != State::Result) {
    if constexpr (is_pull) {
      spdlog::trace("Unexpected PULL!");
    } else {
      spdlog::trace("Unexpected DISCARD!");
    }
    // Same as `unexpected RUN` case.
    return State::Close;
  }

  try {
    std::optional<int> n;
    std::optional<int> qid;

    if (session.version_.major == 4) {
      Value extra;
      if (!session.decoder_.ReadValue(&extra, Value::Type::Map)) {
        spdlog::trace("Couldn't read extra field!");
      }
      const auto &extra_map = extra.ValueMap();
      if (extra_map.count("n")) {
        if (const auto n_value = extra_map.at("n").ValueInt(); n_value != kPullAll) {
          n = n_value;
        }
      }

      if (extra_map.count("qid")) {
        if (const auto qid_value = extra_map.at("qid").ValueInt(); qid_value != kPullLast) {
          qid = qid_value;
        }
      }
    }

    std::map<std::string, Value> summary;
    if constexpr (is_pull) {
      // Pull can throw.
      summary = session.Pull(&session.encoder_, n, qid);
    } else {
      summary = session.Discard(n, qid);
    }

    if (!session.encoder_.MessageSuccess(summary)) {
      spdlog::trace("Couldn't send query summary!");
      return State::Close;
    }

    if (summary.count("has_more") && summary.at("has_more").ValueBool()) {
      return State::Result;
    }

    return State::Idle;
  } catch (const std::exception &e) {
    return HandleFailure(session, e);
  }
}
}  // namespace detail

template <typename Session>
State HandlePull(Session &session, State state, Marker marker) {
  return detail::HandlePullDiscard<true>(session, state, marker);
}

template <typename Session>
State HandleDiscard(Session &session, State state, Marker marker) {
  return detail::HandlePullDiscard<false>(session, state, marker);
}

template <typename Session>
State HandleReset(Session &session, State, Marker marker) {
  // IMPORTANT: This implementation of the Bolt RESET command isn't fully
  // compliant to the protocol definition. In the protocol it is defined
  // that this command should immediately stop any running commands and
  // reset the session to a clean state. That means that we should always
  // make a look-ahead for the RESET command before processing anything.
  // Our implementation, for now, does everything in a blocking fashion
  // so we cannot simply "kill" a transaction while it is running. So
  // now this command only resets the session to a clean state. It
  // does not IGNORE running and pending commands as it should.
  if (marker != Marker::TinyStruct) {
    spdlog::trace("Expected TinyStruct marker, but received 0x{:02X}!", utils::UnderlyingCast(marker));
    return State::Close;
  }

  // Clear all pending data and send a success message.
  session.encoder_buffer_.Clear();

  if (!session.encoder_.MessageSuccess()) {
    spdlog::trace("Couldn't send success message!");
    return State::Close;
  }

  session.Abort();

  return State::Idle;
}

template <typename Session>
State HandleBegin(Session &session, State state, Marker marker) {
  if (session.version_.major == 1) {
    spdlog::trace("BEGIN messsage not supported in Bolt v1!");
    return State::Close;
  }

  if (marker != Marker::TinyStruct1) {
    spdlog::trace("Expected TinyStruct1 marker, but received 0x{:02x}!", utils::UnderlyingCast(marker));
    return State::Close;
  }

  Value extra;
  if (!session.decoder_.ReadValue(&extra, Value::Type::Map)) {
    spdlog::trace("Couldn't read extra fields!");
    return State::Close;
  }

  if (state != State::Idle) {
    spdlog::trace("Unexpected BEGIN command!");
    return State::Close;
  }

  DMG_ASSERT(!session.encoder_buffer_.HasData(), "There should be no data to write in this state");

  if (!session.encoder_.MessageSuccess({})) {
    spdlog::trace("Couldn't send success message!");
    return State::Close;
  }

  try {
    session.BeginTransaction();
  } catch (const std::exception &e) {
    return HandleFailure(session, e);
  }

  return State::Idle;
}

template <typename Session>
State HandleCommit(Session &session, State state, Marker marker) {
  if (session.version_.major == 1) {
    spdlog::trace("COMMIT messsage not supported in Bolt v1!");
    return State::Close;
  }

  if (marker != Marker::TinyStruct) {
    spdlog::trace("Expected TinyStruct marker, but received 0x{:02x}!", utils::UnderlyingCast(marker));
    return State::Close;
  }

  if (state != State::Idle) {
    spdlog::trace("Unexpected COMMIT command!");
    return State::Close;
  }

  DMG_ASSERT(!session.encoder_buffer_.HasData(), "There should be no data to write in this state");

  try {
    if (!session.encoder_.MessageSuccess({})) {
      spdlog::trace("Couldn't send success message!");
      return State::Close;
    }
    session.CommitTransaction();
    return State::Idle;
  } catch (const std::exception &e) {
    return HandleFailure(session, e);
  }
}

template <typename Session>
State HandleRollback(Session &session, State state, Marker marker) {
  if (session.version_.major == 1) {
    spdlog::trace("ROLLBACK messsage not supported in Bolt v1!");
    return State::Close;
  }

  if (marker != Marker::TinyStruct) {
    spdlog::trace("Expected TinyStruct marker, but received 0x{:02x}!", utils::UnderlyingCast(marker));
    return State::Close;
  }

  if (state != State::Idle) {
    spdlog::trace("Unexpected ROLLBACK command!");
    return State::Close;
  }

  DMG_ASSERT(!session.encoder_buffer_.HasData(), "There should be no data to write in this state");

  try {
    if (!session.encoder_.MessageSuccess({})) {
      spdlog::trace("Couldn't send success message!");
      return State::Close;
    }
    session.RollbackTransaction();
    return State::Idle;
  } catch (const std::exception &e) {
    return HandleFailure(session, e);
  }
}

/**
 * Executor state run function
 * This function executes an initialized Bolt session.
 * It executes: RUN, PULL_ALL, DISCARD_ALL & RESET.
 * @param session the session that should be used for the run
 */
template <typename Session>
State StateExecutingRun(Session &session, State state) {
  Marker marker;
  Signature signature;
  if (!session.decoder_.ReadMessageHeader(&signature, &marker)) {
    spdlog::trace("Missing header data!");
    return State::Close;
  }

  if (UNLIKELY(signature == Signature::Noop && session.version_.major == 4 && session.version_.minor == 1)) {
    spdlog::trace("Received NOOP message");
    return state;
  }

  if (signature == Signature::Run) {
    return HandleRun(session, state, marker);
  } else if (signature == Signature::Pull) {
    return HandlePull(session, state, marker);
  } else if (signature == Signature::Discard) {
    return HandleDiscard(session, state, marker);
  } else if (signature == Signature::Begin) {
    return HandleBegin(session, state, marker);
  } else if (signature == Signature::Commit) {
    return HandleCommit(session, state, marker);
  } else if (signature == Signature::Rollback) {
    return HandleRollback(session, state, marker);
  } else if (signature == Signature::Reset) {
    return HandleReset(session, state, marker);
  } else if (signature == Signature::Goodbye && session.version_.major != 1) {
    throw SessionClosedException("Closing connection.");
  } else {
    spdlog::trace("Unrecognized signature received (0x{:02X})!", utils::UnderlyingCast(signature));
    return State::Close;
  }
}
}  // namespace communication::bolt
