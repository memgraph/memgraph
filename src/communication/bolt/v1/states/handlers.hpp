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

#include <exception>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "communication/bolt/metrics.hpp"
#include "communication/bolt/v1/codes.hpp"
#include "communication/bolt/v1/constants.hpp"
#include "communication/bolt/v1/exceptions.hpp"
#include "communication/bolt/v1/state.hpp"
#include "communication/bolt/v1/value.hpp"
#include "communication/exceptions.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/logging.hpp"
#include "utils/message.hpp"

namespace memgraph::communication::bolt {
// TODO: Revise these error messages
inline std::pair<std::string, std::string> ExceptionToErrorMessage(const std::exception &e) {
  if (const auto *verbose = dynamic_cast<const VerboseError *>(&e)) {
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
  spdlog::error(utils::MessageWithLink("Unknown exception occurred during query execution {}.", e.what(),
                                       "https://memgr.ph/unknown"));
  return {"Memgraph.DatabaseError.MemgraphError.MemgraphError",
          "An unknown exception occurred, this is unexpected. Real message "
          "should be in database logs."};
}

namespace details {

template <bool is_pull, typename TSession>
State HandlePullDiscard(TSession &session, std::optional<int> n, std::optional<int> qid) {
  try {
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

    if (summary.contains("has_more") && summary.at("has_more").ValueBool()) {
      return State::Result;
    }

    return State::Idle;
  } catch (const std::exception &e) {
    return HandleFailure(session, e);
  }
}

template <bool is_pull, typename TSession>
State HandlePullDiscardV1(TSession &session, const State state, const Marker marker) {
  const auto expected_marker = Marker::TinyStruct;
  if (marker != expected_marker) {
    spdlog::trace("Expected TinyStruct marker, but received 0x{:02X}!", utils::UnderlyingCast(marker));
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

  return HandlePullDiscard<is_pull, TSession>(session, std::nullopt, std::nullopt);
}

template <bool is_pull, typename TSession>
State HandlePullDiscardV4(TSession &session, const State state, const Marker marker) {
  const auto expected_marker = Marker::TinyStruct1;
  if (marker != expected_marker) {
    spdlog::trace("Expected TinyStruct1 marker, but received 0x{:02X}!", utils::UnderlyingCast(marker));
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
  std::optional<int> n;
  std::optional<int> qid;
  Value extra;
  if (!session.decoder_.ReadValue(&extra, Value::Type::Map)) {
    spdlog::trace("Couldn't read extra field!");
  }
  const auto &extra_map = extra.ValueMap();
  if (extra_map.contains("n")) {
    if (const auto n_value = extra_map.at("n").ValueInt(); n_value != kPullAll) {
      n = n_value;
    }
  }

  if (extra_map.contains("qid")) {
    if (const auto qid_value = extra_map.at("qid").ValueInt(); qid_value != kPullLast) {
      qid = qid_value;
    }
  }
  return HandlePullDiscard<is_pull, TSession>(session, n, qid);
}
}  // namespace details

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
State HandleRunV1(TSession &session, const State state, const Marker marker) {
  const auto expected_marker = Marker::TinyStruct2;
  if (marker != expected_marker) {
    spdlog::trace("Expected {} marker, but received 0x{:02X}!",
                  session.version_.major == 1 ? "TinyStruct2" : "TinyStruct3", utils::UnderlyingCast(marker));
    return State::Close;
  }
  Value query;
  Value params;
  if (!session.decoder_.ReadValue(&query, Value::Type::String)) {
    spdlog::trace("Couldn't read query string!");
    return State::Close;
  }

  if (!session.decoder_.ReadValue(&params, Value::Type::Map)) {
    spdlog::trace("Couldn't read parameters!");
    return State::Close;
  }

  if (state != State::Idle) {
    // Client could potentially recover if we move to error state, but there is
    // no legitimate situation in which well working client would end up in this
    // situation.
    spdlog::trace("Unexpected RUN command!");
    return State::Close;
  }

  DMG_ASSERT(!session.encoder_buffer_.HasData(), "There should be no data to write in this state");

#if MG_ENTERPRISE
  spdlog::debug("[Run - {}] '{}'", session.GetCurrentDB(), query.ValueString());
#else
  spdlog::debug("[Run] '{}'", query.ValueString());
#endif

  // Increment number of queries in the metrics
  IncrementQueryMetrics(session);

  try {
    // Interpret can throw.
    const auto [header, qid] = session.Interpret(query.ValueString(), params.ValueMap(), {});
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

template <typename TSession>
State HandleRunV4(TSession &session, const State state, const Marker marker) {
  const auto expected_marker = Marker::TinyStruct3;
  if (marker != expected_marker) {
    spdlog::trace("Expected TinyStruct3 marker, but received 0x{:02X}!", utils::UnderlyingCast(marker));
    return State::Close;
  }
  Value query;
  Value params;
  Value extra;
  if (!session.decoder_.ReadValue(&query, Value::Type::String)) {
    spdlog::trace("Couldn't read query string!");
    return State::Close;
  }

  if (!session.decoder_.ReadValue(&params, Value::Type::Map)) {
    spdlog::trace("Couldn't read parameters!");
    return State::Close;
  }

  // Even though this part seems unnecessary it is needed to move the buffer
  if (!session.decoder_.ReadValue(&extra, Value::Type::Map)) {
    spdlog::trace("Couldn't read extra field!");
    return State::Close;
  }

  if (state != State::Idle) {
    // Client could potentially recover if we move to error state, but there is
    // no legitimate situation in which well working client would end up in this
    // situation.
    spdlog::trace("Unexpected RUN command!");
    return State::Close;
  }

  DMG_ASSERT(!session.encoder_buffer_.HasData(), "There should be no data to write in this state");

  try {
    session.Configure(extra.ValueMap());
  } catch (const std::exception &e) {
    return HandleFailure(session, e);
  }

#if MG_ENTERPRISE
  spdlog::debug("[Run - {}] '{}'", session.GetCurrentDB(), query.ValueString());
#else
  spdlog::debug("[Run] '{}'", query.ValueString());
#endif

  // Increment number of queries in the metrics
  IncrementQueryMetrics(session);

  try {
    // Interpret can throw.
    const auto [header, qid] = session.Interpret(query.ValueString(), params.ValueMap(), extra.ValueMap());
    // Convert std::string to Value
    std::vector<Value> vec;
    std::map<std::string, Value> data;
    vec.reserve(header.size());
    for (auto &i : header) vec.emplace_back(std::move(i));
    data.emplace("fields", std::move(vec));
    if (qid.has_value()) {
      data.emplace("qid", Value{*qid});
    }

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

template <typename TSession>
State HandleRunV5(TSession &session, const State state, const Marker marker) {
  // Using V4 on purpose
  return HandleRunV4<TSession>(session, state, marker);
}

template <typename TSession>
State HandlePullV1(TSession &session, const State state, const Marker marker) {
  return details::HandlePullDiscardV1<true>(session, state, marker);
}

template <typename TSession>
State HandlePullV4(TSession &session, const State state, const Marker marker) {
  return details::HandlePullDiscardV4<true>(session, state, marker);
}

template <typename TSession>
State HandlePullV5(TSession &session, const State state, const Marker marker) {
  // Using V4 on purpose
  return HandlePullV4<TSession>(session, state, marker);
}

template <typename TSession>
State HandleDiscardV1(TSession &session, const State state, const Marker marker) {
  return details::HandlePullDiscardV1<false>(session, state, marker);
}

template <typename TSession>
State HandleDiscardV4(TSession &session, const State state, const Marker marker) {
  return details::HandlePullDiscardV4<false>(session, state, marker);
}

template <typename TSession>
State HandleDiscardV5(TSession &session, const State state, const Marker marker) {
  // Using V4 on purpose
  return HandleDiscardV4<TSession>(session, state, marker);
}

template <typename TSession>
State HandleReset(TSession &session, const Marker marker) {
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

  session.Abort();

  if (!session.encoder_.MessageSuccess()) {
    spdlog::trace("Couldn't send success message!");
    return State::Close;
  }

  return State::Idle;
}

template <typename TSession>
State HandleBegin(TSession &session, const State state, const Marker marker) {
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
    session.Configure(extra.ValueMap());
    session.BeginTransaction(extra.ValueMap());
  } catch (const std::exception &e) {
    return HandleFailure(session, e);
  }

  return State::Idle;
}

template <typename TSession>
State HandleCommit(TSession &session, const State state, const Marker marker) {
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
    session.CommitTransaction();
    if (!session.encoder_.MessageSuccess({})) {
      spdlog::trace("Couldn't send success message!");
      return State::Close;
    }
    return State::Idle;
  } catch (const std::exception &e) {
    return HandleFailure(session, e);
  }
}

template <typename TSession>
State HandleRollback(TSession &session, const State state, const Marker marker) {
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
    session.RollbackTransaction();
    if (!session.encoder_.MessageSuccess({})) {
      spdlog::trace("Couldn't send success message!");
      return State::Close;
    }
    return State::Idle;
  } catch (const std::exception &e) {
    return HandleFailure(session, e);
  }
}

template <typename TSession>
State HandleNoop(const State state) {
  spdlog::trace("Received NOOP message");
  return state;
}

template <typename TSession>
State HandleGoodbye() {
  throw SessionClosedException("Closing connection.");
}

template <typename TSession>
State HandleRoute(TSession &session, const Marker marker) {
  // Route message is not implemented since it is Neo4j specific, therefore we will receive it and inform user that
  // there is no implementation. Before that, we have to read out the fields from the buffer to leave it in a clean
  // state.
  if (marker != Marker::TinyStruct3) {
    spdlog::trace("Expected TinyStruct3 marker, but received 0x{:02x}!", utils::UnderlyingCast(marker));
    return State::Close;
  }
  Value routing;
  if (!session.decoder_.ReadValue(&routing, Value::Type::Map)) {
    spdlog::trace("Couldn't read routing field!");
    return State::Close;
  }

  Value bookmarks;
  if (!session.decoder_.ReadValue(&bookmarks, Value::Type::List)) {
    spdlog::trace("Couldn't read bookmarks field!");
    return State::Close;
  }
  Value db;
  if (!session.decoder_.ReadValue(&db)) {
    spdlog::trace("Couldn't read db field!");
    return State::Close;
  }
  session.encoder_buffer_.Clear();
  bool fail_sent =
      session.encoder_.MessageFailure({{"code", "66"}, {"message", "Route message is not supported in Memgraph!"}});
  if (!fail_sent) {
    spdlog::trace("Couldn't send failure message!");
    return State::Close;
  }
  return State::Error;
}

template <typename TSession>
State HandleLogOff() {
  // No arguments sent, the user just needs to reauthenticate
  return State::Init;
}
}  // namespace memgraph::communication::bolt
