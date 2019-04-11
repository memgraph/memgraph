#pragma once

#include <map>
#include <new>
#include <string>

#include <glog/logging.h>

#include "communication/bolt/v1/codes.hpp"
#include "communication/bolt/v1/exceptions.hpp"
#include "communication/bolt/v1/state.hpp"
#include "communication/bolt/v1/value.hpp"

namespace communication::bolt {

// TODO (mferencevic): revise these error messages
inline std::pair<std::string, std::string> ExceptionToErrorMessage(
    const std::exception &e) {
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
    LOG(FATAL) << "Memgraph is out of memory";
  }
  // All exceptions used in memgraph are derived from BasicException. Since
  // we caught some other exception we don't know what is going on. Return
  // DatabaseError, log real message and return generic string.
  LOG(ERROR) << "Unknown exception occurred during query execution "
             << e.what();
  return {"Memgraph.DatabaseError.MemgraphError.MemgraphError",
          "An unknown exception occurred, this is unexpected. Real message "
          "should be in database logs."};
}

template <typename TSession>
State HandleRun(TSession &session, State state, Marker marker) {
  const std::map<std::string, Value> kEmptyFields = {
      {"fields", std::vector<Value>{}}};

  if (marker != Marker::TinyStruct2) {
    DLOG(WARNING) << fmt::format(
        "Expected TinyStruct2 marker, but received 0x{:02X}!",
        utils::UnderlyingCast(marker));
    return State::Close;
  }

  Value query, params;
  if (!session.decoder_.ReadValue(&query, Value::Type::String)) {
    DLOG(WARNING) << "Couldn't read query string!";
    return State::Close;
  }

  if (!session.decoder_.ReadValue(&params, Value::Type::Map)) {
    DLOG(WARNING) << "Couldn't read parameters!";
    return State::Close;
  }

  if (state != State::Idle) {
    // Client could potentially recover if we move to error state, but there is
    // no legitimate situation in which well working client would end up in this
    // situation.
    DLOG(WARNING) << "Unexpected RUN command!";
    return State::Close;
  }

  DCHECK(!session.encoder_buffer_.HasData())
      << "There should be no data to write in this state";

  DLOG(INFO) << fmt::format("[Run] '{}'", query.ValueString());

  try {
    // Interpret can throw.
    auto header = session.Interpret(query.ValueString(), params.ValueMap());
    // Convert std::string to Value
    std::vector<Value> vec;
    std::map<std::string, Value> data;
    vec.reserve(header.size());
    for (auto &i : header) vec.push_back(Value(i));
    data.insert(std::make_pair(std::string("fields"), Value(vec)));
    // Send the header.
    if (!session.encoder_.MessageSuccess(data)) {
      DLOG(WARNING) << "Couldn't send query header!";
      return State::Close;
    }
    return State::Result;
  } catch (const std::exception &e) {
    DLOG(WARNING) << fmt::format("Error message: {}", e.what());
    if (const auto *p = dynamic_cast<const utils::StacktraceException *>(&e)) {
      DLOG(WARNING) << fmt::format("Error trace: {}", p->trace());
    }
    session.encoder_buffer_.Clear();
    auto code_message = ExceptionToErrorMessage(e);
    bool fail_sent = session.encoder_.MessageFailure(
        {{"code", code_message.first}, {"message", code_message.second}});
    if (!fail_sent) {
      DLOG(WARNING) << "Couldn't send failure message!";
      return State::Close;
    }
    return State::Error;
  }
}

template <typename Session>
State HandlePullAll(Session &session, State state, Marker marker) {
  if (marker != Marker::TinyStruct) {
    DLOG(WARNING) << fmt::format(
        "Expected TinyStruct marker, but received 0x{:02X}!",
        utils::UnderlyingCast(marker));
    return State::Close;
  }

  if (state != State::Result) {
    DLOG(WARNING) << "Unexpected PULL_ALL!";
    // Same as `unexpected RUN` case.
    return State::Close;
  }

  try {
    // PullAll can throw.
    auto summary = session.PullAll(&session.encoder_);
    if (!session.encoder_.MessageSuccess(summary)) {
      DLOG(WARNING) << "Couldn't send query summary!";
      return State::Close;
    }
    return State::Idle;
  } catch (const std::exception &e) {
    DLOG(WARNING) << fmt::format("Error message: {}", e.what());
    if (const auto *p = dynamic_cast<const utils::StacktraceException *>(&e)) {
      DLOG(WARNING) << fmt::format("Error trace: {}", p->trace());
    }
    session.encoder_buffer_.Clear();
    auto code_message = ExceptionToErrorMessage(e);
    bool fail_sent = session.encoder_.MessageFailure(
        {{"code", code_message.first}, {"message", code_message.second}});
    if (!fail_sent) {
      DLOG(WARNING) << "Couldn't send failure message!";
      return State::Close;
    }
    return State::Error;
  }
}

template <typename Session>
State HandleDiscardAll(Session &session, State state, Marker marker) {
  if (marker != Marker::TinyStruct) {
    DLOG(WARNING) << fmt::format(
        "Expected TinyStruct marker, but received 0x{:02X}!",
        utils::UnderlyingCast(marker));
    return State::Close;
  }

  if (state != State::Result) {
    DLOG(WARNING) << "Unexpected DISCARD_ALL!";
    // Same as `unexpected RUN` case.
    return State::Close;
  }

  // Clear all pending data and send a success message.
  session.encoder_buffer_.Clear();
  if (!session.encoder_.MessageSuccess()) {
    DLOG(WARNING) << "Couldn't send success message!";
    return State::Close;
  }

  return State::Idle;
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
    DLOG(WARNING) << fmt::format(
        "Expected TinyStruct marker, but received 0x{:02X}!",
        utils::UnderlyingCast(marker));
    return State::Close;
  }

  // Clear all pending data and send a success message.
  session.encoder_buffer_.Clear();
  if (!session.encoder_.MessageSuccess()) {
    DLOG(WARNING) << "Couldn't send success message!";
    return State::Close;
  }

  session.Abort();

  return State::Idle;
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
    DLOG(WARNING) << "Missing header data!";
    return State::Close;
  }

  if (signature == Signature::Run) {
    return HandleRun(session, state, marker);
  } else if (signature == Signature::PullAll) {
    return HandlePullAll(session, state, marker);
  } else if (signature == Signature::DiscardAll) {
    return HandleDiscardAll(session, state, marker);
  } else if (signature == Signature::Reset) {
    return HandleReset(session, state, marker);
  } else {
    DLOG(WARNING) << fmt::format("Unrecognized signature recieved (0x{:02X})!",
                                 utils::UnderlyingCast(signature));
    return State::Close;
  }
}
}  // namespace communication::bolt
