#pragma once

#include <map>
#include <string>

#include <glog/logging.h>

#include "communication/bolt/v1/codes.hpp"
#include "communication/bolt/v1/decoder/decoded_value.hpp"
#include "communication/bolt/v1/state.hpp"
#include "query/exceptions.hpp"
#include "query/typed_value.hpp"
#include "utils/exceptions.hpp"

namespace communication::bolt {

template <typename Session>
State HandleRun(Session &session, State state, Marker marker) {
  const std::map<std::string, query::TypedValue> kEmptyFields = {
      {"fields", std::vector<query::TypedValue>{}}};

  if (marker != Marker::TinyStruct2) {
    DLOG(WARNING) << fmt::format(
        "Expected TinyStruct2 marker, but received 0x{:02X}!",
        underlying_cast(marker));
    return State::Close;
  }

  DecodedValue query, params;
  if (!session.decoder_.ReadValue(&query, DecodedValue::Type::String)) {
    DLOG(WARNING) << "Couldn't read query string!";
    return State::Close;
  }

  if (!session.decoder_.ReadValue(&params, DecodedValue::Type::Map)) {
    DLOG(WARNING) << "Couldn't read parameters!";
    return State::Close;
  }

  if (state == State::WaitForRollback) {
    if (query.ValueString() == "ROLLBACK") {
      session.Abort();
      // One MessageSuccess for RUN command should be flushed.
      session.encoder_.MessageSuccess(kEmptyFields);
      // One for PULL_ALL should be chunked.
      session.encoder_.MessageSuccess({}, false);
      return State::Result;
    }
    DLOG(WARNING) << "Expected RUN \"ROLLBACK\" not received!";
    // Client could potentially recover if we move to error state, but we don't
    // implement rollback of single command in transaction, only rollback of
    // whole transaction so we can't continue in this transaction if we receive
    // new RUN command.
    return State::Close;
  }

  if (state != State::Idle) {
    // Client could potentially recover if we move to error state, but there is
    // no legitimate situation in which well working client would end up in this
    // situation.
    DLOG(WARNING) << "Unexpected RUN command!";
    return State::Close;
  }

  debug_assert(!session.encoder_buffer_.HasData(),
               "There should be no data to write in this state");

  DLOG(INFO) << fmt::format("[Run] '{}'", query.ValueString());
  bool in_explicit_transaction = false;
  if (session.db_accessor_) {
    // Transaction already exists.
    in_explicit_transaction = true;
  } else {
    // Create new transaction.
    session.db_accessor_ = session.dbms_.active();
  }

  DLOG(INFO) << fmt::format("[ActiveDB] '{}'", session.db_accessor_->name());

  // If there was not explicitly started transaction before maybe we are
  // starting one now.
  if (!in_explicit_transaction && query.ValueString() == "BEGIN") {
    // Check if query string is "BEGIN". If it is then we should start
    // transaction and wait for in-transaction queries.
    // TODO: "BEGIN" is not defined by bolt protocol or opencypher so we should
    // test if all drivers really denote transaction start with "BEGIN" string.
    // Same goes for "ROLLBACK" and "COMMIT".
    //
    // One MessageSuccess for RUN command should be flushed.
    session.encoder_.MessageSuccess(kEmptyFields);
    // One for PULL_ALL should be chunked.
    session.encoder_.MessageSuccess({}, false);
    return State::Result;
  }

  if (in_explicit_transaction) {
    if (query.ValueString() == "COMMIT") {
      session.Commit();
      // One MessageSuccess for RUN command should be flushed.
      session.encoder_.MessageSuccess(kEmptyFields);
      // One for PULL_ALL should be chunked.
      session.encoder_.MessageSuccess({}, false);
      return State::Result;
    } else if (query.ValueString() == "ROLLBACK") {
      session.Abort();
      // One MessageSuccess for RUN command should be flushed.
      session.encoder_.MessageSuccess(kEmptyFields);
      // One for PULL_ALL should be chunked.
      session.encoder_.MessageSuccess({}, false);
      return State::Result;
    }
    session.db_accessor_->AdvanceCommand();
  }

  try {
    auto &params_map = params.ValueMap();
    std::map<std::string, query::TypedValue> params_tv(params_map.begin(),
                                                       params_map.end());
    session.interpreter_.Interpret(query.ValueString(), *session.db_accessor_,
                                   session.output_stream_, params_tv);

    if (!in_explicit_transaction) {
      session.Commit();
    }
    // The query engine has already stored all query data in the buffer.
    // We should only send the first chunk now which is the success
    // message which contains header data. The rest of this data (records
    // and summary) will be sent after a PULL_ALL command from the client.
    if (!session.encoder_buffer_.FlushFirstChunk()) {
      DLOG(WARNING) << "Couldn't flush header data from the buffer!";
      return State::Close;
    }
    return State::Result;
    // TODO: Remove duplication in error handling.
  } catch (const utils::BasicException &e) {
    // clear header success message
    session.encoder_buffer_.Clear();
    bool fail_sent = session.encoder_.MessageFailure(
        {{"code", "Memgraph.Exception"}, {"message", e.what()}});
    DLOG(WARNING) << fmt::format("Error message: {}", e.what());
    if (!fail_sent) {
      DLOG(WARNING) << "Couldn't send failure message!";
      return State::Close;
    }
    if (!in_explicit_transaction) {
      session.Abort();
      return State::ErrorIdle;
    }
    return State::ErrorWaitForRollback;
  } catch (const utils::StacktraceException &e) {
    // clear header success message
    session.encoder_buffer_.Clear();
    bool fail_sent = session.encoder_.MessageFailure(
        {{"code", "Memgraph.Exception"}, {"message", e.what()}});
    DLOG(WARNING) << fmt::format("Error message: {}", e.what());
    DLOG(WARNING) << fmt::format("Error trace: {}", e.trace());
    if (!fail_sent) {
      DLOG(WARNING) << "Couldn't send failure message!";
      return State::Close;
    }
    if (!in_explicit_transaction) {
      session.Abort();
      return State::ErrorIdle;
    }
    return State::ErrorWaitForRollback;
  } catch (const std::exception &e) {
    // clear header success message
    session.encoder_buffer_.Clear();
    bool fail_sent = session.encoder_.MessageFailure(
        {{"code", "Memgraph.Exception"},
         {"message",
          "An unknown exception occured, please contact your database "
          "administrator."}});
    DLOG(WARNING) << fmt::format("std::exception {}", e.what());
    if (!fail_sent) {
      DLOG(WARNING) << "Couldn't send failure message!";
      return State::Close;
    }
    if (!in_explicit_transaction) {
      session.Abort();
      return State::ErrorIdle;
    }
    return State::ErrorWaitForRollback;
  }
}

// TODO: Get rid of duplications in PullAll/DiscardAll functions.
template <typename Session>
State HandlePullAll(Session &session, State state, Marker marker) {
  DLOG(INFO) << "[PullAll]";
  if (marker != Marker::TinyStruct) {
    DLOG(WARNING) << fmt::format(
        "Expected TinyStruct marker, but received 0x{:02X}!",
        underlying_cast(marker));
    return State::Close;
  }

  if (state != State::Result) {
    DLOG(WARNING) << "Unexpected PULL_ALL!";
    // Same as `unexpected RUN` case.
    return State::Close;
  }
  // Flush pending data to the client, the success message is streamed
  // from the query engine, it contains statistics from the query run.
  if (!session.encoder_buffer_.Flush()) {
    DLOG(WARNING) << "Couldn't flush data from the buffer!";
    return State::Close;
  }
  return State::Idle;
}

template <typename Session>
State HandleDiscardAll(Session &session, State state, Marker marker) {
  DLOG(INFO) << "[DiscardAll]";
  if (marker != Marker::TinyStruct) {
    DLOG(WARNING) << fmt::format(
        "Expected TinyStruct marker, but received 0x{:02X}!",
        underlying_cast(marker));
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
        underlying_cast(marker));
    return State::Close;
  }
  // clear all pending data and send a success message
  session.encoder_buffer_.Clear();
  if (!session.encoder_.MessageSuccess()) {
    DLOG(WARNING) << "Couldn't send success message!";
    return State::Close;
  }
  if (session.db_accessor_) {
    session.Abort();
  }
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
                                 underlying_cast(signature));
    return State::Close;
  }
}
}
