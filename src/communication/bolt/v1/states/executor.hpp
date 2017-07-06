#pragma once

#include <string>

#include <glog/logging.h>

#include "communication/bolt/v1/codes.hpp"
#include "communication/bolt/v1/state.hpp"
#include "query/exceptions.hpp"
#include "query/typed_value.hpp"
#include "utils/exceptions.hpp"

namespace communication::bolt {

/**
 * Executor state run function
 * This function executes an initialized Bolt session.
 * It executes: RUN, PULL_ALL, DISCARD_ALL & RESET.
 * @param session the session that should be used for the run
 */
template <typename Session>
State StateExecutorRun(Session &session) {
  Marker marker;
  Signature signature;
  if (!session.decoder_.ReadMessageHeader(&signature, &marker)) {
    DLOG(WARNING) << "Missing header data!";
    return State::Close;
  }

  if (signature == Signature::Run) {
    if (marker != Marker::TinyStruct2) {
      DLOG(WARNING) << fmt::format(
          "Expected TinyStruct2 marker, but received 0x{:02X}!",
          underlying_cast(marker));
      return State::Close;
    }

    query::TypedValue query, params;
    if (!session.decoder_.ReadTypedValue(&query,
                                         query::TypedValue::Type::String)) {
      DLOG(WARNING) << "Couldn't read query string!";
      return State::Close;
    }

    if (!session.decoder_.ReadTypedValue(&params,
                                         query::TypedValue::Type::Map)) {
      DLOG(WARNING) << "Couldn't read parameters!";
      return State::Close;
    }

    auto db_accessor = session.dbms_.active();
    DLOG(INFO) << fmt::format("[ActiveDB] '{}'", db_accessor->name());

    try {
      DLOG(INFO) << fmt::format("[Run] '{}'", query.Value<std::string>());
      auto is_successfully_executed = session.query_engine_.Run(
          query.Value<std::string>(), *db_accessor, session.output_stream_);

      if (!is_successfully_executed) {
        // abort transaction
        db_accessor->abort();

        // clear any leftover messages in the buffer
        session.encoder_buffer_.Clear();

        // send failure message
        bool exec_fail_sent = session.encoder_.MessageFailure(
            {{"code", "Memgraph.QueryExecutionFail"},
             {"message",
              "Query execution has failed (probably there is no "
              "element or there are some problems with concurrent "
              "access -> client has to resolve problems with "
              "concurrent access)"}});

        if (!exec_fail_sent) {
          DLOG(WARNING) << "Couldn't send failure message!";
          return State::Close;
        } else {
          DLOG(WARNING) << "Query execution failed!";
          return State::Error;
        }
      } else {
        db_accessor->commit();
        // The query engine has already stored all query data in the buffer.
        // We should only send the first chunk now which is the success
        // message which contains header data. The rest of this data (records
        // and summary) will be sent after a PULL_ALL command from the client.
        if (!session.encoder_buffer_.FlushFirstChunk()) {
          DLOG(WARNING) << "Couldn't flush header data from the buffer!";
          return State::Close;
        }
        return State::Executor;
      }

    } catch (const utils::BasicException &e) {
      // clear header success message
      session.encoder_buffer_.Clear();
      db_accessor->abort();
      bool fail_sent = session.encoder_.MessageFailure(
          {{"code", "Memgraph.Exception"}, {"message", e.what()}});
      DLOG(WARNING) << fmt::format("Error message: {}", e.what());
      if (!fail_sent) {
        DLOG(WARNING) << "Couldn't send failure message!";
        return State::Close;
      }
      return State::Error;

    } catch (const utils::StacktraceException &e) {
      // clear header success message
      session.encoder_buffer_.Clear();
      db_accessor->abort();
      bool fail_sent = session.encoder_.MessageFailure(
          {{"code", "Memgraph.Exception"}, {"message", e.what()}});
      DLOG(WARNING) << fmt::format("Error message: {}", e.what());
      DLOG(WARNING) << fmt::format("Error trace: {}", e.trace());
      if (!fail_sent) {
        DLOG(WARNING) << "Couldn't send failure message!";
        return State::Close;
      }
      return State::Error;

    } catch (std::exception &e) {
      // clear header success message
      session.encoder_buffer_.Clear();
      db_accessor->abort();
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
      return State::Error;
    }

  } else if (signature == Signature::PullAll) {
    DLOG(INFO) << "[PullAll]";
    if (marker != Marker::TinyStruct) {
      DLOG(WARNING) << fmt::format(
          "Expected TinyStruct marker, but received 0x{:02X}!",
          underlying_cast(marker));
      return State::Close;
    }
    if (!session.encoder_buffer_.HasData()) {
      // the buffer doesn't have data, return a failure message
      bool data_fail_sent = session.encoder_.MessageFailure(
          {{"code", "Memgraph.Exception"},
           {"message",
            "There is no data to "
            "send, you have to execute a RUN command before a PULL_ALL!"}});
      if (!data_fail_sent) {
        DLOG(WARNING) << "Couldn't send failure message!";
        return State::Close;
      }
      return State::Error;
    }
    // flush pending data to the client, the success message is streamed
    // from the query engine, it contains statistics from the query run
    if (!session.encoder_buffer_.Flush()) {
      DLOG(WARNING) << "Couldn't flush data from the buffer!";
      return State::Close;
    }
    return State::Executor;

  } else if (signature == Signature::DiscardAll) {
    DLOG(INFO) << "[DiscardAll]";
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
    return State::Executor;

  } else if (signature == Signature::Reset) {
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
    return State::Executor;

  } else {
    DLOG(WARNING) << fmt::format("Unrecognized signature recieved (0x{:02X})!",
                                 underlying_cast(signature));
    return State::Close;
  }
}
}
