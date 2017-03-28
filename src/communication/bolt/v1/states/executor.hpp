#pragma once

#include <string>

#include "communication/bolt/v1/messaging/codes.hpp"
#include "communication/bolt/v1/state.hpp"
#include "logging/default.hpp"
#include "query/exceptions.hpp"

namespace communication::bolt {

struct Query {
  Query(std::string &&statement) : statement(statement) {}
  std::string statement;
};

/**
 * TODO (mferencevic): finish & document
 */
template <typename Session>
State StateExecutorRun(Session &session) {
  // initialize logger
  static Logger logger = logging::log->logger("State EXECUTOR");

  // just read one byte that represents the struct type, we can skip the
  // information contained in this byte
  session.decoder_.read_byte();
  auto message_type = session.decoder_.read_byte();

  if (message_type == MessageCode::Run) {
    Query query(session.decoder_.read_string());

    // TODO (mferencevic): implement proper exception handling
    auto db_accessor = session.dbms_.active();
    logger.debug("[ActiveDB] '{}'", db_accessor->name());

    try {
      logger.trace("[Run] '{}'", query.statement);
      auto is_successfully_executed = session.query_engine_.Run(
          query.statement, *db_accessor, session.output_stream_);

      if (!is_successfully_executed) {
        db_accessor->abort();
        session.encoder_.MessageFailure(
            {{"code", "Memgraph.QueryExecutionFail"},
             {"message",
              "Query execution has failed (probably there is no "
              "element or there are some problems with concurrent "
              "access -> client has to resolve problems with "
              "concurrent access)"}});
        return ERROR;
      } else {
        db_accessor->commit();
      }

      return EXECUTOR;
    // !! QUERY ENGINE -> RUN METHOD -> EXCEPTION HANDLING !!
    } catch (const query::SyntaxException &e) {
      db_accessor->abort();
      session.encoder_.MessageFailure(
          {{"code", "Memgraph.SyntaxException"}, {"message", "Syntax error"}});
      return ERROR;
    } catch (const query::QueryEngineException &e) {
      db_accessor->abort();
      session.encoder_.MessageFailure(
          {{"code", "Memgraph.QueryEngineException"},
           {"message", "Query engine was unable to execute the query"}});
      return ERROR;
    } catch (const StacktraceException &e) {
      db_accessor->abort();
      session.encoder_.MessageFailure({{"code", "Memgraph.StacktraceException"},
                                       {"message", "Unknown exception"}});
      return ERROR;
    } catch (std::exception &e) {
      db_accessor->abort();
      session.encoder_.MessageFailure(
          {{"code", "Memgraph.Exception"}, {"message", "Unknown exception"}});
      return ERROR;
    }
    // TODO (mferencevic): finish the error handling, cover all exceptions
    //                     which can be raised from query engine
    //     * [abort, MessageFailure, return ERROR] should be extracted into 
    //       separate function (or something equivalent)
    //
    // !! QUERY ENGINE -> RUN METHOD -> EXCEPTION HANDLING !!

  } else if (message_type == MessageCode::PullAll) {
    logger.trace("[PullAll]");
    session.encoder_buffer_.Flush();
  } else if (message_type == MessageCode::DiscardAll) {
    logger.trace("[DiscardAll]");

    // TODO: discard state
    // TODO: write_success, send
    session.encoder_.MessageSuccess();
  } else if (message_type == MessageCode::Reset) {
    // TODO: rollback current transaction
    // discard all records waiting to be sent
    return EXECUTOR;
  } else {
    logger.error("Unrecognized message recieved");
    logger.debug("Invalid message type 0x{:02X}", message_type);

    return ERROR;
  }

  return EXECUTOR;
}
}
