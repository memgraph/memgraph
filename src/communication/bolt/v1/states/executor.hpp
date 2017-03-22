#pragma once

#include <string>

#include "dbms/dbms.hpp"
#include "query/engine.hpp"
#include "communication/bolt/v1/states/executor.hpp"
#include "communication/bolt/v1/messaging/codes.hpp"
#include "communication/bolt/v1/state.hpp"

#include "logging/default.hpp"

namespace communication::bolt {

struct Query {
  std::string statement;
};

template<typename Socket>
State state_executor_run(ResultStream<Socket> &output_stream, Encoder<ChunkedBuffer<Socket>, Socket>& encoder, BoltDecoder &decoder, Dbms &dmbs, QueryEngine<ResultStream<Socket>> &query_engine){
  Logger logger = logging::log->logger("State EXECUTOR");
  // just read one byte that represents the struct type, we can skip the
  // information contained in this byte
  decoder.read_byte();

  logger.debug("Run");

  auto message_type = decoder.read_byte();

  if (message_type == MessageCode::Run) {
    Query query;

    query.statement = decoder.read_string();

    // TODO (mferencevic): refactor bolt exception handling
    try {
      logger.trace("[Run] '{}'", query.statement);

      auto db_accessor = dmbs.active();
      logger.debug("[ActiveDB] '{}'", db_accessor.name());

      auto is_successfully_executed =
          query_engine.Run(query.statement, db_accessor, output_stream);

      if (!is_successfully_executed) {
        // TODO: write_failure, send
        encoder.MessageFailure(
            {{"code", "Memgraph.QueryExecutionFail"},
             {"message",
              "Query execution has failed (probably there is no "
              "element or there are some problems with concurrent "
              "access -> client has to resolve problems with "
              "concurrent access)"}});
        return ERROR;
      }

      return EXECUTOR;
      // TODO: RETURN success MAYBE
    } catch (const frontend::opencypher::SyntaxException &e) {
      // TODO: write_failure, send
      encoder.MessageFailure(
          {{"code", "Memgraph.SyntaxException"}, {"message", "Syntax error"}});
      return ERROR;
    // } catch (const backend::cpp::GeneratorException &e) {
    //   output_stream.write_failure(
    //       {{"code", "Memgraph.GeneratorException"},
    //        {"message", "Unsupported query"}});
    //   output_stream.send();
    //   return ERROR;
    } catch (const QueryEngineException &e) {
      // TODO: write_failure, send
      encoder.MessageFailure(
          {{"code", "Memgraph.QueryEngineException"},
           {"message", "Query engine was unable to execute the query"}});
      return ERROR;
    } catch (const StacktraceException &e) {
      // TODO: write_failure, send
      encoder.MessageFailure(
          {{"code", "Memgraph.StacktraceException"},
           {"message", "Unknown exception"}});
      return ERROR;
    } catch (std::exception &e) {
      // TODO: write_failure, send
      encoder.MessageFailure(
          {{"code", "Memgraph.Exception"}, {"message", "Unknown exception"}});
      return ERROR;
    }
  } else if (message_type == MessageCode::PullAll) {
    logger.trace("[PullAll]");
    // TODO: all query output should not be immediately flushed from the buffer, it should wait the PullAll command to start flushing!!
    //output_stream.send();
  } else if (message_type == MessageCode::DiscardAll) {
    logger.trace("[DiscardAll]");

    // TODO: discard state
    // TODO: write_success, send
    encoder.MessageSuccess();
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
