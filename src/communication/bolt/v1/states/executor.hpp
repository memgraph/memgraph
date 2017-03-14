#pragma once

#include <string>

#include "dbms/dbms.hpp"
#include "query/engine.hpp"
#include "communication/bolt/v1/states/executor.hpp"
#include "communication/bolt/v1/messaging/codes.hpp"
#include "communication/bolt/v1/state.hpp"

#include "logging/default.hpp"

namespace bolt {

struct Query {
  std::string statement;
};

template<typename Socket>
State state_executor_run(RecordStream<Socket> &output_stream, BoltDecoder &decoder, Dbms &dmbs, QueryEngine<RecordStream<Socket>> &query_engine){
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
      logger.debug("[ActiveDB] '{}'", db_accessor->name());

      auto is_successfully_executed =
          query_engine.Run(query.statement, *db_accessor, output_stream);

      if (!is_successfully_executed) {
        output_stream.write_failure(
            {{"code", "Memgraph.QueryExecutionFail"},
             {"message",
              "Query execution has failed (probably there is no "
              "element or there are some problems with concurrent "
              "access -> client has to resolve problems with "
              "concurrent access)"}});
        output_stream.send();
        return ERROR;
      }

      return EXECUTOR;
      // TODO: RETURN success MAYBE
    } catch (const frontend::opencypher::SyntaxException &e) {
      output_stream.write_failure(
          {{"code", "Memgraph.SyntaxException"}, {"message", "Syntax error"}});
      output_stream.send();
      return ERROR;
    // } catch (const backend::cpp::GeneratorException &e) {
    //   output_stream.write_failure(
    //       {{"code", "Memgraph.GeneratorException"},
    //        {"message", "Unsupported query"}});
    //   output_stream.send();
    //   return ERROR;
    } catch (const QueryEngineException &e) {
      output_stream.write_failure(
          {{"code", "Memgraph.QueryEngineException"},
           {"message", "Query engine was unable to execute the query"}});
      output_stream.send();
      return ERROR;
    } catch (const StacktraceException &e) {
      output_stream.write_failure(
          {{"code", "Memgraph.StacktraceException"},
           {"message", "Unknow exception"}});
      output_stream.send();
      return ERROR;
    } catch (std::exception &e) {
      output_stream.write_failure(
          {{"code", "Memgraph.Exception"}, {"message", "unknow exception"}});
      output_stream.send();
      return ERROR;
    }
  } else if (message_type == MessageCode::PullAll) {
    logger.trace("[PullAll]");
    output_stream.send();
  } else if (message_type == MessageCode::DiscardAll) {
    logger.trace("[DiscardAll]");

    // TODO: discard state

    output_stream.write_success();
    output_stream.chunk();
    output_stream.send();
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
