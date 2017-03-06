#pragma once

#include "communication/bolt/v1/state.hpp"
#include "communication/bolt/v1/transport/bolt_decoder.hpp"
#include "communication/bolt/v1/serialization/record_stream.hpp"

#include "logging/default.hpp"

namespace bolt {

template<typename Socket>
State state_error_run(RecordStream<Socket> &output_stream, BoltDecoder &decoder) {
  Logger logger = logging::log->logger("State ERROR");
  logger.trace("Run");

  decoder.read_byte();
  auto message_type = decoder.read_byte();

  logger.trace("Message type byte is: {:02X}", message_type);

  if (message_type == MessageCode::PullAll) {
    output_stream.write_ignored();
    output_stream.chunk();
    output_stream.send();
    return ERROR;
  } else if (message_type == MessageCode::AckFailure) {
    // TODO reset current statement? is it even necessary?
    logger.trace("AckFailure received");

    output_stream.write_success_empty();
    output_stream.chunk();
    output_stream.send();

    return EXECUTOR;
  } else if (message_type == MessageCode::Reset) {
    // TODO rollback current transaction
    // discard all records waiting to be sent

    output_stream.write_success_empty();
    output_stream.chunk();
    output_stream.send();

    return EXECUTOR;
  }

  // TODO: write this as single call
  output_stream.write_ignored();
  output_stream.chunk();
  output_stream.send();

  return ERROR;
}
}
