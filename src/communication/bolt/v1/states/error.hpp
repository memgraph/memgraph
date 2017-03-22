#pragma once

#include "communication/bolt/v1/state.hpp"
#include "communication/bolt/v1/transport/bolt_decoder.hpp"
#include "communication/bolt/v1/encoder/result_stream.hpp"

#include "logging/default.hpp"

namespace communication::bolt {

template<typename Socket>
State state_error_run(ResultStream<Socket> &output_stream, Encoder<ChunkedBuffer<Socket>, Socket>& encoder, BoltDecoder &decoder) {
  Logger logger = logging::log->logger("State ERROR");
  logger.trace("Run");

  decoder.read_byte();
  auto message_type = decoder.read_byte();

  logger.trace("Message type byte is: {:02X}", message_type);

  if (message_type == MessageCode::PullAll) {
    // TODO: write_ignored, chunk, send
    encoder.MessageIgnored();
    return ERROR;
  } else if (message_type == MessageCode::AckFailure) {
    // TODO reset current statement? is it even necessary?
    logger.trace("AckFailure received");

    // TODO: write_success, chunk, send
    encoder.MessageSuccess();

    return EXECUTOR;
  } else if (message_type == MessageCode::Reset) {
    // TODO rollback current transaction
    // discard all records waiting to be sent

    // TODO: write_success, chunk, send
    encoder.MessageSuccess();

    return EXECUTOR;
  }

  // TODO: write_ignored, chunk, send
  encoder.MessageIgnored();

  return ERROR;
}
}
