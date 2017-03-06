#pragma once

#include "communication/bolt/v1/state.hpp"
#include "communication/bolt/v1/transport/bolt_decoder.hpp"
#include "communication/bolt/v1/serialization/record_stream.hpp"
#include "communication/bolt/v1/messaging/codes.hpp"

#include "logging/default.hpp"
#include "utils/likely.hpp"

namespace bolt {

template<typename Socket>
State state_init_run(RecordStream<Socket> &output_stream, BoltDecoder &decoder) {
  Logger logger = logging::log->logger("State INIT");
  logger.debug("Parsing message");

  auto struct_type = decoder.read_byte();

  if (UNLIKELY((struct_type & 0x0F) > pack::Rule::MaxInitStructSize)) {
    logger.debug("{}", struct_type);

    logger.debug(
        "Expected struct marker of max size 0x{:02} instead of 0x{:02X}",
        (unsigned)pack::Rule::MaxInitStructSize, (unsigned)struct_type);

    return NULLSTATE;
  }

  auto message_type = decoder.read_byte();

  if (UNLIKELY(message_type != MessageCode::Init)) {
    logger.debug("Expected Init (0x01) instead of (0x{:02X})",
                 (unsigned)message_type);

    return NULLSTATE;
  }

  auto client_name = decoder.read_string();

  if (struct_type == pack::Code::StructTwo) {
    // TODO process authentication tokens
  }

  logger.debug("Executing state");
  logger.debug("Client connected '{}'", client_name);

  output_stream.write_success_empty();
  output_stream.chunk();
  output_stream.send();

  return EXECUTOR;
}

}
