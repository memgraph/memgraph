#pragma once

#include "communication/bolt/v1/encoder/result_stream.hpp"
#include "communication/bolt/v1/messaging/codes.hpp"
#include "communication/bolt/v1/packing/codes.hpp"
#include "communication/bolt/v1/state.hpp"
#include "communication/bolt/v1/transport/bolt_decoder.hpp"
#include "logging/default.hpp"
#include "utils/likely.hpp"

namespace communication::bolt {

/**
 * TODO (mferencevic): finish & document
 */
template <typename Session>
State StateInitRun(Session &session) {
  static Logger logger = logging::log->logger("State INIT");
  logger.debug("Parsing message");

  auto struct_type = session.decoder_.read_byte();

  if (UNLIKELY((struct_type & 0x0F) > pack::Rule::MaxInitStructSize)) {
    logger.debug("{}", struct_type);
    logger.debug(
        "Expected struct marker of max size 0x{:02} instead of 0x{:02X}",
        (unsigned)pack::Rule::MaxInitStructSize, (unsigned)struct_type);
    return NULLSTATE;
  }

  auto message_type = session.decoder_.read_byte();

  if (UNLIKELY(message_type != MessageCode::Init)) {
    logger.debug("Expected Init (0x01) instead of (0x{:02X})",
                 (unsigned)message_type);
    return NULLSTATE;
  }

  auto client_name = session.decoder_.read_string();

  if (struct_type == pack::Code::StructTwo) {
    // TODO process authentication tokens
  }

  logger.debug("Executing state");
  logger.debug("Client connected '{}'", client_name);

  // TODO: write_success, chunk, send
  session.encoder_.MessageSuccess();

  return EXECUTOR;
}
}
