#pragma once

#include "communication/bolt/v1/messaging/codes.hpp"
#include "communication/bolt/v1/state.hpp"
#include "logging/default.hpp"

namespace communication::bolt {

/**
 * TODO (mferencevic): finish & document
 */
template <typename Session>
State StateErrorRun(Session &session) {
  static Logger logger = logging::log->logger("State ERROR");

  session.decoder_.read_byte();
  auto message_type = session.decoder_.read_byte();

  logger.trace("Message type byte is: {:02X}", message_type);

  if (message_type == MessageCode::PullAll) {
    session.encoder_.MessageIgnored();
    return ERROR;
  } else if (message_type == MessageCode::AckFailure) {
    // TODO reset current statement? is it even necessary?
    logger.trace("AckFailure received");
    session.encoder_.MessageSuccess();
    return EXECUTOR;
  } else if (message_type == MessageCode::Reset) {
    // TODO rollback current transaction
    // discard all records waiting to be sent
    session.encoder_.MessageSuccess();
    return EXECUTOR;
  }
  session.encoder_.MessageIgnored();
  return ERROR;
}
}
