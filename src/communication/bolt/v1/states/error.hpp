#pragma once

#include "communication/bolt/v1/codes.hpp"
#include "communication/bolt/v1/state.hpp"
#include "logging/default.hpp"

namespace communication::bolt {

/**
 * Error state run function
 * This function handles a Bolt session when it is in an error state.
 * The error state is exited upon receiving an ACK_FAILURE or RESET message.
 * @param session the session that should be used for the run
 */
template <typename Session>
State StateErrorRun(Session &session) {
  static Logger logger = logging::log->logger("State ERROR");

  Marker marker;
  Signature signature;
  if (!session.decoder_.ReadMessageHeader(&signature, &marker)) {
    logger.debug("Missing header data!");
    return State::Close;
  }

  logger.trace("Message signature is: 0x{:02X}", underlying_cast(signature));

  // clear the data buffer if it has any leftover data
  session.encoder_buffer_.Clear();

  if (signature == Signature::AckFailure || signature == Signature::Reset) {
    if (signature == Signature::AckFailure)
      logger.trace("AckFailure received");
    else
      logger.trace("Reset received");

    if (!session.encoder_.MessageSuccess()) {
      logger.debug("Couldn't send success message!");
      return State::Close;
    }
    return State::Executor;
  } else {
    uint8_t value = underlying_cast(marker);

    // all bolt client messages have less than 15 parameters
    // so if we receive anything than a TinyStruct it's an error
    if ((value & 0xF0) != underlying_cast(Marker::TinyStruct)) {
      logger.debug("Expected TinyStruct marker, but received 0x{:02X}!", value);
      return State::Close;
    }

    // we need to clean up all parameters from this command
    value &= 0x0F;  // the length is stored in the lower nibble
    query::TypedValue tv;
    for (int i = 0; i < value; ++i) {
      if (!session.decoder_.ReadTypedValue(&tv)) {
        logger.debug("Couldn't clean up parameter {} / {}!", i, value);
        return State::Close;
      }
    }

    // ignore this message
    if (!session.encoder_.MessageIgnored()) {
      logger.debug("Couldn't send ignored message!");
      return State::Close;
    }

    // cleanup done, command ignored, stay in error state
    return State::Error;
  }
}
}
