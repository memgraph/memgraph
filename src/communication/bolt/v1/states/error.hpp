#pragma once

#include <fmt/format.h>
#include <glog/logging.h>

#include "communication/bolt/v1/codes.hpp"
#include "communication/bolt/v1/state.hpp"
#include "communication/bolt/v1/value.hpp"
#include "utils/cast.hpp"

namespace communication::bolt {

/**
 * Error state run function
 * This function handles a Bolt session when it is in an error state.
 * The error state is exited upon receiving an ACK_FAILURE or RESET message.
 * @param session the session that should be used for the run
 */
template <typename TSession>
State StateErrorRun(TSession &session, State state) {
  Marker marker;
  Signature signature;
  if (!session.decoder_.ReadMessageHeader(&signature, &marker)) {
    DLOG(WARNING) << "Missing header data!";
    return State::Close;
  }

  // Clear the data buffer if it has any leftover data.
  session.encoder_buffer_.Clear();

  if ((session.version_.major == 1 && signature == Signature::AckFailure) ||
      signature == Signature::Reset) {
    if (signature == Signature::AckFailure) {
      DLOG(INFO) << "AckFailure received";
    } else {
      DLOG(INFO) << "Reset received";
    }

    // From version 4.0 client doesn't expect response message for a RESET
    if (session.version_.major == 1) {
      if (!session.encoder_.MessageSuccess()) {
        DLOG(WARNING) << "Couldn't send success message!";
        return State::Close;
      }
    }

    if (signature == Signature::Reset) {
      session.Abort();
      return State::Idle;
    }

    // We got AckFailure get back to right state.
    CHECK(state == State::Error) << "Shouldn't happen";
    return State::Idle;
  } else {
    uint8_t value = utils::UnderlyingCast(marker);

    // All bolt client messages have less than 15 parameters so if we receive
    // anything than a TinyStruct it's an error.
    if ((value & 0xF0) != utils::UnderlyingCast(Marker::TinyStruct)) {
      DLOG(WARNING) << fmt::format(
          "Expected TinyStruct marker, but received 0x{:02X}!", value);
      return State::Close;
    }

    // We need to clean up all parameters from this command.
    value &= 0x0F;  // The length is stored in the lower nibble.
    Value dv;
    for (int i = 0; i < value; ++i) {
      if (!session.decoder_.ReadValue(&dv)) {
        DLOG(WARNING) << fmt::format("Couldn't clean up parameter {} / {}!", i,
                                     value);
        return State::Close;
      }
    }

    // Ignore this message.
    if (!session.encoder_.MessageIgnored()) {
      DLOG(WARNING) << "Couldn't send ignored message!";
      return State::Close;
    }

    // Cleanup done, command ignored, stay in error state.
    return state;
  }
}
}  // namespace communication::bolt
