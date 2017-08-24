#pragma once

#include <fmt/format.h>
#include <glog/logging.h>

#include "communication/bolt/v1/codes.hpp"
#include "communication/bolt/v1/decoder/decoded_value.hpp"
#include "communication/bolt/v1/state.hpp"
#include "query/typed_value.hpp"

namespace communication::bolt {

/**
 * Error state run function
 * This function handles a Bolt session when it is in an error state.
 * The error state is exited upon receiving an ACK_FAILURE or RESET message.
 * @param session the session that should be used for the run
 */
template <typename Session>
State StateErrorRun(Session &session, State state) {
  Marker marker;
  Signature signature;
  if (!session.decoder_.ReadMessageHeader(&signature, &marker)) {
    DLOG(INFO) << "Missing header data!";
    return State::Close;
  }

  DLOG(INFO) << fmt::format("Message signature is: 0x{:02X}",
                            underlying_cast(signature));

  // Clear the data buffer if it has any leftover data.
  session.encoder_buffer_.Clear();

  if (signature == Signature::AckFailure || signature == Signature::Reset) {
    if (signature == Signature::AckFailure) {
      DLOG(INFO) << "AckFailure received";
    } else {
      DLOG(INFO) << "Reset received";
    }

    if (!session.encoder_.MessageSuccess()) {
      DLOG(WARNING) << "Couldn't send success message!";
      return State::Close;
    }
    if (signature == Signature::Reset) {
      if (session.db_accessor_) {
        session.Abort();
      }
      return State::Idle;
    }

    // We got AckFailure get back to right state.
    if (state == State::ErrorIdle) {
      return State::Idle;
    } else if (state == State::ErrorWaitForRollback) {
      return State::WaitForRollback;
    } else {
      permanent_assert(false, "Shouldn't happen");
    }
  } else {
    uint8_t value = underlying_cast(marker);

    // all bolt client messages have less than 15 parameters
    // so if we receive anything than a TinyStruct it's an error
    if ((value & 0xF0) != underlying_cast(Marker::TinyStruct)) {
      DLOG(WARNING) << fmt::format(
          "Expected TinyStruct marker, but received 0x{:02X}!", value);
      return State::Close;
    }

    // we need to clean up all parameters from this command
    value &= 0x0F;  // the length is stored in the lower nibble
    DecodedValue dv;
    for (int i = 0; i < value; ++i) {
      if (!session.decoder_.ReadValue(&dv)) {
        DLOG(WARNING) << fmt::format("Couldn't clean up parameter {} / {}!", i,
                                     value);
        return State::Close;
      }
    }

    // ignore this message
    if (!session.encoder_.MessageIgnored()) {
      DLOG(WARNING) << "Couldn't send ignored message!";
      return State::Close;
    }

    // cleanup done, command ignored, stay in error state
    return state;
  }
}
}
