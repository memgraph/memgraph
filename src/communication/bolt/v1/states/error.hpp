// Copyright 2021 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <fmt/format.h>

#include "communication/bolt/v1/codes.hpp"
#include "communication/bolt/v1/state.hpp"
#include "communication/bolt/v1/value.hpp"
#include "utils/cast.hpp"
#include "utils/likely.hpp"
#include "utils/logging.hpp"

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
    spdlog::trace("Missing header data!");
    return State::Close;
  }

  if (UNLIKELY(signature == Signature::Noop && session.version_.major == 4 && session.version_.minor == 1)) {
    spdlog::trace("Received NOOP message");
    return state;
  }

  // Clear the data buffer if it has any leftover data.
  session.encoder_buffer_.Clear();

  if ((session.version_.major == 1 && signature == Signature::AckFailure) || signature == Signature::Reset) {
    if (signature == Signature::AckFailure) {
      spdlog::trace("AckFailure received");
    } else {
      spdlog::trace("Reset received");
    }

    if (!session.encoder_.MessageSuccess()) {
      spdlog::trace("Couldn't send success message!");
      return State::Close;
    }

    if (signature == Signature::Reset) {
      session.Abort();
      return State::Idle;
    }

    // We got AckFailure get back to right state.
    MG_ASSERT(state == State::Error, "Shouldn't happen");
    return State::Idle;
  } else {
    uint8_t value = utils::UnderlyingCast(marker);

    // All bolt client messages have less than 15 parameters so if we receive
    // anything than a TinyStruct it's an error.
    if ((value & 0xF0) != utils::UnderlyingCast(Marker::TinyStruct)) {
      spdlog::trace("Expected TinyStruct marker, but received 0x{:02X}!", value);
      return State::Close;
    }

    // We need to clean up all parameters from this command.
    value &= 0x0F;  // The length is stored in the lower nibble.
    Value dv;
    for (int i = 0; i < value; ++i) {
      if (!session.decoder_.ReadValue(&dv)) {
        spdlog::trace("Couldn't clean up parameter {} / {}!", i, value);
        return State::Close;
      }
    }

    // Ignore this message.
    if (!session.encoder_.MessageIgnored()) {
      spdlog::trace("Couldn't send ignored message!");
      return State::Close;
    }

    // Cleanup done, command ignored, stay in error state.
    return state;
  }
}
}  // namespace communication::bolt
