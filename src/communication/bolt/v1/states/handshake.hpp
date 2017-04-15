#pragma once

#include "communication/bolt/v1/state.hpp"
#include "logging/default.hpp"

namespace communication::bolt {

static constexpr uint8_t preamble[4] = {0x60, 0x60, 0xB0, 0x17};
static constexpr uint8_t protocol[4] = {0x00, 0x00, 0x00, 0x01};

/**
 * Handshake state run function
 * This function runs everything to make a Bolt handshake with the client.
 * @param session the session that should be used for the run
 */
template <typename Session>
State StateHandshakeRun(Session &session) {
  static Logger logger = logging::log->logger("State HANDSHAKE");

  auto precmp = memcmp(session.buffer_.data(), preamble, sizeof(preamble));
  if (UNLIKELY(precmp != 0)) {
    logger.debug("Received a wrong preamble!");
    return State::Close;
  }

  // TODO so far we only support version 1 of the protocol so it doesn't
  // make sense to check which version the client prefers
  // this will change in the future

  if (!session.socket_.Write(protocol, sizeof(protocol))) {
    logger.debug("Couldn't write handshake response!");
    return State::Close;
  }
  session.connected_ = true;

  // Delete data from buffer. It is guaranteed that there will be exactly
  // 20 bytes in the buffer so we can use buffer_.size() here.
  session.buffer_.Shift(session.buffer_.size());

  return State::Init;
}
}
