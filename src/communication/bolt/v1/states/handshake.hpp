#pragma once

#include <glog/logging.h>

#include "communication/bolt/v1/constants.hpp"
#include "communication/bolt/v1/state.hpp"

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
  auto precmp = memcmp(session.buffer_.data(), preamble, sizeof(preamble));
  if (UNLIKELY(precmp != 0)) {
    DLOG(WARNING) << "Received a wrong preamble!";
    return State::Close;
  }

  // TODO so far we only support version 1 of the protocol so it doesn't
  // make sense to check which version the client prefers
  // this will change in the future

  if (!session.socket_.Write(protocol, sizeof(protocol))) {
    DLOG(WARNING) << "Couldn't write handshake response!";
    return State::Close;
  }
  session.connected_ = true;

  // Delete data from buffer. It is guaranteed that there will more than, or
  // equal to 20 bytes (HANDSHAKE_SIZE) in the buffer.
  session.buffer_.Shift(HANDSHAKE_SIZE);

  return State::Init;
}
}
