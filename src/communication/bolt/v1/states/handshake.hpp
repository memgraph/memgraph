#pragma once

#include <glog/logging.h>

#include "communication/bolt/v1/codes.hpp"
#include "communication/bolt/v1/constants.hpp"
#include "communication/bolt/v1/state.hpp"

namespace communication::bolt {

/**
 * Handshake state run function
 * This function runs everything to make a Bolt handshake with the client.
 * @param session the session that should be used for the run
 */
template <typename TSession>
State StateHandshakeRun(TSession &session) {
  auto precmp = memcmp(session.buffer_.data(), kPreamble, sizeof(kPreamble));
  if (UNLIKELY(precmp != 0)) {
    DLOG(WARNING) << "Received a wrong preamble!";
    return State::Close;
  }

  // TODO so far we only support version 1 of the protocol so it doesn't
  // make sense to check which version the client prefers this will change in
  // the future.

  if (!session.timeout_socket_.Write(kProtocol, sizeof(kProtocol))) {
    DLOG(WARNING) << "Couldn't write handshake response!";
    return State::Close;
  }

  // Delete data from buffer. It is guaranteed that there will more than, or
  // equal to 20 bytes (HANDSHAKE_SIZE) in the buffer.
  session.buffer_.Shift(HANDSHAKE_SIZE);

  return State::Init;
}
}
