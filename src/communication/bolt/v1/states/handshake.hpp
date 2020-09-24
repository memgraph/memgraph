#pragma once

#include <glog/logging.h>

#include <fmt/format.h>

#include "communication/bolt/v1/codes.hpp"
#include "communication/bolt/v1/constants.hpp"
#include "communication/bolt/v1/state.hpp"
#include "utils/likely.hpp"

namespace communication::bolt {

/**
 * Handshake state run function
 * This function runs everything to make a Bolt handshake with the client.
 * @param session the session that should be used for the run
 */
template <typename TSession>
State StateHandshakeRun(TSession &session) {
  auto precmp =
      memcmp(session.input_stream_.data(), kPreamble, sizeof(kPreamble));
  if (UNLIKELY(precmp != 0)) {
    DLOG(WARNING) << "Received a wrong preamble!";
    return State::Close;
  }

  DCHECK(session.input_stream_.size() >= kHandshakeSize)
      << "Wrong size of the handshake data!";

  auto dataPosition = session.input_stream_.data() + sizeof(kPreamble);

  uint8_t protocol[4] = {0x00};
  for (int i = 0; i < 4 && !protocol[3]; ++i) {
    dataPosition += 2;  // version is defined only by the last 2 bytes

    uint16_t version = 0;
    memcpy(&version, dataPosition, sizeof(version));
    if (!version) {
      break;
    }

    for (const auto supportedVersion : kSupportedVersions) {
      if (supportedVersion == version) {
        memcpy(protocol + 2, &version, sizeof(version));
        break;
      }
    }

    dataPosition += 2;
  }

  session.version_.minor = protocol[2];
  session.version_.major = protocol[3];
  if (!session.version_.major) {
    DLOG(WARNING) << "Server doesn't support any of the requested versions!";
    return State::Close;
  }

  if (!session.output_stream_.Write(protocol, sizeof(protocol))) {
    DLOG(WARNING) << "Couldn't write handshake response!";
    return State::Close;
  }

  // Delete data from the input stream. It is guaranteed that there will more
  // than, or equal to 20 bytes (kHandshakeSize) in the buffer.
  session.input_stream_.Shift(kHandshakeSize);

  return State::Init;
}
}  // namespace communication::bolt
