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
#include <algorithm>
#include <cstdint>
#include <iterator>

#include "communication/bolt/v1/codes.hpp"
#include "communication/bolt/v1/constants.hpp"
#include "communication/bolt/v1/state.hpp"
#include "utils/likely.hpp"
#include "utils/logging.hpp"

namespace communication::bolt {

inline bool CopyProtocolInformationIfSupported(uint16_t version, uint8_t *protocol) {
  const auto *supported_version = std::find(std::begin(kSupportedVersions), std::end(kSupportedVersions), version);
  if (supported_version != std::end(kSupportedVersions)) {
    std::memcpy(protocol, &version, sizeof(version));
    return true;
  }
  return false;
}

inline bool CopyProtocolInformationIfSupportedWithOffset(auto data_position, uint8_t *protocol) {
  struct bolt_range_version {
    uint8_t offset;
    uint8_t minor;
    uint8_t major;
  } bolt_range_version;
  std::memcpy(&bolt_range_version, data_position, sizeof(bolt_range_version));
  if (bolt_range_version.major == 0 || bolt_range_version.minor == 0) return false;
  bolt_range_version.offset = std::min(bolt_range_version.offset, bolt_range_version.minor);

  for (uint8_t i{0U}; i <= bolt_range_version.offset; i++) {
    uint8_t current_minor = bolt_range_version.minor - i;
    if (CopyProtocolInformationIfSupported(static_cast<uint16_t>((bolt_range_version.major << 8U) + current_minor),
                                           protocol)) {
      return true;
    }
  }
  return false;
}

/**
 * Handshake state run function
 * This function runs everything to make a Bolt handshake with the client.
 * @param session the session that should be used for the run
 */
template <typename TSession>
State StateHandshakeRun(TSession &session) {
  auto precmp = std::memcmp(session.input_stream_.data(), kPreamble, sizeof(kPreamble));
  if (precmp != 0) [[unlikely]] {
    spdlog::trace("Received a wrong preamble!");
    return State::Close;
  }

  DMG_ASSERT(session.input_stream_.size() >= kHandshakeSize, "Wrong size of the handshake data!");

  auto dataPosition = session.input_stream_.data() + sizeof(kPreamble);
  uint8_t protocol[4] = {0x00};

  for (int i = 0; i < 4 && !protocol[3]; ++i) {
    // If there is an offset defined (e.g. 0x00 0x03 0x03 0x04) the second byte
    // That would enable the client to pick between 4.0 and 4.3 versions
    // as per changes in handshake bolt protocol in v4.3
    if (CopyProtocolInformationIfSupportedWithOffset(dataPosition + 1, protocol + 2)) break;

    dataPosition += 2;  // version is defined only by the last 2 bytes
    uint16_t version{0};
    std::memcpy(&version, dataPosition, sizeof(version));
    if (!version) {
      break;
    }
    if (CopyProtocolInformationIfSupported(version, protocol + 2)) {
      break;
    }

    dataPosition += 2;
  }

  session.version_.minor = protocol[2];
  session.version_.major = protocol[3];
  if (!session.version_.major) {
    spdlog::trace("Server doesn't support any of the requested versions!");
    return State::Close;
  }

  if (!session.output_stream_.Write(protocol, sizeof(protocol))) {
    spdlog::trace("Couldn't write handshake response!");
    return State::Close;
  }

  spdlog::info("Using version {}.{} of protocol", session.version_.major, session.version_.minor);

  // Delete data from the input stream. It is guaranteed that there will more
  // than, or equal to 20 bytes (kHandshakeSize) in the buffer.
  session.input_stream_.Shift(kHandshakeSize);

  return State::Init;
}
}  // namespace communication::bolt
