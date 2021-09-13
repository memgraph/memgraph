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
#include <iterator>

#include "communication/bolt/v1/codes.hpp"
#include "communication/bolt/v1/constants.hpp"
#include "communication/bolt/v1/state.hpp"
#include "utils/likely.hpp"
#include "utils/logging.hpp"

namespace communication::bolt {

inline bool FindCompatibleBoltVersion(uint16_t version, uint8_t *protocol) {
  const auto *supported_version = std::find(std::begin(kSupportedVersions), std::end(kSupportedVersions), version);
  if (supported_version != std::end(kSupportedVersions)) {
    std::memcpy(protocol + 2, &version, sizeof(version));
    return true;
  }
  return false;
}

inline bool FindCompatibleBoltVersionUsingOffset(auto data_position, uint8_t *protocol) {
  uint8_t version_offset{0};
  data_position += 1;
  std::memcpy(&version_offset, data_position, sizeof(uint8_t));

  if (!version_offset) {
    return false;
  }

  data_position += 1;
  uint16_t version = 0;
  std::memcpy(&version, data_position, sizeof(version));
  if (!version) {
    return false;
  }
  for (uint8_t i{0}; i <= version_offset; i++) {
    auto current_version = version - i;
    if (FindCompatibleBoltVersion(current_version, protocol)) {
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
  if (UNLIKELY(precmp != 0)) {
    spdlog::trace("Received a wrong preamble!");
    return State::Close;
  }

  DMG_ASSERT(session.input_stream_.size() >= kHandshakeSize, "Wrong size of the handshake data!");

  auto dataPosition = session.input_stream_.data() + sizeof(kPreamble);
  uint8_t protocol[4] = {0x00};

  // If there is an offset defined (e.g. 0x00 0x03 0x03 0x04) the second byte
  // That would enable the client to pick 4.0-4.3 versions
  // As per changes in handshake bolt protocol in v4.3
  if (!FindCompatibleBoltVersionUsingOffset(dataPosition, protocol)) {
    for (int i = 0; i < 4 && !protocol[3]; ++i) {
      dataPosition += 2;  // version is defined only by the last 2 bytes

      uint16_t version = 0;
      std::memcpy(&version, dataPosition, sizeof(version));
      if (!version) {
        break;
      }
      if (FindCompatibleBoltVersion(version, protocol)) {
        break;
      }

      dataPosition += 2;
    }
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
