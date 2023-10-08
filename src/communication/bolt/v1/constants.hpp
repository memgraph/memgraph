// Copyright 2023 Memgraph Ltd.
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

#include <cstddef>
#include <cstdint>

namespace memgraph::communication::bolt {

/**
 * Sizes related to the chunk defined in Bolt protocol.
 */
inline constexpr size_t kChunkHeaderSize = 2;
inline constexpr size_t kChunkMaxDataSize = 65535;
inline constexpr size_t kChunkWholeSize = kChunkHeaderSize + kChunkMaxDataSize;

/**
 * Handshake size defined in the Bolt protocol.
 */
inline constexpr size_t kHandshakeSize = 20;

inline constexpr uint16_t kSupportedVersions[] = {0x0100, 0x0400, 0x0401, 0x0403, 0x0502};

inline constexpr int kPullAll = -1;
inline constexpr int kPullLast = -1;
}  // namespace memgraph::communication::bolt
