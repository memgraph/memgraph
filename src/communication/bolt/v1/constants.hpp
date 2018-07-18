#pragma once

#include <cstddef>

namespace communication::bolt {

/**
 * Sizes related to the chunk defined in Bolt protocol.
 */
static constexpr size_t kChunkHeaderSize = 2;
static constexpr size_t kChunkMaxDataSize = 65535;
static constexpr size_t kChunkWholeSize = kChunkHeaderSize + kChunkMaxDataSize;

/**
 * Handshake size defined in the Bolt protocol.
 */
static constexpr size_t kHandshakeSize = 20;
}  // namespace communication::bolt
