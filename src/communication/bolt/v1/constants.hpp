#pragma once

#include <cstddef>
#include <cstdint>

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

static constexpr uint16_t kSupportedVersions[3] = {0x0100, 0x0400, 0x0401};

static constexpr int kPullAll = -1;
static constexpr int kPullLast = -1;
}  // namespace communication::bolt
