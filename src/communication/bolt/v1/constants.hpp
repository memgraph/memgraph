#pragma once

#include <cstddef>

namespace communication::bolt {

/**
 * Sizes related to the chunk defined in Bolt protocol.
 */
static constexpr size_t CHUNK_HEADER_SIZE = 2;
static constexpr size_t MAX_CHUNK_SIZE = 65535;
static constexpr size_t CHUNK_END_MARKER_SIZE = 2;
static constexpr size_t WHOLE_CHUNK_SIZE =
    CHUNK_HEADER_SIZE + MAX_CHUNK_SIZE + CHUNK_END_MARKER_SIZE;

/**
 * Handshake size defined in the Bolt protocol.
 */
static constexpr size_t HANDSHAKE_SIZE = 20;
}
