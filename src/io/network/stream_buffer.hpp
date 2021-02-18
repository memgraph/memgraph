#pragma once

#include <cstdint>

namespace io::network {

/**
 * StreamBuffer
 * Used for getting a pointer and size of a preallocated block of memory.
 * The network stack than uses this block of memory to read data from a
 * socket.
 */
struct StreamBuffer {
  uint8_t *data;
  size_t len;
};
}  // namespace io::network
