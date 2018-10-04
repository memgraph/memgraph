#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace storage {

/**
 * Buffer used for serialization of disk properties. The buffer
 * implements a template parameter Buffer interface from BaseEncoder
 * and Decoder classes for bolt serialization.
 */
class PODBuffer {
 public:
  PODBuffer() = default;
  explicit PODBuffer(const std::string &s) {
    buffer = std::vector<uint8_t>{s.begin(), s.end()};
  }

  /**
   * Writes data to buffer
   *
   * @param data - Pointer to data to be written.
   * @param len - Data length.
   */
  void Write(const uint8_t *data, size_t len) {
    for (size_t i = 0; i < len; ++i) buffer.push_back(data[i]);
  }

  /**
   * Reads raw data from buffer.
   *
   * @param data - pointer to where data should be stored.
   * @param len - data length
   * @return - True if successful, False otherwise.
   */
  bool Read(uint8_t *data, size_t len) {
    if (len > buffer.size()) return false;
    memcpy(data, buffer.data(), len);
    buffer.erase(buffer.begin(), buffer.begin() + len);
    return true;
  }

  std::vector<uint8_t> buffer;
};

}  // namespace storage
