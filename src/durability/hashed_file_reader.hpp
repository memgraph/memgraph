#pragma once

#include <fstream>

#include "hasher.hpp"
#include "utils/bswap.hpp"

/**
 * Buffer reads data from file and calculates hash of read data. Implements
 * template param Buffer interface from BaseDecoder class.
 */
class HashedFileReader {
 public:
  /** Opens the file for reading. Returns true if successful. */
  bool Open(const std::string &file) {
    input_stream_.open(file, std::ios::in | std::ios::binary);
    hasher_ = Hasher();
    return !input_stream_.fail();
  }

  /** Closes ifstream. Returns false if closing fails. */
  bool Close() {
    input_stream_.close();
    return !input_stream_.fail();
  }

  /**
   * Reads raw data from stream.
   *
   * @param data - pointer to where data should be stored.
   * @param n - data length.
   * @param hash - If the read should be included in the hash calculation.
   */
  bool Read(uint8_t *data, size_t n, bool hash = true) {
    input_stream_.read(reinterpret_cast<char *>(data), n);
    if (input_stream_.fail()) return false;
    if (hash) hasher_.Update(data, n);
    return true;
  }

  /**
   * Reads a TValue value from the stream.
   *
   * @param val - The value to read into.
   * @param hash - If the read should be included in the hash calculation.
   * @tparam TValue - Type of value being read.
   * @return - If the read was successful.
   */
  template <typename TValue>
  bool ReadType(TValue &val, bool hash = true) {
    if (!Read(reinterpret_cast<uint8_t *>(&val), sizeof(TValue), hash))
      return false;
    // TODO: must be platform specific in the future
    val = utils::Bswap(val);
    return true;
  }

  void Seek(std::streamoff offset, std::ios_base::seekdir way) {
    input_stream_.seekg(offset, way);
  }

  void Seek(std::streampos pos) { input_stream_.seekg(pos); }

  auto Tellg() { return input_stream_.tellg(); }

  /** Returns the hash of the data read so far from the stream. */
  uint64_t hash() const { return hasher_.hash(); }

 private:
  Hasher hasher_;
  std::ifstream input_stream_;
};
