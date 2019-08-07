#pragma once

#include <fstream>

#include "hasher.hpp"
#include "utils/endian.hpp"

/**
 * Buffer that writes data to file and calculates hash of written data.
 * Implements template param Buffer interface from BaseEncoder class.
 *
 * All of the methods on a HashedFileWriter can throw an exception.
 */
class HashedFileWriter {
 public:
  /** Constructor, initialize ofstream to throw exception on fail. */
  HashedFileWriter() {
    output_stream_.exceptions(std::ifstream::failbit | std::ifstream::badbit);
  }

  /** Constructor which also takes a file path and opens it immediately. */
  explicit HashedFileWriter(const std::string &path) : HashedFileWriter() {
    output_stream_.open(path, std::ios::out | std::ios::binary);
  }

  /** Opens the writer */
  void Open(const std::string &path) {
    output_stream_.open(path, std::ios::out | std::ios::binary);
    hasher_ = Hasher();
  }

  /** Closes the writer. */
  void Close() { output_stream_.close(); }

  /**
   * Writes data to stream.
   *
   * @param data - Pointer to data to write.
   * @param n - Data length.
   * @param hash - If writing should update the hash.
   * @return - True if succesful.
   */
  void Write(const uint8_t *data, size_t n, bool hash = true) {
    output_stream_.write(reinterpret_cast<const char *>(data), n);
    if (hash) hasher_.Update(data, n);
  }

  /**
   * Writes a TValue to the stream.
   *
   * @param val - The value to write.
   * @param hash - If writing should update the hash.
   * @return - True if succesful.
   */
  template <typename TValue>
  void WriteValue(const TValue &val, bool hash = true) {
    TValue val_big = utils::HostToBigEndian(val);
    Write(reinterpret_cast<const uint8_t *>(&val_big), sizeof(TValue), hash);
  }

  // TODO try to remove before diff
  /** Does nothing. Just for API compatibility with the bolt buffer. */
  void Chunk() {}

  /** Flushes data to stream. */
  void Flush() { output_stream_.flush(); }

  /** Returns the hash of the data written so far to the stream. */
  uint64_t hash() const { return hasher_.hash(); }

 private:
  std::ofstream output_stream_;
  Hasher hasher_;
};
