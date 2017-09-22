#pragma once

#include <fstream>
#include "hasher.hpp"
#include "utils/bswap.hpp"

/**
 * Buffer that writes data to file and calculates hash of written data.
 * Implements template param Buffer interface from BaseEncoder class. Hash is
 * incremented when Write is called. If any ofstream operation fails,
 * std::ifstream::failure is thrown.
 */
class FileWriterBuffer {
 public:
  /**
   * Constructor, initialize ofstream to throw exception on fail.
   */
  FileWriterBuffer() {
    output_stream_.exceptions(std::ifstream::failbit | std::ifstream::badbit);
  }

  /**
   * Constructor which also takes a file path and opens it immediately.
   */
  FileWriterBuffer(const std::string &path) : FileWriterBuffer() { Open(path); }

  /**
   * Opens ofstream to file given in constructor.
   * @param file:
   *    path to ofstream file
   */
  void Open(const std::string &file) {
    output_stream_.open(file, std::ios::out | std::ios::binary);
  }

  /**
   * Closes ofstream.
   */
  void Close() { output_stream_.close(); }

  /**
   * Writes data to stream and increases hash.
   * @param data:
   *    pointer to data.
   * @param n:
   *    data length.
   */
  void Write(const uint8_t *data, size_t n) {
    hasher_.Update(data, n);
    output_stream_.write(reinterpret_cast<const char *>(data), n);
  }
  /**
   * BaseEncoder needs this method, it is not needed in this buffer.
   */
  void Chunk() {}
  /**
   * Flushes data to stream.
   */
  void Flush() { output_stream_.flush(); }

  /**
   * Writes summary to ofstream in big endian format. This method should be
   * called when writing all other data in the file is done. Returns true if
   * writing was successful.
   */
  void WriteSummary(int64_t vertex_num, int64_t edge_num) {
    debug_assert(vertex_num >= 0, "Number of vertices should't be negative");
    debug_assert(vertex_num >= 0, "Number of edges should't be negative");
    WriteLong(vertex_num);
    WriteLong(edge_num);
    WriteLong(hasher_.hash());
  }

 private:
  /**
   * Method writes uint64_t to ofstream.
   */
  void WriteLong(uint64_t val) {
    uint64_t bval = bswap(val);
    output_stream_.write(reinterpret_cast<const char *>(&bval), sizeof(bval));
  }

  /**
   * Stream used to write data to file.
   */
  std::ofstream output_stream_;
  /**
   * Used to calculate hash of written data.
   */
  Hasher hasher_;
};
