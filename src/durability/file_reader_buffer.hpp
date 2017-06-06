#pragma once

#include <fstream>
#include "durability/summary.hpp"
#include "hasher.hpp"
#include "utils/bswap.hpp"

/**
 * Buffer reads data from file and calculates hash of read data. Implements
 * template param Buffer interface from BaseDecoder class. Should be closed
 * before destructing.
 */
class FileReaderBuffer {
 public:
  /**
   * Opens ifstream to a file, resets hash and reads summary. Returns false if
   * opening fails.
   * @param file:
   *    path to a file to which should be read.
   * @param summary:
   *    reference to a summary object where summary should be written.
   */
  bool Open(const std::string &file, snapshot::Summary &summary) {
    input_stream_.open(file);
    if (input_stream_.fail()) return false;
    return ReadSummary(summary);
  }

  /**
   * Closes ifstream. Returns false if closing fails.
   */
  bool Close() {
    input_stream_.close();
    return !input_stream_.fail();
  }

  /**
   * Reads data from stream.
   * @param data:
   *    pointer where data should be stored.
   * @param n:
   *    data length.
   */
  bool Read(uint8_t *data, size_t n) {
    input_stream_.read(reinterpret_cast<char *>(data), n);
    if (input_stream_.fail()) return false;
    hasher_.Update(data, n);
    return true;
  }
  /**
   * Returns hash of read data.
   */
  uint64_t hash() const { return hasher_.hash(); }

 private:
  /**
   * Reads type T from buffer. Data is written in buffer in big endian format.
   * Expected system endianness is little endian.
   */
  template <typename T>
  bool ReadType(T &val) {
    if (!Read(reinterpret_cast<uint8_t *>(&val), sizeof(T))) return false;
    // TODO: must be platform specific in the future
    val = bswap(val);
    return true;
  }

  /**
   * Reads summary from the end of a file and resets hash. Method should be
   * called after ifstream opening. Stream starts reading data from the
   * beginning of file in the next read call. Returns false if reading fails.
   * @param summary:
   *    reference to a summary object where summary should be written.
   */
  bool ReadSummary(snapshot::Summary &summary) {
    debug_assert(input_stream_.tellg() == 0,
                 "Summary should be read before other data!");
    input_stream_.seekg(-static_cast<int64_t>(sizeof(snapshot::Summary)),
                        std::ios::end);
    if (input_stream_.fail()) return false;
    if (!ReadType(summary.vertex_num_) || !ReadType(summary.edge_num_) ||
        !ReadType(summary.hash_))
      return false;
    input_stream_.seekg(0, std::ios::beg);
    hasher_.Reset();
    return !input_stream_.fail();
  }

  /**
   * Used for calculating hash of read data.
   */
  Hasher hasher_;
  /**
   * Ifstream used for reading from file.
   */
  std::ifstream input_stream_;
};
