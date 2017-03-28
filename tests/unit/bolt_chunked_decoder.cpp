#include <array>
#include <cassert>
#include <cstring>
#include <deque>
#include <iostream>
#include <vector>

#include "communication/bolt/v1/transport/chunked_decoder.hpp"
#include "gtest/gtest.h"

/**
 * DummyStream which is going to be used to test output data.
 */
struct DummyStream {
  /**
   * TODO (mferencevic): apply google style guide once decoder will be
   *                     refactored + document
   */
  void write(const uint8_t *values, size_t n) {
    data.insert(data.end(), values, values + n);
  }
  std::vector<uint8_t> data;
};
using DecoderT = communication::bolt::ChunkedDecoder<DummyStream>;

TEST(ChunkedDecoderTest, WriteString) {
  DummyStream stream;
  DecoderT decoder(stream);

  std::vector<uint8_t> chunks[] = {
      {0x00, 0x08, 'A', ' ', 'q', 'u', 'i', 'c', 'k', ' ', 0x00, 0x06, 'b', 'r',
       'o', 'w', 'n', ' '},
      {0x00, 0x0A, 'f', 'o', 'x', ' ', 'j', 'u', 'm', 'p', 's', ' '},
      {0x00, 0x07, 'o', 'v', 'e', 'r', ' ', 'a', ' '},
      {0x00, 0x08, 'l', 'a', 'z', 'y', ' ', 'd', 'o', 'g', 0x00, 0x00}};
  static constexpr size_t N = std::extent<decltype(chunks)>::value;

  for (size_t i = 0; i < N; ++i) {
    auto &chunk = chunks[i];
    logging::info("Chunk size: {}", chunk.size());

    const uint8_t *start = chunk.data();
    auto finished = decoder.decode(start, chunk.size());

    // break early if finished
    if (finished) break;
  }

  // check validity
  std::string decoded = "A quick brown fox jumps over a lazy dog";
  ASSERT_EQ(decoded.size(), stream.data.size());
  for (size_t i = 0; i < decoded.size(); ++i)
    ASSERT_EQ(decoded[i], stream.data[i]);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
