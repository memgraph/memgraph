#include "bolt_common.hpp"
#include "communication/bolt/v1/encoder/chunked_encoder_buffer.hpp"

// aliases
using SocketT = TestSocket;
using BufferT = communication::bolt::ChunkedEncoderBuffer<SocketT>;

// constants
using communication::bolt::CHUNK_HEADER_SIZE;
using communication::bolt::CHUNK_END_MARKER_SIZE;
using communication::bolt::MAX_CHUNK_SIZE;
using communication::bolt::WHOLE_CHUNK_SIZE;

// test data
constexpr const int TEST_DATA_SIZE = 100000;
uint8_t test_data[TEST_DATA_SIZE];

/**
 * Verifies a single chunk. The chunk should be constructed from header
 * (chunk size), data and end marker. The header is two bytes long number
 * written in big endian format. Data is array of elements from test_data
 * which max size is 0xFFFF. The end marker is always two bytes long array of
 * two zeros.
 *
 * @param data pointer on data array (array of bytes)
 * @param size of data array
 * @param offset offset from the begining of the test data
 * @param final_chunk if set to true then check for 0x00 0x00 after the chunk
 */
void VerifyChunkOfTestData(uint8_t *data, int size, uint64_t offset = 0,
                           bool final_chunk = true) {
  // first two bytes are size (big endian)
  uint8_t lower_byte = size & 0xFF;
  uint8_t higher_byte = (size & 0xFF00) >> 8;
  ASSERT_EQ(*data, higher_byte);
  ASSERT_EQ(*(data + 1), lower_byte);

  // in the data array should be size number of ones
  // the header is skipped
  for (auto i = 0; i < size; ++i) {
    ASSERT_EQ(data[i + CHUNK_HEADER_SIZE], test_data[i + offset]);
  }

  // last two bytes should be zeros
  // next to header and data
  if (final_chunk) {
    ASSERT_EQ(data[CHUNK_HEADER_SIZE + size], 0x00);
    ASSERT_EQ(data[CHUNK_HEADER_SIZE + size + 1], 0x00);
  }
}

TEST(BoltChunkedEncoderBuffer, OneSmallChunk) {
  int size = 100;

  // initialize tested buffer
  SocketT socket(10);
  BufferT buffer(socket);

  // write into buffer
  buffer.Write(test_data, size);
  buffer.Flush();

  // check the output array
  // the array should look like: [0, 100, first 100 bytes of test data, 0, 0]
  VerifyChunkOfTestData(socket.output.data(), size);
}

TEST(BoltChunkedEncoderBuffer, TwoSmallChunks) {
  int size1 = 100;
  int size2 = 200;

  // initialize tested buffer
  SocketT socket(10);
  BufferT buffer(socket);

  // write into buffer
  buffer.Write(test_data, size1);
  buffer.Chunk();
  buffer.Write(test_data + size1, size2);
  buffer.Flush();

  // check the output array
  // the output array should look like this:
  // [0, 100, first 100 bytes of test data, 0, 0] +
  // [0, 100, second 100 bytes of test data, 0, 0]
  auto data = socket.output.data();
  VerifyChunkOfTestData(data, size1);
  VerifyChunkOfTestData(
      data + CHUNK_HEADER_SIZE + size1 + CHUNK_END_MARKER_SIZE, size2, size1);
}

TEST(BoltChunkedEncoderBuffer, OneAndAHalfOfMaxChunk) {
  // initialize tested buffer
  SocketT socket(10);
  BufferT buffer(socket);

  // write into buffer
  buffer.Write(test_data, TEST_DATA_SIZE);
  buffer.Flush();

  // check the output array
  // the output array should look like this:
  // [0xFF, 0xFF, first 65535 bytes of test data,
  //  0x86, 0xA1, 34465 bytes of test data after the first 65535 bytes, 0, 0]
  auto output = socket.output.data();
  VerifyChunkOfTestData(output, MAX_CHUNK_SIZE, 0, false);
  VerifyChunkOfTestData(output + WHOLE_CHUNK_SIZE,
                        TEST_DATA_SIZE - MAX_CHUNK_SIZE, MAX_CHUNK_SIZE);
}

int main(int argc, char **argv) {
  InitializeData(test_data, TEST_DATA_SIZE);
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
