#define NDEBUG
#include "bolt_common.hpp"

#include "communication/bolt/v1/encoder/chunked_buffer.hpp"


constexpr const int SIZE = 131072;
uint8_t data[SIZE];


void verify_output(std::vector<uint8_t>& output, const uint8_t* data, uint64_t size) {
  uint64_t len = 0, pos = 0;
  uint8_t tail[2] = { 0, 0 };
  uint16_t head;
  while (size > 0) {
    head = len = std::min(size, communication::bolt::CHUNK_SIZE);
    head = bswap(head);
    check_output(output, reinterpret_cast<uint8_t *>(&head), sizeof(head), false);
    check_output(output, data + pos, len, false);
    check_output(output, tail, 2, false);
    size -= len;
    pos += len;
  }
  check_output(output, nullptr, 0, true);
}

TEST(Bolt, ChunkedBuffer) {
  TestSocket socket(10);
  communication::bolt::ChunkedBuffer<TestSocket> chunked_buffer(socket);
  std::vector<uint8_t>& output = socket.output;

  for (int i = 0; i <= SIZE; i += 16) {
    chunked_buffer.Write(data, i);
    chunked_buffer.Flush();
    verify_output(output, data, i);
  }
}


int main(int argc, char** argv) {
  initialize_data(data, SIZE);
  logging::init_sync();
  logging::log->pipe(std::make_unique<Stdout>());
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
