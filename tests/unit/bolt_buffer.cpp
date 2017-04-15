#include "bolt_common.hpp"
#include "communication/bolt/v1/decoder/buffer.hpp"

constexpr const int SIZE = 4096;
uint8_t data[SIZE];

using BufferT = communication::bolt::Buffer<>;
using StreamBufferT = io::network::StreamBuffer;

TEST(BoltBuffer, AllocateAndWritten) {
  BufferT buffer;
  StreamBufferT sb = buffer.Allocate();

  memcpy(sb.data, data, 1000);
  buffer.Written(1000);

  ASSERT_EQ(buffer.size(), 1000);

  uint8_t *tmp = buffer.data();
  for (int i = 0; i < 1000; ++i)
    EXPECT_EQ(data[i], tmp[i]);
}

TEST(BoltBuffer, Shift) {
  BufferT buffer;
  StreamBufferT sb = buffer.Allocate();

  memcpy(sb.data, data, 1000);
  buffer.Written(1000);

  sb = buffer.Allocate();
  memcpy(sb.data, data + 1000, 1000);
  buffer.Written(1000);

  ASSERT_EQ(buffer.size(), 2000);

  uint8_t *tmp = buffer.data();
  for (int i = 0; i < 1000; ++i)
    EXPECT_EQ(data[i], tmp[i]);

  buffer.Shift(1000);
  ASSERT_EQ(buffer.size(), 1000);
  tmp = buffer.data();

  for (int i = 0; i < 1000; ++i)
    EXPECT_EQ(data[i + 1000], tmp[i]);
}

int main(int argc, char **argv) {
  InitializeData(data, SIZE);
  logging::init_sync();
  logging::log->pipe(std::make_unique<Stdout>());
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
