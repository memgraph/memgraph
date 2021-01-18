#include "bolt_common.hpp"
#include "communication/bolt/v1/decoder/chunked_decoder_buffer.hpp"
#include "communication/buffer.hpp"

constexpr const int SIZE = 131072;
uint8_t data[SIZE];

using BufferT = communication::Buffer;
using StreamBufferT = io::network::StreamBuffer;
using DecoderBufferT =
    communication::bolt::ChunkedDecoderBuffer<BufferT::ReadEnd>;
using ChunkStateT = communication::bolt::ChunkState;

TEST(BoltBuffer, CorrectChunk) {
  uint8_t tmp[2000];
  BufferT buffer;
  DecoderBufferT decoder_buffer(*buffer.read_end());
  StreamBufferT sb = buffer.write_end()->Allocate();

  sb.data[0] = 0x03;
  sb.data[1] = 0xe8;
  memcpy(sb.data + 2, data, 1000);
  sb.data[1002] = 0;
  sb.data[1003] = 0;
  buffer.write_end()->Written(1004);

  ASSERT_EQ(decoder_buffer.GetChunk(), ChunkStateT::Whole);
  ASSERT_EQ(decoder_buffer.GetChunk(), ChunkStateT::Done);

  ASSERT_EQ(decoder_buffer.Read(tmp, 1000), true);
  for (int i = 0; i < 1000; ++i) EXPECT_EQ(data[i], tmp[i]);

  ASSERT_EQ(buffer.read_end()->size(), 0);
}

TEST(BoltBuffer, CorrectChunkTrailingData) {
  uint8_t tmp[2000];
  BufferT buffer;
  DecoderBufferT decoder_buffer(*buffer.read_end());
  StreamBufferT sb = buffer.write_end()->Allocate();

  sb.data[0] = 0x03;
  sb.data[1] = 0xe8;
  memcpy(sb.data + 2, data, 2002);
  sb.data[1002] = 0;
  sb.data[1003] = 0;
  buffer.write_end()->Written(2004);

  ASSERT_EQ(decoder_buffer.GetChunk(), ChunkStateT::Whole);
  ASSERT_EQ(decoder_buffer.GetChunk(), ChunkStateT::Done);

  ASSERT_EQ(decoder_buffer.Read(tmp, 1000), true);
  for (int i = 0; i < 1000; ++i) EXPECT_EQ(data[i], tmp[i]);

  uint8_t *leftover = buffer.read_end()->data();
  ASSERT_EQ(buffer.read_end()->size(), 1000);
  for (int i = 0; i < 1000; ++i) EXPECT_EQ(data[i + 1002], leftover[i]);
}

TEST(BoltBuffer, GraduallyPopulatedChunk) {
  uint8_t tmp[2000];
  BufferT buffer;
  DecoderBufferT decoder_buffer(*buffer.read_end());
  StreamBufferT sb = buffer.write_end()->Allocate();

  sb.data[0] = 0x03;
  sb.data[1] = 0xe8;
  buffer.write_end()->Written(2);

  for (int i = 0; i < 5; ++i) {
    ASSERT_EQ(decoder_buffer.GetChunk(), ChunkStateT::Partial);
    sb = buffer.write_end()->Allocate();
    memcpy(sb.data, data + 200 * i, 200);
    buffer.write_end()->Written(200);
  }

  sb = buffer.write_end()->Allocate();
  sb.data[0] = 0;
  sb.data[1] = 0;
  buffer.write_end()->Written(2);
  ASSERT_EQ(decoder_buffer.GetChunk(), ChunkStateT::Whole);
  ASSERT_EQ(decoder_buffer.GetChunk(), ChunkStateT::Done);

  ASSERT_EQ(decoder_buffer.Read(tmp, 1000), true);
  for (int i = 0; i < 1000; ++i) EXPECT_EQ(data[i], tmp[i]);

  ASSERT_EQ(buffer.read_end()->size(), 0);
}

TEST(BoltBuffer, GraduallyPopulatedChunkTrailingData) {
  uint8_t tmp[2000];
  BufferT buffer;
  DecoderBufferT decoder_buffer(*buffer.read_end());
  StreamBufferT sb = buffer.write_end()->Allocate();

  sb.data[0] = 0x03;
  sb.data[1] = 0xe8;
  buffer.write_end()->Written(2);

  for (int i = 0; i < 5; ++i) {
    ASSERT_EQ(decoder_buffer.GetChunk(), ChunkStateT::Partial);
    sb = buffer.write_end()->Allocate();
    memcpy(sb.data, data + 200 * i, 200);
    buffer.write_end()->Written(200);
  }

  sb = buffer.write_end()->Allocate();
  sb.data[0] = 0;
  sb.data[1] = 0;
  buffer.write_end()->Written(2);

  sb = buffer.write_end()->Allocate();
  memcpy(sb.data, data, 1000);
  buffer.write_end()->Written(1000);

  ASSERT_EQ(decoder_buffer.GetChunk(), ChunkStateT::Whole);
  ASSERT_EQ(decoder_buffer.GetChunk(), ChunkStateT::Done);

  ASSERT_EQ(decoder_buffer.Read(tmp, 1000), true);
  for (int i = 0; i < 1000; ++i) EXPECT_EQ(data[i], tmp[i]);

  uint8_t *leftover = buffer.read_end()->data();
  ASSERT_EQ(buffer.read_end()->size(), 1000);
  for (int i = 0; i < 1000; ++i) EXPECT_EQ(data[i], leftover[i]);
}

int main(int argc, char **argv) {
  InitializeData(data, SIZE);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
