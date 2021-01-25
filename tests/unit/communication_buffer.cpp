#include "bolt_common.hpp"
#include "communication/buffer.hpp"

constexpr const int SIZE = 4096;
uint8_t data[SIZE];

using communication::Buffer;

struct CommunicationBuffer : ::testing::Test {
  // In newer gtest library (1.8.1+) this is changed to SetUpTestSuite
  static void SetUpTestCase() { InitializeData(data, SIZE); }
};

TEST_F(CommunicationBuffer, AllocateAndWritten) {
  Buffer buffer;
  auto sb = buffer.write_end()->Allocate();

  memcpy(sb.data, data, 1000);
  buffer.write_end()->Written(1000);

  ASSERT_EQ(buffer.read_end()->size(), 1000);

  uint8_t *tmp = buffer.read_end()->data();
  for (int i = 0; i < 1000; ++i) EXPECT_EQ(data[i], tmp[i]);
}

TEST_F(CommunicationBuffer, Shift) {
  Buffer buffer;
  auto sb = buffer.write_end()->Allocate();

  memcpy(sb.data, data, 1000);
  buffer.write_end()->Written(1000);

  sb = buffer.write_end()->Allocate();
  memcpy(sb.data, data + 1000, 1000);
  buffer.write_end()->Written(1000);

  ASSERT_EQ(buffer.read_end()->size(), 2000);

  uint8_t *tmp = buffer.read_end()->data();
  for (int i = 0; i < 1000; ++i) EXPECT_EQ(data[i], tmp[i]);

  buffer.read_end()->Shift(1000);
  ASSERT_EQ(buffer.read_end()->size(), 1000);
  tmp = buffer.read_end()->data();

  for (int i = 0; i < 1000; ++i) EXPECT_EQ(data[i + 1000], tmp[i]);
}

TEST_F(CommunicationBuffer, Resize) {
  Buffer buffer;
  auto sb = buffer.write_end()->Allocate();

  buffer.read_end()->Resize(sb.len + 1000);

  auto sbn = buffer.write_end()->Allocate();
  ASSERT_EQ(sb.len + 1000, sbn.len);
}
