#include <glog/logging.h>
#include <gtest/gtest.h>

#include "storage/pod_buffer.hpp"

class PODBufferTest : public ::testing::Test {
 protected:
  storage::PODBuffer buffer_;

  void SetUp() override { buffer_ = storage::PODBuffer(""); }

  void Write(const uint8_t *data, size_t len) { buffer_.Write(data, len); }

  bool Read(uint8_t *data, size_t len) { return buffer_.Read(data, len); }
};

TEST_F(PODBufferTest, ReadEmpty) {
  uint8_t data[10];
  ASSERT_TRUE(Read(data, 0));
  for (int i = 1; i <= 5; ++i) ASSERT_FALSE(Read(data, i));
}

TEST_F(PODBufferTest, ReadNonEmpty) {
  uint8_t input_data[10];
  uint8_t output_data[10];

  for (int i = 0; i < 10; ++i) input_data[i] = i;

  Write(input_data, 10);
  ASSERT_TRUE(Read(output_data, 10));

  for (int i = 0; i < 10; ++i) ASSERT_EQ(output_data[i], i);

  ASSERT_FALSE(Read(output_data, 1));
}

TEST_F(PODBufferTest, WriteRead) {
  uint8_t input_data[10];
  uint8_t output_data[10];

  for (int i = 0; i < 10; ++i) input_data[i] = i;

  Write(input_data, 10);
  ASSERT_TRUE(Read(output_data, 5));

  for (int i = 0; i < 5; ++i) ASSERT_EQ(output_data[i], i);

  ASSERT_TRUE(Read(output_data, 5));

  for (int i = 0; i < 5; ++i) ASSERT_EQ(output_data[i], i + 5);

  ASSERT_FALSE(Read(output_data, 1));

  Write(input_data + 5, 5);
  ASSERT_TRUE(Read(output_data, 5));

  for (int i = 0; i < 5; ++i) ASSERT_EQ(output_data[i], i + 5);

  ASSERT_FALSE(Read(output_data, 1));
}
