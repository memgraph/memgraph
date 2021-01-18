#include "bolt_common.hpp"
#include "bolt_testdata.hpp"

#include "communication/bolt/v1/decoder/decoder.hpp"

using communication::bolt::Value;

constexpr const int SIZE = 131072;
uint8_t data[SIZE];

/**
 * TestDecoderBuffer
 * This class provides a dummy Buffer used for testing the Decoder.
 * It's Read function is the necessary public interface for the Decoder.
 * It's Write and Clear methods are used for testing. Through the Write
 * method you can store data in the buffer, and throgh the Clear method
 * you can clear the buffer. The decoder uses the Read function to get
 * data from the buffer.
 */
class TestDecoderBuffer {
 public:
  bool Read(uint8_t *data, size_t len) {
    if (len > buffer_.size()) return false;
    memcpy(data, buffer_.data(), len);
    buffer_.erase(buffer_.begin(), buffer_.begin() + len);
    return true;
  }

  void Write(const uint8_t *data, size_t len) {
    for (size_t i = 0; i < len; ++i) buffer_.push_back(data[i]);
  }

  void Clear() { buffer_.clear(); }

 private:
  std::vector<uint8_t> buffer_;
};

using DecoderT = communication::bolt::Decoder<TestDecoderBuffer>;

TEST(BoltDecoder, NullAndBool) {
  TestDecoderBuffer buffer;
  DecoderT decoder(buffer);
  Value dv;

  // test null
  buffer.Write((const uint8_t *)"\xC0", 1);
  ASSERT_EQ(decoder.ReadValue(&dv), true);
  ASSERT_EQ(dv.type(), Value::Type::Null);

  // test true
  buffer.Write((const uint8_t *)"\xC3", 1);
  ASSERT_EQ(decoder.ReadValue(&dv), true);
  ASSERT_EQ(dv.type(), Value::Type::Bool);
  ASSERT_EQ(dv.ValueBool(), true);

  // test false
  buffer.Write((const uint8_t *)"\xC2", 1);
  ASSERT_EQ(decoder.ReadValue(&dv), true);
  ASSERT_EQ(dv.type(), Value::Type::Bool);
  ASSERT_EQ(dv.ValueBool(), false);
}

TEST(BoltDecoder, Int) {
  TestDecoderBuffer buffer;
  DecoderT decoder(buffer);

  Value dv;

  // test invalid marker
  buffer.Clear();
  buffer.Write((uint8_t *)"\xCD", 1);  // 0xCD is reserved in the protocol
  ASSERT_EQ(decoder.ReadValue(&dv), false);

  for (int i = 0; i < 28; ++i) {
    // test missing data
    buffer.Clear();
    buffer.Write(int_encoded[i], int_encoded_len[i] - 1);
    ASSERT_EQ(decoder.ReadValue(&dv), false);

    // test all ok
    buffer.Clear();
    buffer.Write(int_encoded[i], int_encoded_len[i]);
    ASSERT_EQ(decoder.ReadValue(&dv), true);
    ASSERT_EQ(dv.type(), Value::Type::Int);
    ASSERT_EQ(dv.ValueInt(), int_decoded[i]);
  }
}

TEST(BoltDecoder, Double) {
  TestDecoderBuffer buffer;
  DecoderT decoder(buffer);

  Value dv;

  for (int i = 0; i < 4; ++i) {
    // test missing data
    buffer.Clear();
    buffer.Write(double_encoded[i], 8);
    ASSERT_EQ(decoder.ReadValue(&dv), false);

    // test all ok
    buffer.Clear();
    buffer.Write(double_encoded[i], 9);
    ASSERT_EQ(decoder.ReadValue(&dv), true);
    ASSERT_EQ(dv.type(), Value::Type::Double);
    ASSERT_EQ(dv.ValueDouble(), double_decoded[i]);
  }
}

TEST(BoltDecoder, String) {
  TestDecoderBuffer buffer;
  DecoderT decoder(buffer);

  Value dv;

  uint8_t headers[][6] = {"\x8F", "\xD0\x0F", "\xD1\x00\x0F",
                          "\xD2\x00\x00\x00\x0F"};
  int headers_len[] = {1, 2, 3, 5};

  for (int i = 0; i < 4; ++i) {
    // test missing data in header
    buffer.Clear();
    buffer.Write(headers[i], headers_len[i] - 1);
    ASSERT_EQ(decoder.ReadValue(&dv), false);

    // test missing elements
    buffer.Clear();
    buffer.Write(headers[i], headers_len[i]);
    buffer.Write(data, 14);
    ASSERT_EQ(decoder.ReadValue(&dv), false);

    // test all ok
    buffer.Clear();
    buffer.Write(headers[i], headers_len[i]);
    buffer.Write(data, 15);
    ASSERT_EQ(decoder.ReadValue(&dv), true);
    ASSERT_EQ(dv.type(), Value::Type::String);
    std::string &str = dv.ValueString();
    for (int j = 0; j < 15; ++j) EXPECT_EQ((uint8_t)str[j], data[j]);
  }
}

TEST(BoltDecoder, StringLarge) {
  TestDecoderBuffer buffer;
  DecoderT decoder(buffer);
  Value dv;

  uint8_t header[6] = "\xD2\x00\x01\x86\xA0";

  // test missing data
  buffer.Clear();
  buffer.Write(header, 5);
  buffer.Write(data, 10);
  ASSERT_EQ(decoder.ReadValue(&dv), false);

  // test all ok
  buffer.Clear();
  buffer.Write(header, 5);
  buffer.Write(data, 100000);
  ASSERT_EQ(decoder.ReadValue(&dv), true);
  ASSERT_EQ(dv.type(), Value::Type::String);
  std::string &str = dv.ValueString();
  for (int j = 0; j < 100000; ++j) EXPECT_EQ((uint8_t)str[j], data[j]);
}

TEST(BoltDecoder, List) {
  TestDecoderBuffer buffer;
  DecoderT decoder(buffer);

  Value dv;

  uint8_t headers[][6] = {"\x9F", "\xD4\x0F", "\xD5\x00\x0F",
                          "\xD6\x00\x00\x00\x0F"};
  int headers_len[] = {1, 2, 3, 5};

  for (int i = 0; i < 4; ++i) {
    // test missing data in header
    buffer.Clear();
    buffer.Write(headers[i], headers_len[i] - 1);
    ASSERT_EQ(decoder.ReadValue(&dv), false);

    // test missing elements
    buffer.Clear();
    buffer.Write(headers[i], headers_len[i]);
    for (uint8_t j = 0; j < 14; ++j) buffer.Write(&j, 1);
    ASSERT_EQ(decoder.ReadValue(&dv), false);

    // test all ok
    buffer.Clear();
    buffer.Write(headers[i], headers_len[i]);
    for (uint8_t j = 0; j < 15; ++j) buffer.Write(&j, 1);
    ASSERT_EQ(decoder.ReadValue(&dv), true);
    ASSERT_EQ(dv.type(), Value::Type::List);
    std::vector<Value> &val = dv.ValueList();
    ASSERT_EQ(val.size(), 15);
    for (int j = 0; j < 15; ++j) EXPECT_EQ(val[j].ValueInt(), j);
  }
}

TEST(BoltDecoder, Map) {
  TestDecoderBuffer buffer;
  DecoderT decoder(buffer);

  Value dv;

  uint8_t headers[][6] = {"\xAF", "\xD8\x0F", "\xD9\x00\x0F",
                          "\xDA\x00\x00\x00\x0F"};
  int headers_len[] = {1, 2, 3, 5};

  uint8_t index[] = "\x81\x61";
  uint8_t wrong_index = 1;

  for (int i = 0; i < 4; ++i) {
    // test missing data in header
    buffer.Clear();
    buffer.Write(headers[i], headers_len[i] - 1);
    ASSERT_EQ(decoder.ReadValue(&dv), false);

    // test wrong index type
    buffer.Clear();
    buffer.Write(headers[i], headers_len[i]);
    buffer.Write(&wrong_index, 1);
    buffer.Write(&wrong_index, 1);
    ASSERT_EQ(decoder.ReadValue(&dv), false);

    // test missing element data
    buffer.Clear();
    buffer.Write(headers[i], headers_len[i]);
    buffer.Write(index, 2);
    ASSERT_EQ(decoder.ReadValue(&dv), false);

    // test missing elements
    buffer.Clear();
    buffer.Write(headers[i], headers_len[i]);
    for (uint8_t j = 0; j < 14; ++j) {
      buffer.Write(index, 2);
      buffer.Write(&j, 1);
    }
    ASSERT_EQ(decoder.ReadValue(&dv), false);

    // test elements with same index
    buffer.Clear();
    buffer.Write(headers[i], headers_len[i]);
    for (uint8_t j = 0; j < 15; ++j) {
      buffer.Write(index, 2);
      buffer.Write(&j, 1);
    }
    ASSERT_EQ(decoder.ReadValue(&dv), false);

    // test all ok
    buffer.Clear();
    buffer.Write(headers[i], headers_len[i]);
    for (uint8_t j = 0; j < 15; ++j) {
      uint8_t tmp = 'a' + j;
      buffer.Write(index, 1);
      buffer.Write(&tmp, 1);
      buffer.Write(&j, 1);
    }
    ASSERT_EQ(decoder.ReadValue(&dv), true);
    ASSERT_EQ(dv.type(), Value::Type::Map);
    std::map<std::string, Value> &val = dv.ValueMap();
    ASSERT_EQ(val.size(), 15);
    for (int j = 0; j < 15; ++j) {
      char tmp_chr = 'a' + j;
      Value tmp_dv = val[std::string(1, tmp_chr)];
      EXPECT_EQ(tmp_dv.type(), Value::Type::Int);
      EXPECT_EQ(tmp_dv.ValueInt(), j);
    }
  }
}

TEST(BoltDecoder, Vertex) {
  TestDecoderBuffer buffer;
  DecoderT decoder(buffer);

  Value dv;

  uint8_t header[] = "\xB3\x4E";
  uint8_t wrong_header[] = "\x00\x00";
  uint8_t test_int[] = "\x01";
  uint8_t test_str[] = "\x81\x61";
  uint8_t test_list[] = "\x91";
  uint8_t test_map[] = "\xA1";

  // test missing signature
  buffer.Clear();
  buffer.Write(wrong_header, 1);
  ASSERT_EQ(decoder.ReadValue(&dv, Value::Type::Vertex), false);

  // test wrong marker
  buffer.Clear();
  buffer.Write(wrong_header, 2);
  ASSERT_EQ(decoder.ReadValue(&dv, Value::Type::Vertex), false);

  // test wrong signature
  buffer.Clear();
  buffer.Write(header, 1);
  buffer.Write(wrong_header, 1);
  ASSERT_EQ(decoder.ReadValue(&dv, Value::Type::Vertex), false);

  // test ID wrong type
  buffer.Clear();
  buffer.Write(header, 2);
  buffer.Write(test_str, 2);
  ASSERT_EQ(decoder.ReadValue(&dv, Value::Type::Vertex), false);

  // test labels wrong outer type
  buffer.Clear();
  buffer.Write(header, 2);
  buffer.Write(test_int, 1);
  buffer.Write(test_int, 1);
  ASSERT_EQ(decoder.ReadValue(&dv, Value::Type::Vertex), false);

  // test labels wrong inner type
  buffer.Clear();
  buffer.Write(header, 2);
  buffer.Write(test_int, 1);
  buffer.Write(test_list, 1);
  buffer.Write(test_int, 1);
  ASSERT_EQ(decoder.ReadValue(&dv, Value::Type::Vertex), false);

  // test properties wrong outer type
  buffer.Clear();
  buffer.Write(header, 2);
  buffer.Write(test_int, 1);
  buffer.Write(test_list, 1);
  buffer.Write(test_str, 2);
  ASSERT_EQ(decoder.ReadValue(&dv, Value::Type::Vertex), false);

  // test all ok
  buffer.Clear();
  buffer.Write(header, 2);
  buffer.Write(test_int, 1);
  buffer.Write(test_list, 1);
  buffer.Write(test_str, 2);
  buffer.Write(test_map, 1);
  buffer.Write(test_str, 2);
  buffer.Write(test_int, 1);
  ASSERT_EQ(decoder.ReadValue(&dv, Value::Type::Vertex), true);
  auto &vertex = dv.ValueVertex();
  ASSERT_EQ(vertex.id.AsUint(), 1);
  ASSERT_EQ(vertex.labels[0], std::string("a"));
  ASSERT_EQ(vertex.properties[std::string("a")].ValueInt(), 1);
}

TEST(BoltDecoder, Edge) {
  TestDecoderBuffer buffer;
  DecoderT decoder(buffer);

  Value de;

  uint8_t header[] = "\xB5\x52";
  uint8_t wrong_header[] = "\x00\x00";
  uint8_t test_int1[] = "\x01";
  uint8_t test_int2[] = "\x02";
  uint8_t test_int3[] = "\x03";
  uint8_t test_str[] = "\x81\x61";
  uint8_t test_map[] = "\xA1";

  // test missing signature
  buffer.Clear();
  buffer.Write(wrong_header, 1);
  ASSERT_EQ(decoder.ReadValue(&de, Value::Type::Edge), false);

  // test wrong marker
  buffer.Clear();
  buffer.Write(wrong_header, 2);
  ASSERT_EQ(decoder.ReadValue(&de, Value::Type::Edge), false);

  // test wrong signature
  buffer.Clear();
  buffer.Write(header, 1);
  buffer.Write(wrong_header, 1);
  ASSERT_EQ(decoder.ReadValue(&de, Value::Type::Edge), false);

  // test ID wrong type
  buffer.Clear();
  buffer.Write(header, 2);
  buffer.Write(test_str, 2);
  ASSERT_EQ(decoder.ReadValue(&de, Value::Type::Edge), false);

  // test from_id wrong type
  buffer.Clear();
  buffer.Write(header, 2);
  buffer.Write(test_int1, 1);
  buffer.Write(test_str, 2);
  ASSERT_EQ(decoder.ReadValue(&de, Value::Type::Edge), false);

  // test to_id wrong type
  buffer.Clear();
  buffer.Write(header, 2);
  buffer.Write(test_int1, 1);
  buffer.Write(test_int2, 1);
  buffer.Write(test_str, 2);
  ASSERT_EQ(decoder.ReadValue(&de, Value::Type::Edge), false);

  // test type wrong type
  buffer.Clear();
  buffer.Write(header, 2);
  buffer.Write(test_int1, 1);
  buffer.Write(test_int2, 1);
  buffer.Write(test_int3, 1);
  buffer.Write(test_int1, 1);
  ASSERT_EQ(decoder.ReadValue(&de, Value::Type::Edge), false);

  // test properties wrong outer type
  buffer.Clear();
  buffer.Write(header, 2);
  buffer.Write(test_int1, 1);
  buffer.Write(test_int2, 1);
  buffer.Write(test_int3, 1);
  buffer.Write(test_str, 2);
  buffer.Write(test_int1, 1);
  ASSERT_EQ(decoder.ReadValue(&de, Value::Type::Edge), false);

  // test all ok
  buffer.Clear();
  buffer.Write(header, 2);
  buffer.Write(test_int1, 1);
  buffer.Write(test_int2, 1);
  buffer.Write(test_int3, 1);
  buffer.Write(test_str, 2);
  buffer.Write(test_map, 1);
  buffer.Write(test_str, 2);
  buffer.Write(test_int1, 1);
  ASSERT_EQ(decoder.ReadValue(&de, Value::Type::Edge), true);
  auto &edge = de.ValueEdge();
  ASSERT_EQ(edge.id.AsUint(), 1);
  ASSERT_EQ(edge.from.AsUint(), 2);
  ASSERT_EQ(edge.to.AsUint(), 3);
  ASSERT_EQ(edge.type, std::string("a"));
  ASSERT_EQ(edge.properties[std::string("a")].ValueInt(), 1);
}

int main(int argc, char **argv) {
  InitializeData(data, SIZE);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
