#include <glog/logging.h>

#include "bolt_common.hpp"
#include "bolt_testdata.hpp"

#include "communication/bolt/v1/decoder/decoder.hpp"
#include "query/typed_value.hpp"

using query::TypedValue;

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
  TypedValue tv;

  // test null
  buffer.Write((const uint8_t *)"\xC0", 1);
  ASSERT_EQ(decoder.ReadTypedValue(&tv), true);
  ASSERT_EQ(tv.type(), TypedValue::Type::Null);

  // test true
  buffer.Write((const uint8_t *)"\xC3", 1);
  ASSERT_EQ(decoder.ReadTypedValue(&tv), true);
  ASSERT_EQ(tv.type(), TypedValue::Type::Bool);
  ASSERT_EQ(tv.Value<bool>(), true);

  // test false
  buffer.Write((const uint8_t *)"\xC2", 1);
  ASSERT_EQ(decoder.ReadTypedValue(&tv), true);
  ASSERT_EQ(tv.type(), TypedValue::Type::Bool);
  ASSERT_EQ(tv.Value<bool>(), false);
}

TEST(BoltDecoder, Int) {
  TestDecoderBuffer buffer;
  DecoderT decoder(buffer);

  TypedValue tv;

  // test invalid marker
  buffer.Clear();
  buffer.Write((uint8_t *)"\xCD", 1);  // 0xCD is reserved in the protocol
  ASSERT_EQ(decoder.ReadTypedValue(&tv), false);

  for (int i = 0; i < 28; ++i) {
    // test missing data
    buffer.Clear();
    buffer.Write(int_encoded[i], int_encoded_len[i] - 1);
    ASSERT_EQ(decoder.ReadTypedValue(&tv), false);

    // test all ok
    buffer.Clear();
    buffer.Write(int_encoded[i], int_encoded_len[i]);
    ASSERT_EQ(decoder.ReadTypedValue(&tv), true);
    ASSERT_EQ(tv.type(), TypedValue::Type::Int);
    ASSERT_EQ(tv.Value<int64_t>(), int_decoded[i]);
  }
}

TEST(BoltDecoder, Double) {
  TestDecoderBuffer buffer;
  DecoderT decoder(buffer);

  TypedValue tv;

  for (int i = 0; i < 4; ++i) {
    // test missing data
    buffer.Clear();
    buffer.Write(double_encoded[i], 8);
    ASSERT_EQ(decoder.ReadTypedValue(&tv), false);

    // test all ok
    buffer.Clear();
    buffer.Write(double_encoded[i], 9);
    ASSERT_EQ(decoder.ReadTypedValue(&tv), true);
    ASSERT_EQ(tv.type(), TypedValue::Type::Double);
    ASSERT_EQ(tv.Value<double>(), double_decoded[i]);
  }
}

TEST(BoltDecoder, String) {
  TestDecoderBuffer buffer;
  DecoderT decoder(buffer);

  TypedValue tv;

  uint8_t headers[][6] = {"\x8F", "\xD0\x0F", "\xD1\x00\x0F",
                          "\xD2\x00\x00\x00\x0F"};
  int headers_len[] = {1, 2, 3, 5};

  for (int i = 0; i < 4; ++i) {
    // test missing data in header
    buffer.Clear();
    buffer.Write(headers[i], headers_len[i] - 1);
    ASSERT_EQ(decoder.ReadTypedValue(&tv), false);

    // test missing elements
    buffer.Clear();
    buffer.Write(headers[i], headers_len[i]);
    buffer.Write(data, 14);
    ASSERT_EQ(decoder.ReadTypedValue(&tv), false);

    // test all ok
    buffer.Clear();
    buffer.Write(headers[i], headers_len[i]);
    buffer.Write(data, 15);
    ASSERT_EQ(decoder.ReadTypedValue(&tv), true);
    ASSERT_EQ(tv.type(), TypedValue::Type::String);
    std::string &str = tv.Value<std::string>();
    for (int j = 0; j < 15; ++j) EXPECT_EQ((uint8_t)str[j], data[j]);
  }
}

TEST(BoltDecoder, List) {
  TestDecoderBuffer buffer;
  DecoderT decoder(buffer);

  TypedValue tv;

  uint8_t headers[][6] = {"\x9F", "\xD4\x0F", "\xD5\x00\x0F",
                          "\xD6\x00\x00\x00\x0F"};
  int headers_len[] = {1, 2, 3, 5};

  for (int i = 0; i < 4; ++i) {
    // test missing data in header
    buffer.Clear();
    buffer.Write(headers[i], headers_len[i] - 1);
    ASSERT_EQ(decoder.ReadTypedValue(&tv), false);

    // test missing elements
    buffer.Clear();
    buffer.Write(headers[i], headers_len[i]);
    for (uint8_t j = 0; j < 14; ++j) buffer.Write(&j, 1);
    ASSERT_EQ(decoder.ReadTypedValue(&tv), false);

    // test all ok
    buffer.Clear();
    buffer.Write(headers[i], headers_len[i]);
    for (uint8_t j = 0; j < 15; ++j) buffer.Write(&j, 1);
    ASSERT_EQ(decoder.ReadTypedValue(&tv), true);
    ASSERT_EQ(tv.type(), TypedValue::Type::List);
    std::vector<TypedValue> &val = tv.Value<std::vector<TypedValue>>();
    ASSERT_EQ(val.size(), 15);
    for (int j = 0; j < 15; ++j) EXPECT_EQ(val[j].Value<int64_t>(), j);
  }
}

TEST(BoltDecoder, Map) {
  TestDecoderBuffer buffer;
  DecoderT decoder(buffer);

  TypedValue tv;

  uint8_t headers[][6] = {"\xAF", "\xD8\x0F", "\xD9\x00\x0F",
                          "\xDA\x00\x00\x00\x0F"};
  int headers_len[] = {1, 2, 3, 5};

  uint8_t index[] = "\x81\x61";
  uint8_t wrong_index = 1;

  for (int i = 0; i < 4; ++i) {
    // test missing data in header
    buffer.Clear();
    buffer.Write(headers[i], headers_len[i] - 1);
    ASSERT_EQ(decoder.ReadTypedValue(&tv), false);

    // test wrong index type
    buffer.Clear();
    buffer.Write(headers[i], headers_len[i]);
    buffer.Write(&wrong_index, 1);
    buffer.Write(&wrong_index, 1);
    ASSERT_EQ(decoder.ReadTypedValue(&tv), false);

    // test missing element data
    buffer.Clear();
    buffer.Write(headers[i], headers_len[i]);
    buffer.Write(index, 2);
    ASSERT_EQ(decoder.ReadTypedValue(&tv), false);

    // test missing elements
    buffer.Clear();
    buffer.Write(headers[i], headers_len[i]);
    for (uint8_t j = 0; j < 14; ++j) {
      buffer.Write(index, 2);
      buffer.Write(&j, 1);
    }
    ASSERT_EQ(decoder.ReadTypedValue(&tv), false);

    // test elements with same index
    buffer.Clear();
    buffer.Write(headers[i], headers_len[i]);
    for (uint8_t j = 0; j < 15; ++j) {
      buffer.Write(index, 2);
      buffer.Write(&j, 1);
    }
    ASSERT_EQ(decoder.ReadTypedValue(&tv), false);

    // test all ok
    buffer.Clear();
    buffer.Write(headers[i], headers_len[i]);
    for (uint8_t j = 0; j < 15; ++j) {
      uint8_t tmp = 'a' + j;
      buffer.Write(index, 1);
      buffer.Write(&tmp, 1);
      buffer.Write(&j, 1);
    }
    ASSERT_EQ(decoder.ReadTypedValue(&tv), true);
    ASSERT_EQ(tv.type(), TypedValue::Type::Map);
    std::map<std::string, TypedValue> &val =
        tv.Value<std::map<std::string, TypedValue>>();
    ASSERT_EQ(val.size(), 15);
    for (int j = 0; j < 15; ++j) {
      char tmp_chr = 'a' + j;
      TypedValue tmp_tv = val[std::string(1, tmp_chr)];
      EXPECT_EQ(tmp_tv.type(), TypedValue::Type::Int);
      EXPECT_EQ(tmp_tv.Value<int64_t>(), j);
    }
  }
}

TEST(BoltDecoder, Vertex) {
  TestDecoderBuffer buffer;
  DecoderT decoder(buffer);

  communication::bolt::DecodedVertex dv;

  uint8_t header[] = "\xB3\x4E";
  uint8_t wrong_header[] = "\x00\x00";
  uint8_t test_int[] = "\x01";
  uint8_t test_str[] = "\x81\x61";
  uint8_t test_list[] = "\x91";
  uint8_t test_map[] = "\xA1";

  // test missing signature
  buffer.Clear();
  buffer.Write(wrong_header, 1);
  ASSERT_EQ(decoder.ReadVertex(&dv), false);

  // test wrong marker
  buffer.Clear();
  buffer.Write(wrong_header, 2);
  ASSERT_EQ(decoder.ReadVertex(&dv), false);

  // test wrong signature
  buffer.Clear();
  buffer.Write(header, 1);
  buffer.Write(wrong_header, 1);
  ASSERT_EQ(decoder.ReadVertex(&dv), false);

  // test ID wrong type
  buffer.Clear();
  buffer.Write(header, 2);
  buffer.Write(test_str, 2);
  ASSERT_EQ(decoder.ReadVertex(&dv), false);

  // test labels wrong outer type
  buffer.Clear();
  buffer.Write(header, 2);
  buffer.Write(test_int, 1);
  buffer.Write(test_int, 1);
  ASSERT_EQ(decoder.ReadVertex(&dv), false);

  // test labels wrong inner type
  buffer.Clear();
  buffer.Write(header, 2);
  buffer.Write(test_int, 1);
  buffer.Write(test_list, 1);
  buffer.Write(test_int, 1);
  ASSERT_EQ(decoder.ReadVertex(&dv), false);

  // test properties wrong outer type
  buffer.Clear();
  buffer.Write(header, 2);
  buffer.Write(test_int, 1);
  buffer.Write(test_list, 1);
  buffer.Write(test_str, 2);
  ASSERT_EQ(decoder.ReadVertex(&dv), false);

  // test all ok
  buffer.Clear();
  buffer.Write(header, 2);
  buffer.Write(test_int, 1);
  buffer.Write(test_list, 1);
  buffer.Write(test_str, 2);
  buffer.Write(test_map, 1);
  buffer.Write(test_str, 2);
  buffer.Write(test_int, 1);
  ASSERT_EQ(decoder.ReadVertex(&dv), true);
  ASSERT_EQ(dv.id, 1);
  ASSERT_EQ(dv.labels[0], std::string("a"));
  ASSERT_EQ(dv.properties[std::string("a")].Value<int64_t>(), 1);
}

TEST(BoltDecoder, Edge) {
  TestDecoderBuffer buffer;
  DecoderT decoder(buffer);

  communication::bolt::DecodedEdge de;

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
  ASSERT_EQ(decoder.ReadEdge(&de), false);

  // test wrong marker
  buffer.Clear();
  buffer.Write(wrong_header, 2);
  ASSERT_EQ(decoder.ReadEdge(&de), false);

  // test wrong signature
  buffer.Clear();
  buffer.Write(header, 1);
  buffer.Write(wrong_header, 1);
  ASSERT_EQ(decoder.ReadEdge(&de), false);

  // test ID wrong type
  buffer.Clear();
  buffer.Write(header, 2);
  buffer.Write(test_str, 2);
  ASSERT_EQ(decoder.ReadEdge(&de), false);

  // test from_id wrong type
  buffer.Clear();
  buffer.Write(header, 2);
  buffer.Write(test_int1, 1);
  buffer.Write(test_str, 2);
  ASSERT_EQ(decoder.ReadEdge(&de), false);

  // test to_id wrong type
  buffer.Clear();
  buffer.Write(header, 2);
  buffer.Write(test_int1, 1);
  buffer.Write(test_int2, 1);
  buffer.Write(test_str, 2);
  ASSERT_EQ(decoder.ReadEdge(&de), false);

  // test type wrong type
  buffer.Clear();
  buffer.Write(header, 2);
  buffer.Write(test_int1, 1);
  buffer.Write(test_int2, 1);
  buffer.Write(test_int3, 1);
  buffer.Write(test_int1, 1);
  ASSERT_EQ(decoder.ReadEdge(&de), false);

  // test properties wrong outer type
  buffer.Clear();
  buffer.Write(header, 2);
  buffer.Write(test_int1, 1);
  buffer.Write(test_int2, 1);
  buffer.Write(test_int3, 1);
  buffer.Write(test_str, 2);
  buffer.Write(test_int1, 1);
  ASSERT_EQ(decoder.ReadEdge(&de), false);

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
  ASSERT_EQ(decoder.ReadEdge(&de), true);
  ASSERT_EQ(de.id, 1);
  ASSERT_EQ(de.from, 2);
  ASSERT_EQ(de.to, 3);
  ASSERT_EQ(de.type, std::string("a"));
  ASSERT_EQ(de.properties[std::string("a")].Value<int64_t>(), 1);
}

int main(int argc, char **argv) {
  InitializeData(data, SIZE);
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
