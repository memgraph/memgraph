// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <gtest/gtest.h>

#include <filesystem>
#include <limits>

#include "storage/v2/durability/serialization.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/temporal.hpp"

static const std::string kTestMagic{"MGtest"};
static const uint64_t kTestVersion{1};

class DecoderEncoderTest : public ::testing::Test {
 public:
  void SetUp() override { Clear(); }

  void TearDown() override { Clear(); }

  std::filesystem::path storage_file{std::filesystem::temp_directory_path() /
                                     "MG_test_unit_storage_v2_decoder_encoder.bin"};

  std::filesystem::path alternate_file{std::filesystem::temp_directory_path() /
                                       "MG_test_unit_storage_v2_decoder_encoder_alternate.bin"};

 private:
  void Clear() {
    if (std::filesystem::exists(storage_file)) {
      std::filesystem::remove(storage_file);
    }
    if (std::filesystem::exists(alternate_file)) {
      std::filesystem::remove(alternate_file);
    }
  }
};

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(DecoderEncoderTest, ReadMarker) {
  {
    memgraph::storage::durability::Encoder encoder;
    encoder.Initialize(storage_file, kTestMagic, kTestVersion);
    for (const auto &item : memgraph::storage::durability::kMarkersAll) {
      encoder.WriteMarker(item);
    }
    {
      uint8_t invalid = 1;
      encoder.Write(&invalid, sizeof(invalid));
    }
    encoder.Finalize();
  }
  {
    memgraph::storage::durability::Decoder decoder;
    auto version = decoder.Initialize(storage_file, kTestMagic);
    ASSERT_TRUE(version);
    ASSERT_EQ(*version, kTestVersion);
    for (const auto &item : memgraph::storage::durability::kMarkersAll) {
      auto decoded = decoder.ReadMarker();
      ASSERT_TRUE(decoded);
      ASSERT_EQ(*decoded, item);
    }
    ASSERT_FALSE(decoder.ReadMarker());
    ASSERT_FALSE(decoder.ReadMarker());
    auto pos = decoder.GetPosition();
    ASSERT_TRUE(pos);
    ASSERT_EQ(pos, decoder.GetSize());
  }
}

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define GENERATE_READ_TEST(name, type, ...)                        \
  TEST_F(DecoderEncoderTest, Read##name) {                         \
    std::vector<type> dataset{__VA_ARGS__};                        \
    {                                                              \
      memgraph::storage::durability::Encoder encoder;              \
      encoder.Initialize(storage_file, kTestMagic, kTestVersion);  \
      for (const auto &item : dataset) {                           \
        encoder.Write##name(item);                                 \
      }                                                            \
      {                                                            \
        uint8_t invalid = 1;                                       \
        encoder.Write(&invalid, sizeof(invalid));                  \
      }                                                            \
      encoder.Finalize();                                          \
    }                                                              \
    {                                                              \
      memgraph::storage::durability::Decoder decoder;              \
      auto version = decoder.Initialize(storage_file, kTestMagic); \
      ASSERT_TRUE(version);                                        \
      ASSERT_EQ(*version, kTestVersion);                           \
      for (const auto &item : dataset) {                           \
        auto decoded = decoder.Read##name();                       \
        ASSERT_TRUE(decoded);                                      \
        ASSERT_EQ(*decoded, item);                                 \
      }                                                            \
      ASSERT_FALSE(decoder.Read##name());                          \
      ASSERT_FALSE(decoder.Read##name());                          \
      auto pos = decoder.GetPosition();                            \
      ASSERT_TRUE(pos);                                            \
      ASSERT_EQ(pos, decoder.GetSize());                           \
    }                                                              \
  }

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_READ_TEST(Bool, bool, false, true);

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_READ_TEST(Uint, uint64_t, 0, 1, 1000, 123123123, std::numeric_limits<uint64_t>::max());

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_READ_TEST(Double, double, 1.123, 3.1415926535, 0, -505.505, std::numeric_limits<double>::infinity(),
                   -std::numeric_limits<double>::infinity());

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_READ_TEST(String, std::string, "hello", "world", "nandare", "haihaihai", std::string(),
                   std::string(100000, 'a'));

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_READ_TEST(
    PropertyValue, memgraph::storage::PropertyValue, memgraph::storage::PropertyValue(),
    memgraph::storage::PropertyValue(false), memgraph::storage::PropertyValue(true),
    memgraph::storage::PropertyValue(123L), memgraph::storage::PropertyValue(123.5),
    memgraph::storage::PropertyValue("nandare"),
    memgraph::storage::PropertyValue(std::vector<memgraph::storage::PropertyValue>{
        memgraph::storage::PropertyValue("nandare"), memgraph::storage::PropertyValue(123L)}),
    memgraph::storage::PropertyValue(std::map<std::string, memgraph::storage::PropertyValue>{
        {"nandare", memgraph::storage::PropertyValue(123)}}),
    memgraph::storage::PropertyValue(memgraph::storage::TemporalData(memgraph::storage::TemporalType::Date, 23)));

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define GENERATE_SKIP_TEST(name, type, ...)                        \
  TEST_F(DecoderEncoderTest, Skip##name) {                         \
    std::vector<type> dataset{__VA_ARGS__};                        \
    {                                                              \
      memgraph::storage::durability::Encoder encoder;              \
      encoder.Initialize(storage_file, kTestMagic, kTestVersion);  \
      for (const auto &item : dataset) {                           \
        encoder.Write##name(item);                                 \
      }                                                            \
      {                                                            \
        uint8_t invalid = 1;                                       \
        encoder.Write(&invalid, sizeof(invalid));                  \
      }                                                            \
      encoder.Finalize();                                          \
    }                                                              \
    {                                                              \
      memgraph::storage::durability::Decoder decoder;              \
      auto version = decoder.Initialize(storage_file, kTestMagic); \
      ASSERT_TRUE(version);                                        \
      ASSERT_EQ(*version, kTestVersion);                           \
      for (auto it = dataset.begin(); it != dataset.end(); ++it) { \
        ASSERT_TRUE(decoder.Skip##name());                         \
      }                                                            \
      ASSERT_FALSE(decoder.Skip##name());                          \
      ASSERT_FALSE(decoder.Skip##name());                          \
      auto pos = decoder.GetPosition();                            \
      ASSERT_TRUE(pos);                                            \
      ASSERT_EQ(pos, decoder.GetSize());                           \
    }                                                              \
  }

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SKIP_TEST(String, std::string, "hello", "world", "nandare", "haihaihai", std::string(500000, 'a'));

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SKIP_TEST(
    PropertyValue, memgraph::storage::PropertyValue, memgraph::storage::PropertyValue(),
    memgraph::storage::PropertyValue(false), memgraph::storage::PropertyValue(true),
    memgraph::storage::PropertyValue(123L), memgraph::storage::PropertyValue(123.5),
    memgraph::storage::PropertyValue("nandare"),
    memgraph::storage::PropertyValue(std::vector<memgraph::storage::PropertyValue>{
        memgraph::storage::PropertyValue("nandare"), memgraph::storage::PropertyValue(123L)}),
    memgraph::storage::PropertyValue(std::map<std::string, memgraph::storage::PropertyValue>{
        {"nandare", memgraph::storage::PropertyValue(123)}}),
    memgraph::storage::PropertyValue(memgraph::storage::TemporalData(memgraph::storage::TemporalType::Date, 23)));

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define GENERATE_PARTIAL_READ_TEST(name, value)                                          \
  TEST_F(DecoderEncoderTest, PartialRead##name) {                                        \
    {                                                                                    \
      memgraph::storage::durability::Encoder encoder;                                    \
      encoder.Initialize(storage_file, kTestMagic, kTestVersion);                        \
      encoder.Write##name(value);                                                        \
      encoder.Finalize();                                                                \
    }                                                                                    \
    {                                                                                    \
      memgraph::utils::InputFile ifile;                                                  \
      memgraph::utils::OutputFile ofile;                                                 \
      ASSERT_TRUE(ifile.Open(storage_file));                                             \
      ofile.Open(alternate_file, memgraph::utils::OutputFile::Mode::OVERWRITE_EXISTING); \
      auto size = ifile.GetSize();                                                       \
      for (size_t i = 0; i <= size; ++i) {                                               \
        if (i != 0) {                                                                    \
          uint8_t byte;                                                                  \
          ASSERT_TRUE(ifile.Read(&byte, sizeof(byte)));                                  \
          ofile.Write(&byte, sizeof(byte));                                              \
          ofile.Sync();                                                                  \
        }                                                                                \
        memgraph::storage::durability::Decoder decoder;                                  \
        auto version = decoder.Initialize(alternate_file, kTestMagic);                   \
        if (i < kTestMagic.size() + sizeof(kTestVersion)) {                              \
          ASSERT_FALSE(version);                                                         \
        } else {                                                                         \
          ASSERT_TRUE(version);                                                          \
          ASSERT_EQ(*version, kTestVersion);                                             \
        }                                                                                \
        if (i != size) {                                                                 \
          ASSERT_FALSE(decoder.Read##name());                                            \
        } else {                                                                         \
          auto decoded = decoder.Read##name();                                           \
          ASSERT_TRUE(decoded);                                                          \
          ASSERT_EQ(*decoded, value);                                                    \
        }                                                                                \
      }                                                                                  \
    }                                                                                    \
  }

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_PARTIAL_READ_TEST(Marker, memgraph::storage::durability::Marker::SECTION_VERTEX);

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_PARTIAL_READ_TEST(Bool, false);

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_PARTIAL_READ_TEST(Uint, 123123123);

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_PARTIAL_READ_TEST(Double, 3.1415926535);

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_PARTIAL_READ_TEST(String, "nandare");

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_PARTIAL_READ_TEST(
    PropertyValue,
    memgraph::storage::PropertyValue(std::vector<memgraph::storage::PropertyValue>{
        memgraph::storage::PropertyValue(), memgraph::storage::PropertyValue(true),
        memgraph::storage::PropertyValue(123L), memgraph::storage::PropertyValue(123.5),
        memgraph::storage::PropertyValue("nandare"),
        memgraph::storage::PropertyValue{
            std::map<std::string, memgraph::storage::PropertyValue>{{"haihai", memgraph::storage::PropertyValue()}}},
        memgraph::storage::PropertyValue(memgraph::storage::TemporalData(memgraph::storage::TemporalType::Date, 23))}));

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define GENERATE_PARTIAL_SKIP_TEST(name, value)                                          \
  TEST_F(DecoderEncoderTest, PartialSkip##name) {                                        \
    {                                                                                    \
      memgraph::storage::durability::Encoder encoder;                                    \
      encoder.Initialize(storage_file, kTestMagic, kTestVersion);                        \
      encoder.Write##name(value);                                                        \
      encoder.Finalize();                                                                \
    }                                                                                    \
    {                                                                                    \
      memgraph::utils::InputFile ifile;                                                  \
      memgraph::utils::OutputFile ofile;                                                 \
      ASSERT_TRUE(ifile.Open(storage_file));                                             \
      ofile.Open(alternate_file, memgraph::utils::OutputFile::Mode::OVERWRITE_EXISTING); \
      auto size = ifile.GetSize();                                                       \
      for (size_t i = 0; i <= size; ++i) {                                               \
        if (i != 0) {                                                                    \
          uint8_t byte;                                                                  \
          ASSERT_TRUE(ifile.Read(&byte, sizeof(byte)));                                  \
          ofile.Write(&byte, sizeof(byte));                                              \
          ofile.Sync();                                                                  \
        }                                                                                \
        memgraph::storage::durability::Decoder decoder;                                  \
        auto version = decoder.Initialize(alternate_file, kTestMagic);                   \
        if (i < kTestMagic.size() + sizeof(kTestVersion)) {                              \
          ASSERT_FALSE(version);                                                         \
        } else {                                                                         \
          ASSERT_TRUE(version);                                                          \
          ASSERT_EQ(*version, kTestVersion);                                             \
        }                                                                                \
        if (i != size) {                                                                 \
          ASSERT_FALSE(decoder.Skip##name());                                            \
        } else {                                                                         \
          ASSERT_TRUE(decoder.Skip##name());                                             \
        }                                                                                \
      }                                                                                  \
    }                                                                                    \
  }

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_PARTIAL_SKIP_TEST(String, "nandare");

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_PARTIAL_SKIP_TEST(
    PropertyValue,
    memgraph::storage::PropertyValue(std::vector<memgraph::storage::PropertyValue>{
        memgraph::storage::PropertyValue(), memgraph::storage::PropertyValue(true),
        memgraph::storage::PropertyValue(123L), memgraph::storage::PropertyValue(123.5),
        memgraph::storage::PropertyValue("nandare"),
        memgraph::storage::PropertyValue{
            std::map<std::string, memgraph::storage::PropertyValue>{{"haihai", memgraph::storage::PropertyValue()}}},
        memgraph::storage::PropertyValue(memgraph::storage::TemporalData(memgraph::storage::TemporalType::Date, 23))}));

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(DecoderEncoderTest, PropertyValueInvalidMarker) {
  {
    memgraph::storage::durability::Encoder encoder;
    encoder.Initialize(storage_file, kTestMagic, kTestVersion);
    encoder.WritePropertyValue(memgraph::storage::PropertyValue(123L));
    encoder.Finalize();
  }
  {
    memgraph::utils::OutputFile file;
    file.Open(storage_file, memgraph::utils::OutputFile::Mode::OVERWRITE_EXISTING);
    for (auto marker : memgraph::storage::durability::kMarkersAll) {
      bool valid_marker;
      switch (marker) {
        case memgraph::storage::durability::Marker::TYPE_NULL:
        case memgraph::storage::durability::Marker::TYPE_BOOL:
        case memgraph::storage::durability::Marker::TYPE_INT:
        case memgraph::storage::durability::Marker::TYPE_DOUBLE:
        case memgraph::storage::durability::Marker::TYPE_STRING:
        case memgraph::storage::durability::Marker::TYPE_LIST:
        case memgraph::storage::durability::Marker::TYPE_MAP:
        case memgraph::storage::durability::Marker::TYPE_TEMPORAL_DATA:
        case memgraph::storage::durability::Marker::TYPE_PROPERTY_VALUE:
          valid_marker = true;
          break;

        case memgraph::storage::durability::Marker::SECTION_VERTEX:
        case memgraph::storage::durability::Marker::SECTION_EDGE:
        case memgraph::storage::durability::Marker::SECTION_MAPPER:
        case memgraph::storage::durability::Marker::SECTION_METADATA:
        case memgraph::storage::durability::Marker::SECTION_INDICES:
        case memgraph::storage::durability::Marker::SECTION_CONSTRAINTS:
        case memgraph::storage::durability::Marker::SECTION_DELTA:
        case memgraph::storage::durability::Marker::SECTION_EPOCH_HISTORY:
        case memgraph::storage::durability::Marker::SECTION_OFFSETS:
        case memgraph::storage::durability::Marker::DELTA_VERTEX_CREATE:
        case memgraph::storage::durability::Marker::DELTA_VERTEX_DELETE:
        case memgraph::storage::durability::Marker::DELTA_VERTEX_ADD_LABEL:
        case memgraph::storage::durability::Marker::DELTA_VERTEX_REMOVE_LABEL:
        case memgraph::storage::durability::Marker::DELTA_VERTEX_SET_PROPERTY:
        case memgraph::storage::durability::Marker::DELTA_EDGE_CREATE:
        case memgraph::storage::durability::Marker::DELTA_EDGE_DELETE:
        case memgraph::storage::durability::Marker::DELTA_EDGE_SET_PROPERTY:
        case memgraph::storage::durability::Marker::DELTA_TRANSACTION_END:
        case memgraph::storage::durability::Marker::DELTA_LABEL_INDEX_CREATE:
        case memgraph::storage::durability::Marker::DELTA_LABEL_INDEX_DROP:
        case memgraph::storage::durability::Marker::DELTA_LABEL_INDEX_STATS_SET:
        case memgraph::storage::durability::Marker::DELTA_LABEL_INDEX_STATS_CLEAR:
        case memgraph::storage::durability::Marker::DELTA_LABEL_PROPERTY_INDEX_CREATE:
        case memgraph::storage::durability::Marker::DELTA_LABEL_PROPERTY_INDEX_DROP:
        case memgraph::storage::durability::Marker::DELTA_LABEL_PROPERTY_INDEX_STATS_SET:
        case memgraph::storage::durability::Marker::DELTA_LABEL_PROPERTY_INDEX_STATS_CLEAR:
        case memgraph::storage::durability::Marker::DELTA_EXISTENCE_CONSTRAINT_CREATE:
        case memgraph::storage::durability::Marker::DELTA_EXISTENCE_CONSTRAINT_DROP:
        case memgraph::storage::durability::Marker::DELTA_UNIQUE_CONSTRAINT_CREATE:
        case memgraph::storage::durability::Marker::DELTA_UNIQUE_CONSTRAINT_DROP:
        case memgraph::storage::durability::Marker::VALUE_FALSE:
        case memgraph::storage::durability::Marker::VALUE_TRUE:
          valid_marker = false;
          break;
      }
      // We only run this test with invalid markers.
      if (valid_marker) continue;
      {
        file.SetPosition(memgraph::utils::OutputFile::Position::RELATIVE_TO_END,
                         -(sizeof(uint64_t) + sizeof(memgraph::storage::durability::Marker)));
        auto byte = static_cast<uint8_t>(marker);
        file.Write(&byte, sizeof(byte));
        file.Sync();
      }
      {
        memgraph::storage::durability::Decoder decoder;
        auto version = decoder.Initialize(storage_file, kTestMagic);
        ASSERT_TRUE(version);
        ASSERT_EQ(*version, kTestVersion);
        ASSERT_FALSE(decoder.SkipPropertyValue());
      }
      {
        memgraph::storage::durability::Decoder decoder;
        auto version = decoder.Initialize(storage_file, kTestMagic);
        ASSERT_TRUE(version);
        ASSERT_EQ(*version, kTestVersion);
        ASSERT_FALSE(decoder.ReadPropertyValue());
      }
    }
    {
      {
        file.SetPosition(memgraph::utils::OutputFile::Position::RELATIVE_TO_END,
                         -(sizeof(uint64_t) + sizeof(memgraph::storage::durability::Marker)));
        uint8_t byte = 1;
        file.Write(&byte, sizeof(byte));
        file.Sync();
      }
      {
        memgraph::storage::durability::Decoder decoder;
        auto version = decoder.Initialize(storage_file, kTestMagic);
        ASSERT_TRUE(version);
        ASSERT_EQ(*version, kTestVersion);
        ASSERT_FALSE(decoder.SkipPropertyValue());
      }
      {
        memgraph::storage::durability::Decoder decoder;
        auto version = decoder.Initialize(storage_file, kTestMagic);
        ASSERT_TRUE(version);
        ASSERT_EQ(*version, kTestVersion);
        ASSERT_FALSE(decoder.ReadPropertyValue());
      }
    }
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(DecoderEncoderTest, DecoderPosition) {
  {
    memgraph::storage::durability::Encoder encoder;
    encoder.Initialize(storage_file, kTestMagic, kTestVersion);
    encoder.WriteBool(true);
    encoder.Finalize();
  }
  {
    memgraph::storage::durability::Decoder decoder;
    auto version = decoder.Initialize(storage_file, kTestMagic);
    ASSERT_TRUE(version);
    ASSERT_EQ(*version, kTestVersion);
    for (int i = 0; i < 10; ++i) {
      ASSERT_TRUE(decoder.SetPosition(kTestMagic.size() + sizeof(kTestVersion)));
      auto decoded = decoder.ReadBool();
      ASSERT_TRUE(decoded);
      ASSERT_TRUE(*decoded);
      auto pos = decoder.GetPosition();
      ASSERT_TRUE(pos);
      ASSERT_EQ(pos, decoder.GetSize());
    }
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(DecoderEncoderTest, EncoderPosition) {
  {
    memgraph::storage::durability::Encoder encoder;
    encoder.Initialize(storage_file, kTestMagic, kTestVersion);
    encoder.WriteBool(false);
    encoder.SetPosition(kTestMagic.size() + sizeof(kTestVersion));
    ASSERT_EQ(encoder.GetPosition(), kTestMagic.size() + sizeof(kTestVersion));
    encoder.WriteBool(true);
    encoder.Finalize();
  }
  {
    memgraph::storage::durability::Decoder decoder;
    auto version = decoder.Initialize(storage_file, kTestMagic);
    ASSERT_TRUE(version);
    ASSERT_EQ(*version, kTestVersion);
    auto decoded = decoder.ReadBool();
    ASSERT_TRUE(decoded);
    ASSERT_TRUE(*decoded);
    auto pos = decoder.GetPosition();
    ASSERT_TRUE(pos);
    ASSERT_EQ(pos, decoder.GetSize());
  }
}
