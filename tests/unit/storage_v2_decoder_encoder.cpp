// Copyright 2026 Memgraph Ltd.
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
#include <zlib.h>

#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <limits>
#include <memory>
#include <vector>

#include "storage/v2/durability/marker.hpp"
#include "storage/v2/durability/serialization.hpp"
#include "storage/v2/point.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/temporal.hpp"
#include "utils/file.hpp"
#include "utils/small_vector.hpp"
#include "utils/temporal.hpp"

static const std::string kTestMagic{"MGtest"};
static const uint64_t kTestVersion{1};

template <typename T>
class DecoderEncoderTest : public ::testing::Test {
 public:
  void SetUp() override { Clear(); }

  void TearDown() override { Clear(); }

  std::filesystem::path storage_file{std::filesystem::temp_directory_path() /
                                     "MG_test_unit_storage_v2_decoder_encoder.bin"};

  std::filesystem::path alternate_file{std::filesystem::temp_directory_path() /
                                       "MG_test_unit_storage_v2_decoder_encoder_alternate.bin"};

  void ExpectBytes(const std::vector<uint8_t> &expected) {
    memgraph::utils::InputFile file;
    ASSERT_TRUE(file.Open(storage_file));
    ASSERT_EQ(file.GetSize(), expected.size());
    std::vector<uint8_t> actual(expected.size());
    ASSERT_TRUE(file.Read(actual.data(), actual.size()));
    EXPECT_EQ(actual, expected);
  }

  static uint32_t ExpectedCrc(const std::vector<uint8_t> &bytes) {
    return crc32(0, bytes.data(), static_cast<uInt>(bytes.size()));
  }

 private:
  void Clear() {
    if (std::filesystem::exists(this->storage_file)) {
      std::filesystem::remove(this->storage_file);
    }
    if (std::filesystem::exists(alternate_file)) {
      std::filesystem::remove(alternate_file);
    }
  }
};

using FileTypes = testing::Types<memgraph::utils::OutputFile, memgraph::utils::NonConcurrentOutputFile>;
TYPED_TEST_SUITE(DecoderEncoderTest, FileTypes);

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DecoderEncoderTest, StagedCapacityAndBypass) {
  memgraph::storage::durability::Encoder<TypeParam> encoder;
  ASSERT_TRUE(encoder.Initialize(this->storage_file));
  std::vector<uint8_t> expected(8192, 0x11);
  expected.insert(expected.end(), 8192, 0x22);
  encoder.Write(expected.data(), 8192);
  encoder.Write(expected.data() + 8192, 8192);  // Exactly one full stage.
  EXPECT_EQ(encoder.GetPosition(), 16384);
  EXPECT_EQ(encoder.GetSize(), 16384);
  EXPECT_EQ(encoder.CrcAccValue(), this->ExpectedCrc(expected));
  const std::vector<uint8_t> prefix{0x33}, payload(8193, 0x44), suffix{0x55, 0x66};
  encoder.Write(prefix.data(), prefix.size());  // Overflow leaves a new staged prefix.
  EXPECT_EQ(encoder.GetPosition(), expected.size() + prefix.size());
  EXPECT_EQ(encoder.GetSize(), expected.size() + prefix.size());
  encoder.Write(payload.data(), payload.size());
  EXPECT_EQ(encoder.GetPosition(), expected.size() + prefix.size() + payload.size());
  EXPECT_EQ(encoder.GetSize(), expected.size() + prefix.size() + payload.size());
  encoder.Write(suffix.data(), suffix.size());
  expected.insert(expected.end(), prefix.begin(), prefix.end());
  expected.insert(expected.end(), payload.begin(), payload.end());
  expected.insert(expected.end(), suffix.begin(), suffix.end());
  EXPECT_EQ(encoder.GetPosition(), expected.size());
  EXPECT_EQ(encoder.GetSize(), expected.size());
  EXPECT_EQ(encoder.CrcAccValue(), this->ExpectedCrc(expected));
  encoder.Finalize();
  this->ExpectBytes(expected);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DecoderEncoderTest, StagedCrcAndEmptyWrites) {
  memgraph::storage::durability::Encoder<TypeParam> encoder;
  ASSERT_TRUE(encoder.Initialize(this->storage_file));
  std::vector<uint8_t> expected{0x12, 0x34, 0x56};
  encoder.Write(expected.data(), expected.size());
  encoder.Sync();  // Give empty writes a nonzero accumulated CRC to preserve.
  encoder.Write(nullptr, 0);
  encoder.Write(expected.data(), 0);
  EXPECT_EQ(encoder.CrcAccValue(), this->ExpectedCrc(expected));
  encoder.WriteString(std::string_view{});
  expected.push_back(static_cast<uint8_t>(memgraph::storage::durability::Marker::TYPE_STRING));
  expected.insert(expected.end(), sizeof(uint64_t), 0);
  for (int i = 0; i < 3; ++i) EXPECT_EQ(encoder.CrcAccValue(), this->ExpectedCrc(expected));
  expected.push_back(static_cast<uint8_t>(memgraph::storage::durability::Marker::TYPE_INT));
  const uint64_t crc = this->ExpectedCrc(expected);
  EXPECT_EQ(encoder.WriteCrc(), crc);
  for (unsigned i = 0; i < sizeof(crc); ++i) expected.push_back(static_cast<uint8_t>(crc >> (8 * i)));
  EXPECT_EQ(encoder.CrcAccValue(), this->ExpectedCrc(expected));
  EXPECT_EQ(encoder.GetPosition(), expected.size());
  encoder.Finalize();
  this->ExpectBytes(expected);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DecoderEncoderTest, StagedResetCrc) {
  memgraph::storage::durability::Encoder<TypeParam> encoder;
  ASSERT_TRUE(encoder.Initialize(this->storage_file));
  const std::vector<uint8_t> prefix{1, 2, 3}, suffix{4, 5};
  encoder.Write(prefix.data(), prefix.size());
  encoder.ResetCrcAcc();
  EXPECT_EQ(encoder.CrcAccValue(), 0);
  encoder.Write(suffix.data(), suffix.size());
  EXPECT_EQ(encoder.CrcAccValue(), this->ExpectedCrc(suffix));
  encoder.Finalize();
  EXPECT_EQ(encoder.CrcAccValue(), this->ExpectedCrc(suffix));
  this->ExpectBytes({1, 2, 3, 4, 5});
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DecoderEncoderTest, StagedSeekAndCrcPatch) {
  memgraph::storage::durability::Encoder<TypeParam> encoder;
  ASSERT_TRUE(encoder.Initialize(this->storage_file));
  std::vector<uint8_t> expected(32, 0x55);
  auto accumulated = expected;
  encoder.Write(expected.data(), expected.size());
  constexpr uint64_t patch_crc = 0x12345678;
  std::vector<uint8_t> patch{static_cast<uint8_t>(memgraph::storage::durability::Marker::TYPE_INT)};
  for (unsigned i = 0; i < sizeof(patch_crc); ++i) patch.push_back(static_cast<uint8_t>(patch_crc >> (8 * i)));
  encoder.WriteCrcAt(3, patch_crc);
  std::copy(patch.begin(), patch.end(), expected.begin() + 3);
  accumulated.insert(accumulated.end(), patch.begin(), patch.end());
  EXPECT_EQ(encoder.GetPosition(), 12);
  EXPECT_EQ(encoder.GetSize(), 32);
  EXPECT_EQ(encoder.CrcAccValue(), this->ExpectedCrc(accumulated));  // Write order, including overwritten bytes.
  const std::vector<uint8_t> suffix{0xaa, 0xbb, 0xcc};
  encoder.SetPosition(31);
  encoder.Write(suffix.data(), suffix.size());
  expected.resize(31);
  expected.insert(expected.end(), suffix.begin(), suffix.end());
  accumulated.insert(accumulated.end(), suffix.begin(), suffix.end());
  EXPECT_EQ(encoder.GetPosition(), 34);
  EXPECT_EQ(encoder.GetSize(), 34);
  EXPECT_EQ(encoder.CrcAccValue(), this->ExpectedCrc(accumulated));
  encoder.Finalize();
  this->ExpectBytes(expected);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DecoderEncoderTest, StagedCompletion) {
  const std::vector<uint8_t> expected{1, 3, 5, 7};
  for (int completion = 0; completion < 4; ++completion) {
    SCOPED_TRACE(completion);  // Sync, Close, Finalize, then destructor alone.
    std::filesystem::remove(this->storage_file);
    {
      memgraph::storage::durability::Encoder<TypeParam> encoder;
      ASSERT_TRUE(encoder.Initialize(this->storage_file));
      encoder.Write(expected.data(), expected.size());
      EXPECT_EQ(encoder.CrcAccValue(), this->ExpectedCrc(expected));
      switch (completion) {
        case 0:
          encoder.Sync();
          break;
        case 1:
          encoder.Close();
          break;
        case 2:
          encoder.Finalize();
          break;
      }
      if (completion != 3) this->ExpectBytes(expected);
    }
    this->ExpectBytes(expected);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DecoderEncoderTest, StagedFileVisibility) {
  memgraph::storage::durability::Encoder<TypeParam> encoder;
  ASSERT_TRUE(encoder.Initialize(this->storage_file));
  const std::vector<uint8_t> prefix{1, 2, 3}, suffix{4, 5};
  encoder.Write(prefix.data(), prefix.size());
  if constexpr (std::same_as<TypeParam, memgraph::utils::OutputFile>) {
    encoder.DisableFlushing();
    encoder.Write(suffix.data(), suffix.size());
    const auto [data, size] = encoder.CurrentFileBuffer();
    EXPECT_EQ(size, prefix.size());
    EXPECT_EQ(std::vector<uint8_t>(data, data + size), prefix);
    EXPECT_EQ(encoder.GetPosition(), prefix.size() + suffix.size());
    encoder.EnableFlushing();
    encoder.TryFlushing();
    this->ExpectBytes({1, 2, 3, 4, 5});
  } else {
    const auto source = std::unique_ptr<FILE, decltype(&std::fclose)>{
        std::fopen(this->alternate_file.c_str(), "w+b"), &std::fclose};
    ASSERT_NE(source, nullptr);
    ASSERT_EQ(std::fwrite(suffix.data(), 1, suffix.size(), source.get()), suffix.size());
    ASSERT_EQ(std::fflush(source.get()), 0);
    const auto appended = encoder.AppendFrom(::fileno(source.get()), suffix.size());
    ASSERT_TRUE(appended);
    EXPECT_EQ(*appended, suffix.size());
    EXPECT_EQ(encoder.GetPosition(), prefix.size() + suffix.size());
    EXPECT_EQ(encoder.GetSize(), prefix.size() + suffix.size());
    EXPECT_EQ(encoder.CrcAccValue(), this->ExpectedCrc(prefix));  // AppendFrom does not accumulate copied bytes.
    encoder.Finalize();
    this->ExpectBytes({1, 2, 3, 4, 5});
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DecoderEncoderTest, ReadMarker) {
  {
    memgraph::storage::durability::Encoder<TypeParam> encoder;
    encoder.Initialize(this->storage_file, kTestMagic, kTestVersion);
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
    auto version = decoder.Initialize(this->storage_file, kTestMagic);
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
    ASSERT_EQ(pos, decoder.GetSize());
  }
}

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define GENERATE_READ_TEST(name, type, ...)                              \
  TYPED_TEST(DecoderEncoderTest, Read##name) {                           \
    std::vector<type> dataset{__VA_ARGS__};                              \
    {                                                                    \
      memgraph::storage::durability::Encoder<TypeParam> encoder;         \
      encoder.Initialize(this->storage_file, kTestMagic, kTestVersion);  \
      for (const auto &item : dataset) {                                 \
        encoder.Write##name(item);                                       \
      }                                                                  \
      {                                                                  \
        uint8_t invalid = 1;                                             \
        encoder.Write(&invalid, sizeof(invalid));                        \
      }                                                                  \
      encoder.Finalize();                                                \
    }                                                                    \
    {                                                                    \
      memgraph::storage::durability::Decoder decoder;                    \
      auto version = decoder.Initialize(this->storage_file, kTestMagic); \
      ASSERT_TRUE(version);                                              \
      ASSERT_EQ(*version, kTestVersion);                                 \
      for (const auto &item : dataset) {                                 \
        auto decoded = decoder.Read##name();                             \
        ASSERT_TRUE(decoded);                                            \
        ASSERT_EQ(*decoded, item);                                       \
      }                                                                  \
      ASSERT_FALSE(decoder.Read##name());                                \
      ASSERT_FALSE(decoder.Read##name());                                \
      auto pos = decoder.GetPosition();                                  \
      ASSERT_EQ(pos, decoder.GetSize());                                 \
    }                                                                    \
  }

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_READ_TEST(Bool, bool, false, true);

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_READ_TEST(Uint, uint64_t, 0, 1, 1000, 123'123'123, std::numeric_limits<uint64_t>::max());

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_READ_TEST(Double, double, 1.123, 3.1415926535, 0, -505.505, std::numeric_limits<double>::infinity(),
                   -std::numeric_limits<double>::infinity());

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_READ_TEST(String, std::string, "hello", "world", "nandare", "haihaihai", std::string(),
                   std::string(100'000, 'a'));

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_READ_TEST(
    ExternalPropertyValue, memgraph::storage::ExternalPropertyValue, memgraph::storage::ExternalPropertyValue(),
    memgraph::storage::ExternalPropertyValue(false), memgraph::storage::ExternalPropertyValue(true),
    memgraph::storage::ExternalPropertyValue(123L), memgraph::storage::ExternalPropertyValue(123.5),
    memgraph::storage::ExternalPropertyValue("nandare"),
    memgraph::storage::ExternalPropertyValue(std::vector<memgraph::storage::ExternalPropertyValue>{
        memgraph::storage::ExternalPropertyValue("nandare"), memgraph::storage::ExternalPropertyValue(123L)}),
    memgraph::storage::ExternalPropertyValue(memgraph::storage::ExternalPropertyValue::map_t{
        {"nandare", memgraph::storage::ExternalPropertyValue(123)}}),
    memgraph::storage::ExternalPropertyValue(memgraph::storage::TemporalData(memgraph::storage::TemporalType::Date,
                                                                             23)),
    memgraph::storage::ExternalPropertyValue(
        memgraph::storage::ZonedTemporalData(memgraph::storage::ZonedTemporalType::ZonedDateTime,
                                             memgraph::utils::AsSysTime(23), memgraph::utils::Timezone("Etc/UTC"))),
    memgraph::storage::ExternalPropertyValue(memgraph::storage::ZonedTemporalData(
        memgraph::storage::ZonedTemporalType::ZonedDateTime, memgraph::utils::AsSysTime(23),
        memgraph::utils::Timezone(std::chrono::minutes{-60}))),
    memgraph::storage::ExternalPropertyValue(memgraph::storage::Point2d{
        memgraph::storage::CoordinateReferenceSystem::WGS84_2d, 1.0, 2.0}),
    memgraph::storage::ExternalPropertyValue(memgraph::storage::Point2d{
        memgraph::storage::CoordinateReferenceSystem::Cartesian_2d, 1.0, 2.0}),
    memgraph::storage::ExternalPropertyValue(memgraph::storage::Point3d{
        memgraph::storage::CoordinateReferenceSystem::WGS84_3d, 1.0, 2.0, 3.0}),
    memgraph::storage::ExternalPropertyValue(memgraph::storage::Point3d{
        memgraph::storage::CoordinateReferenceSystem::Cartesian_3d, 1.0, 2.0, 3.0}),
    memgraph::storage::ExternalPropertyValue(memgraph::storage::ExternalPropertyValue::VectorIndexIdData{
        memgraph::storage::ExternalPropertyValue::vector_index_id_t{"test_index"},
        memgraph::utils::small_vector<float>{1.0f, 2.0f, 3.0f}}));

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define GENERATE_SKIP_TEST(name, type, ...)                              \
  TYPED_TEST(DecoderEncoderTest, Skip##name) {                           \
    std::vector<type> dataset{__VA_ARGS__};                              \
    {                                                                    \
      memgraph::storage::durability::Encoder<TypeParam> encoder;         \
      encoder.Initialize(this->storage_file, kTestMagic, kTestVersion);  \
      for (const auto &item : dataset) {                                 \
        encoder.Write##name(item);                                       \
      }                                                                  \
      {                                                                  \
        uint8_t invalid = 1;                                             \
        encoder.Write(&invalid, sizeof(invalid));                        \
      }                                                                  \
      encoder.Finalize();                                                \
    }                                                                    \
    {                                                                    \
      memgraph::storage::durability::Decoder decoder;                    \
      auto version = decoder.Initialize(this->storage_file, kTestMagic); \
      ASSERT_TRUE(version);                                              \
      ASSERT_EQ(*version, kTestVersion);                                 \
      for (auto it = dataset.begin(); it != dataset.end(); ++it) {       \
        ASSERT_TRUE(decoder.Skip##name());                               \
      }                                                                  \
      ASSERT_FALSE(decoder.Skip##name());                                \
      ASSERT_FALSE(decoder.Skip##name());                                \
      auto pos = decoder.GetPosition();                                  \
      ASSERT_EQ(pos, decoder.GetSize());                                 \
    }                                                                    \
  }

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SKIP_TEST(String, std::string, "hello", "world", "nandare", "haihaihai", std::string(500'000, 'a'));

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SKIP_TEST(
    ExternalPropertyValue, memgraph::storage::ExternalPropertyValue, memgraph::storage::ExternalPropertyValue(),
    memgraph::storage::ExternalPropertyValue(false), memgraph::storage::ExternalPropertyValue(true),
    memgraph::storage::ExternalPropertyValue(123L), memgraph::storage::ExternalPropertyValue(123.5),
    memgraph::storage::ExternalPropertyValue("nandare"),
    memgraph::storage::ExternalPropertyValue(std::vector<memgraph::storage::ExternalPropertyValue>{
        memgraph::storage::ExternalPropertyValue("nandare"), memgraph::storage::ExternalPropertyValue(123L)}),
    memgraph::storage::ExternalPropertyValue(memgraph::storage::ExternalPropertyValue::map_t{
        {"nandare", memgraph::storage::ExternalPropertyValue(123)}}),
    memgraph::storage::ExternalPropertyValue(memgraph::storage::TemporalData(memgraph::storage::TemporalType::Date,
                                                                             23)),
    memgraph::storage::ExternalPropertyValue(
        memgraph::storage::ZonedTemporalData(memgraph::storage::ZonedTemporalType::ZonedDateTime,
                                             memgraph::utils::AsSysTime(23), memgraph::utils::Timezone("Etc/UTC"))),
    memgraph::storage::ExternalPropertyValue(memgraph::storage::ZonedTemporalData(
        memgraph::storage::ZonedTemporalType::ZonedDateTime, memgraph::utils::AsSysTime(23),
        memgraph::utils::Timezone(std::chrono::minutes{-60}))),
    memgraph::storage::ExternalPropertyValue(memgraph::storage::Point2d{
        memgraph::storage::CoordinateReferenceSystem::WGS84_2d, 1.0, 2.0}),
    memgraph::storage::ExternalPropertyValue(memgraph::storage::Point2d{
        memgraph::storage::CoordinateReferenceSystem::Cartesian_2d, 1.0, 2.0}),
    memgraph::storage::ExternalPropertyValue(memgraph::storage::Point3d{
        memgraph::storage::CoordinateReferenceSystem::WGS84_3d, 1.0, 2.0, 3.0}),
    memgraph::storage::ExternalPropertyValue(memgraph::storage::Point3d{
        memgraph::storage::CoordinateReferenceSystem::Cartesian_3d, 1.0, 2.0, 3.0}),
    memgraph::storage::ExternalPropertyValue(memgraph::storage::ExternalPropertyValue::VectorIndexIdData{
        memgraph::storage::ExternalPropertyValue::vector_index_id_t{"test_index"},
        memgraph::utils::small_vector<float>{1.0f, 2.0f, 3.0f}}));

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define GENERATE_PARTIAL_READ_TEST(name, value)                                                \
  TYPED_TEST(DecoderEncoderTest, PartialRead##name) {                                          \
    {                                                                                          \
      memgraph::storage::durability::Encoder<TypeParam> encoder;                               \
      encoder.Initialize(this->storage_file, kTestMagic, kTestVersion);                        \
      encoder.Write##name(value);                                                              \
      encoder.Finalize();                                                                      \
    }                                                                                          \
    {                                                                                          \
      memgraph::utils::InputFile ifile;                                                        \
      memgraph::utils::OutputFile ofile;                                                       \
      ASSERT_TRUE(ifile.Open(this->storage_file));                                             \
      ofile.Open(this->alternate_file, memgraph::utils::OutputFile::Mode::OVERWRITE_EXISTING); \
      auto size = ifile.GetSize();                                                             \
      for (size_t i = 0; i <= size; ++i) {                                                     \
        if (i != 0) {                                                                          \
          uint8_t byte;                                                                        \
          ASSERT_TRUE(ifile.Read(&byte, sizeof(byte)));                                        \
          ofile.Write(&byte, sizeof(byte));                                                    \
          ofile.Sync();                                                                        \
        }                                                                                      \
        memgraph::storage::durability::Decoder decoder;                                        \
        auto version = decoder.Initialize(this->alternate_file, kTestMagic);                   \
        if (i < kTestMagic.size() + sizeof(kTestVersion)) {                                    \
          ASSERT_FALSE(version);                                                               \
        } else {                                                                               \
          ASSERT_TRUE(version);                                                                \
          ASSERT_EQ(*version, kTestVersion);                                                   \
        }                                                                                      \
        if (i != size) {                                                                       \
          ASSERT_FALSE(decoder.Read##name());                                                  \
        } else {                                                                               \
          auto decoded = decoder.Read##name();                                                 \
          ASSERT_TRUE(decoded);                                                                \
          ASSERT_EQ(*decoded, value);                                                          \
        }                                                                                      \
      }                                                                                        \
    }                                                                                          \
  }

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_PARTIAL_READ_TEST(Marker, memgraph::storage::durability::Marker::SECTION_VERTEX);

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_PARTIAL_READ_TEST(Bool, false);

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_PARTIAL_READ_TEST(Uint, 123'123'123);

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_PARTIAL_READ_TEST(Double, 3.1415926535);

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_PARTIAL_READ_TEST(String, "nandare");

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_PARTIAL_READ_TEST(
    ExternalPropertyValue,
    memgraph::storage::ExternalPropertyValue(std::vector<memgraph::storage::ExternalPropertyValue>{
        memgraph::storage::ExternalPropertyValue(),
        memgraph::storage::ExternalPropertyValue(true),
        memgraph::storage::ExternalPropertyValue(123L),
        memgraph::storage::ExternalPropertyValue(123.5),
        memgraph::storage::ExternalPropertyValue("nandare"),
        memgraph::storage::ExternalPropertyValue{
            memgraph::storage::ExternalPropertyValue::map_t{{"haihai", memgraph::storage::ExternalPropertyValue()}}},
        memgraph::storage::ExternalPropertyValue(memgraph::storage::TemporalData(memgraph::storage::TemporalType::Date,
                                                                                 23)),
        memgraph::storage::ExternalPropertyValue(
            memgraph::storage::ZonedTemporalData(memgraph::storage::ZonedTemporalType::ZonedDateTime,
                                                 memgraph::utils::AsSysTime(23), memgraph::utils::Timezone("Etc/UTC"))),
        memgraph::storage::ExternalPropertyValue(memgraph::storage::ZonedTemporalData(
            memgraph::storage::ZonedTemporalType::ZonedDateTime, memgraph::utils::AsSysTime(23),
            memgraph::utils::Timezone(std::chrono::minutes{-60}))),
        memgraph::storage::ExternalPropertyValue(memgraph::storage::Point2d{
            memgraph::storage::CoordinateReferenceSystem::WGS84_2d, 1.0, 2.0}),
        memgraph::storage::ExternalPropertyValue(memgraph::storage::Point2d{
            memgraph::storage::CoordinateReferenceSystem::Cartesian_2d, 1.0, 2.0}),
        memgraph::storage::ExternalPropertyValue(memgraph::storage::Point3d{
            memgraph::storage::CoordinateReferenceSystem::WGS84_3d, 1.0, 2.0, 3.0}),
        memgraph::storage::ExternalPropertyValue(memgraph::storage::Point3d{
            memgraph::storage::CoordinateReferenceSystem::Cartesian_3d, 1.0, 2.0, 3.0}),
        memgraph::storage::ExternalPropertyValue(memgraph::storage::ExternalPropertyValue::VectorIndexIdData{
            memgraph::storage::ExternalPropertyValue::vector_index_id_t{"test_index"},
            memgraph::utils::small_vector<float>{1.0f, 2.0f, 3.0f}})}));

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define GENERATE_PARTIAL_SKIP_TEST(name, value)                                                \
  TYPED_TEST(DecoderEncoderTest, PartialSkip##name) {                                          \
    {                                                                                          \
      memgraph::storage::durability::Encoder<TypeParam> encoder;                               \
      encoder.Initialize(this->storage_file, kTestMagic, kTestVersion);                        \
      encoder.Write##name(value);                                                              \
      encoder.Finalize();                                                                      \
    }                                                                                          \
    {                                                                                          \
      memgraph::utils::InputFile ifile;                                                        \
      memgraph::utils::OutputFile ofile;                                                       \
      ASSERT_TRUE(ifile.Open(this->storage_file));                                             \
      ofile.Open(this->alternate_file, memgraph::utils::OutputFile::Mode::OVERWRITE_EXISTING); \
      auto size = ifile.GetSize();                                                             \
      for (size_t i = 0; i <= size; ++i) {                                                     \
        if (i != 0) {                                                                          \
          uint8_t byte;                                                                        \
          ASSERT_TRUE(ifile.Read(&byte, sizeof(byte)));                                        \
          ofile.Write(&byte, sizeof(byte));                                                    \
          ofile.Sync();                                                                        \
        }                                                                                      \
        memgraph::storage::durability::Decoder decoder;                                        \
        auto version = decoder.Initialize(this->alternate_file, kTestMagic);                   \
        if (i < kTestMagic.size() + sizeof(kTestVersion)) {                                    \
          ASSERT_FALSE(version);                                                               \
        } else {                                                                               \
          ASSERT_TRUE(version);                                                                \
          ASSERT_EQ(*version, kTestVersion);                                                   \
        }                                                                                      \
        if (i != size) {                                                                       \
          ASSERT_FALSE(decoder.Skip##name());                                                  \
        } else {                                                                               \
          ASSERT_TRUE(decoder.Skip##name());                                                   \
        }                                                                                      \
      }                                                                                        \
    }                                                                                          \
  }

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_PARTIAL_SKIP_TEST(String, "nandare");

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_PARTIAL_SKIP_TEST(
    ExternalPropertyValue,
    memgraph::storage::ExternalPropertyValue(std::vector<memgraph::storage::ExternalPropertyValue>{
        memgraph::storage::ExternalPropertyValue(),
        memgraph::storage::ExternalPropertyValue(true),
        memgraph::storage::ExternalPropertyValue(123L),
        memgraph::storage::ExternalPropertyValue(123.5),
        memgraph::storage::ExternalPropertyValue("nandare"),
        memgraph::storage::ExternalPropertyValue{
            memgraph::storage::ExternalPropertyValue::map_t{{"haihai", memgraph::storage::ExternalPropertyValue()}}},
        memgraph::storage::ExternalPropertyValue(memgraph::storage::TemporalData(memgraph::storage::TemporalType::Date,
                                                                                 23)),
        memgraph::storage::ExternalPropertyValue(
            memgraph::storage::ZonedTemporalData(memgraph::storage::ZonedTemporalType::ZonedDateTime,
                                                 memgraph::utils::AsSysTime(23), memgraph::utils::Timezone("Etc/UTC"))),
        memgraph::storage::ExternalPropertyValue(memgraph::storage::ZonedTemporalData(
            memgraph::storage::ZonedTemporalType::ZonedDateTime, memgraph::utils::AsSysTime(23),
            memgraph::utils::Timezone(std::chrono::minutes{-60}))),
        memgraph::storage::ExternalPropertyValue(memgraph::storage::Point2d{
            memgraph::storage::CoordinateReferenceSystem::WGS84_2d, 1.0, 2.0}),
        memgraph::storage::ExternalPropertyValue(memgraph::storage::Point2d{
            memgraph::storage::CoordinateReferenceSystem::Cartesian_2d, 1.0, 2.0}),
        memgraph::storage::ExternalPropertyValue(memgraph::storage::Point3d{
            memgraph::storage::CoordinateReferenceSystem::WGS84_3d, 1.0, 2.0, 3.0}),
        memgraph::storage::ExternalPropertyValue(memgraph::storage::Point3d{
            memgraph::storage::CoordinateReferenceSystem::Cartesian_3d, 1.0, 2.0, 3.0}),
        memgraph::storage::ExternalPropertyValue(memgraph::storage::ExternalPropertyValue::VectorIndexIdData{
            memgraph::storage::ExternalPropertyValue::vector_index_id_t{"test_index"},
            memgraph::utils::small_vector<float>{1.0F, 2.0F, 3.0F}})}));

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DecoderEncoderTest, PropertyValueInvalidMarker) {
  {
    memgraph::storage::durability::Encoder<TypeParam> encoder;
    encoder.Initialize(this->storage_file, kTestMagic, kTestVersion);
    encoder.WriteExternalPropertyValue(memgraph::storage::ExternalPropertyValue(123L));
    encoder.Finalize();
  }
  {
    memgraph::utils::OutputFile file;
    file.Open(this->storage_file, memgraph::utils::OutputFile::Mode::OVERWRITE_EXISTING);
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
        case memgraph::storage::durability::Marker::TYPE_ZONED_TEMPORAL_DATA:
        case memgraph::storage::durability::Marker::TYPE_PROPERTY_VALUE:
        case memgraph::storage::durability::Marker::TYPE_ENUM:
        case memgraph::storage::durability::Marker::TYPE_POINT_2D:
        case memgraph::storage::durability::Marker::TYPE_POINT_3D:
        case memgraph::storage::durability::Marker::TYPE_VECTOR_INDEX_ID:
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
        case memgraph::storage::durability::Marker::SECTION_EDGE_INDICES:
        case memgraph::storage::durability::Marker::SECTION_OFFSETS:
        case memgraph::storage::durability::Marker::SECTION_ENUMS:
        case memgraph::storage::durability::Marker::SECTION_TTL:
        case memgraph::storage::durability::Marker::DELTA_VERTEX_CREATE:
        case memgraph::storage::durability::Marker::DELTA_VERTEX_DELETE:
        case memgraph::storage::durability::Marker::DELTA_VERTEX_ADD_LABEL:
        case memgraph::storage::durability::Marker::DELTA_VERTEX_REMOVE_LABEL:
        case memgraph::storage::durability::Marker::DELTA_VERTEX_SET_PROPERTY:
        case memgraph::storage::durability::Marker::DELTA_EDGE_CREATE:
        case memgraph::storage::durability::Marker::DELTA_EDGE_DELETE:
        case memgraph::storage::durability::Marker::DELTA_EDGE_SET_PROPERTY:
        case memgraph::storage::durability::Marker::DELTA_TRANSACTION_START:
        case memgraph::storage::durability::Marker::DELTA_TRANSACTION_END:
        case memgraph::storage::durability::Marker::DELTA_LABEL_INDEX_CREATE:
        case memgraph::storage::durability::Marker::DELTA_LABEL_INDEX_DROP:
        case memgraph::storage::durability::Marker::DELTA_POINT_INDEX_CREATE:
        case memgraph::storage::durability::Marker::DELTA_POINT_INDEX_DROP:
        case memgraph::storage::durability::Marker::DELTA_VECTOR_INDEX_CREATE:
        case memgraph::storage::durability::Marker::DELTA_VECTOR_EDGE_INDEX_CREATE:
        case memgraph::storage::durability::Marker::DELTA_VECTOR_INDEX_DROP:
        case memgraph::storage::durability::Marker::DELTA_LABEL_INDEX_STATS_SET:
        case memgraph::storage::durability::Marker::DELTA_LABEL_INDEX_STATS_CLEAR:
        case memgraph::storage::durability::Marker::DELTA_LABEL_PROPERTIES_INDEX_CREATE:
        case memgraph::storage::durability::Marker::DELTA_LABEL_PROPERTIES_INDEX_DROP:
        case memgraph::storage::durability::Marker::DELTA_LABEL_PROPERTIES_INDEX_STATS_SET:
        case memgraph::storage::durability::Marker::DELTA_LABEL_PROPERTIES_INDEX_STATS_CLEAR:
        case memgraph::storage::durability::Marker::DELTA_EDGE_INDEX_CREATE:
        case memgraph::storage::durability::Marker::DELTA_EDGE_INDEX_DROP:
        case memgraph::storage::durability::Marker::DELTA_EDGE_PROPERTY_INDEX_CREATE:
        case memgraph::storage::durability::Marker::DELTA_EDGE_PROPERTY_INDEX_DROP:
        case memgraph::storage::durability::Marker::DELTA_GLOBAL_EDGE_PROPERTY_INDEX_CREATE:
        case memgraph::storage::durability::Marker::DELTA_GLOBAL_EDGE_PROPERTY_INDEX_DROP:
        case memgraph::storage::durability::Marker::DELTA_GLOBAL_VERTEX_PROPERTY_INDEX_CREATE:
        case memgraph::storage::durability::Marker::DELTA_GLOBAL_VERTEX_PROPERTY_INDEX_DROP:
        case memgraph::storage::durability::Marker::DELTA_TEXT_INDEX_CREATE:
        case memgraph::storage::durability::Marker::DELTA_TEXT_EDGE_INDEX_CREATE:
        case memgraph::storage::durability::Marker::DELTA_TEXT_INDEX_DROP:
        case memgraph::storage::durability::Marker::DELTA_EXISTENCE_CONSTRAINT_CREATE:
        case memgraph::storage::durability::Marker::DELTA_EXISTENCE_CONSTRAINT_DROP:
        case memgraph::storage::durability::Marker::DELTA_UNIQUE_CONSTRAINT_CREATE:
        case memgraph::storage::durability::Marker::DELTA_UNIQUE_CONSTRAINT_DROP:
        case memgraph::storage::durability::Marker::DELTA_TYPE_CONSTRAINT_CREATE:
        case memgraph::storage::durability::Marker::DELTA_TYPE_CONSTRAINT_DROP:
        case memgraph::storage::durability::Marker::DELTA_ENUM_CREATE:
        case memgraph::storage::durability::Marker::DELTA_ENUM_ALTER_ADD:
        case memgraph::storage::durability::Marker::DELTA_ENUM_ALTER_UPDATE:
        case memgraph::storage::durability::Marker::DELTA_TTL_OPERATION:
        case memgraph::storage::durability::Marker::SECTION_DESCRIPTIONS:
        case memgraph::storage::durability::Marker::DELTA_DESCRIPTION_SET:
        case memgraph::storage::durability::Marker::DELTA_DESCRIPTION_DELETE:
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
        auto version = decoder.Initialize(this->storage_file, kTestMagic);
        ASSERT_TRUE(version);
        ASSERT_EQ(*version, kTestVersion);
        ASSERT_FALSE(decoder.SkipExternalPropertyValue());
      }
      {
        memgraph::storage::durability::Decoder decoder;
        auto version = decoder.Initialize(this->storage_file, kTestMagic);
        ASSERT_TRUE(version);
        ASSERT_EQ(*version, kTestVersion);
        ASSERT_FALSE(decoder.ReadExternalPropertyValue());
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
        auto version = decoder.Initialize(this->storage_file, kTestMagic);
        ASSERT_TRUE(version);
        ASSERT_EQ(*version, kTestVersion);
        ASSERT_FALSE(decoder.SkipExternalPropertyValue());
      }
      {
        memgraph::storage::durability::Decoder decoder;
        auto version = decoder.Initialize(this->storage_file, kTestMagic);
        ASSERT_TRUE(version);
        ASSERT_EQ(*version, kTestVersion);
        ASSERT_FALSE(decoder.ReadExternalPropertyValue());
      }
    }
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DecoderEncoderTest, DecoderPosition) {
  {
    memgraph::storage::durability::Encoder<TypeParam> encoder;
    encoder.Initialize(this->storage_file, kTestMagic, kTestVersion);
    encoder.WriteBool(true);
    encoder.Finalize();
  }
  {
    memgraph::storage::durability::Decoder decoder;
    auto version = decoder.Initialize(this->storage_file, kTestMagic);
    ASSERT_TRUE(version);
    ASSERT_EQ(*version, kTestVersion);
    for (int i = 0; i < 10; ++i) {
      ASSERT_TRUE(decoder.SetPosition(kTestMagic.size() + sizeof(kTestVersion)));
      auto decoded = decoder.ReadBool();
      ASSERT_TRUE(decoded);
      ASSERT_TRUE(*decoded);
      auto pos = decoder.GetPosition();
      ASSERT_EQ(pos, decoder.GetSize());
    }
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DecoderEncoderTest, EncoderPosition) {
  {
    memgraph::storage::durability::Encoder<TypeParam> encoder;
    encoder.Initialize(this->storage_file, kTestMagic, kTestVersion);
    encoder.WriteBool(false);
    encoder.SetPosition(kTestMagic.size() + sizeof(kTestVersion));
    ASSERT_EQ(encoder.GetPosition(), kTestMagic.size() + sizeof(kTestVersion));
    encoder.WriteBool(true);
    encoder.Finalize();
  }
  {
    memgraph::storage::durability::Decoder decoder;
    auto version = decoder.Initialize(this->storage_file, kTestMagic);
    ASSERT_TRUE(version);
    ASSERT_EQ(*version, kTestVersion);
    auto decoded = decoder.ReadBool();
    ASSERT_TRUE(decoded);
    ASSERT_TRUE(*decoded);
    auto pos = decoder.GetPosition();
    ASSERT_EQ(pos, decoder.GetSize());
  }
}
