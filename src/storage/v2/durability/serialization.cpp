// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <chrono>
#include <cstdint>
#include <optional>

#include "storage/v2/durability/marker.hpp"
#include "storage/v2/durability/serialization.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/temporal.hpp"
#include "utils/cast.hpp"
#include "utils/endian.hpp"
#include "utils/temporal.hpp"

namespace memgraph::storage::durability {

//////////////////////////
// Encoder implementation.
//////////////////////////

namespace {
template <typename FileType>
void WriteSize(Encoder<FileType> *encoder, uint64_t size) {
  size = utils::HostToLittleEndian(size);
  encoder->Write(reinterpret_cast<const uint8_t *>(&size), sizeof(size));
}
}  // namespace

template <typename FileType>
void Encoder<FileType>::Initialize(const std::filesystem::path &path) {
  file_.Open(path, FileType::Mode::OVERWRITE_EXISTING);
}

template <typename FileType>
void Encoder<FileType>::Initialize(const std::filesystem::path &path, const std::string_view magic, uint64_t version) {
  Initialize(path);
  Write(reinterpret_cast<const uint8_t *>(magic.data()), magic.size());
  auto version_encoded = utils::HostToLittleEndian(version);
  Write(reinterpret_cast<const uint8_t *>(&version_encoded), sizeof(version_encoded));
}

template <typename FileType>
void Encoder<FileType>::OpenExisting(const std::filesystem::path &path) {
  file_.Open(path, FileType::Mode::APPEND_TO_EXISTING);
}

template <typename FileType>
void Encoder<FileType>::Close() {
  if (file_.IsOpen()) {
    file_.Close();
  }
}

template <typename FileType>
void Encoder<FileType>::Write(const uint8_t *data, uint64_t size) {
  file_.Write(data, size);
}

template <typename FileType>
void Encoder<FileType>::WriteMarker(Marker marker) {
  auto value = static_cast<uint8_t>(marker);
  Write(&value, sizeof(value));
}

template <typename FileType>
void Encoder<FileType>::WriteBool(bool const value) {
  WriteMarker(Marker::TYPE_BOOL);
  if (value) {
    WriteMarker(Marker::VALUE_TRUE);
  } else {
    WriteMarker(Marker::VALUE_FALSE);
  }
}

template <typename FileType>
void Encoder<FileType>::WriteUint(uint64_t value) {
  value = utils::HostToLittleEndian(value);
  WriteMarker(Marker::TYPE_INT);
  Write(reinterpret_cast<const uint8_t *>(&value), sizeof(value));
}

template <typename FileType>
void Encoder<FileType>::WriteDouble(double value) {
  auto value_uint = utils::MemcpyCast<uint64_t>(value);
  value_uint = utils::HostToLittleEndian(value_uint);
  WriteMarker(Marker::TYPE_DOUBLE);
  Write(reinterpret_cast<const uint8_t *>(&value_uint), sizeof(value_uint));
}

template <typename FileType>
void Encoder<FileType>::WriteString(const std::string_view value) {
  WriteMarker(Marker::TYPE_STRING);
  WriteSize(this, value.size());
  Write(reinterpret_cast<const uint8_t *>(value.data()), value.size());
}

template <typename FileType>
void Encoder<FileType>::WriteEnum(storage::Enum value) {
  WriteMarker(Marker::TYPE_ENUM);
  auto etype = utils::HostToLittleEndian(value.type_id().value_of());
  Write(reinterpret_cast<const uint8_t *>(&etype), sizeof(etype));
  auto evalue = utils::HostToLittleEndian(value.value_id().value_of());
  Write(reinterpret_cast<const uint8_t *>(&evalue), sizeof(evalue));
}

template <typename FileType>
void Encoder<FileType>::WritePoint2d(storage::Point2d value) {
  WriteMarker(Marker::TYPE_POINT_2D);
  WriteUint(CrsToSrid(value.crs()).value_of());
  WriteDouble(value.x());
  WriteDouble(value.y());
}

template <typename FileType>
void Encoder<FileType>::WritePoint3d(storage::Point3d value) {
  WriteMarker(Marker::TYPE_POINT_3D);
  WriteUint(CrsToSrid(value.crs()).value_of());
  WriteDouble(value.x());
  WriteDouble(value.y());
  WriteDouble(value.z());
}

template <typename FileType>
void Encoder<FileType>::WriteExternalPropertyValue(const ExternalPropertyValue &value) {
  WriteMarker(Marker::TYPE_PROPERTY_VALUE);
  switch (value.type()) {
    case ExternalPropertyValue::Type::Null: {
      WriteMarker(Marker::TYPE_NULL);
      break;
    }
    case ExternalPropertyValue::Type::Bool: {
      WriteBool(value.ValueBool());
      break;
    }
    case ExternalPropertyValue::Type::Int: {
      WriteUint(utils::MemcpyCast<uint64_t>(value.ValueInt()));
      break;
    }
    case ExternalPropertyValue::Type::Double: {
      WriteDouble(value.ValueDouble());
      break;
    }
    case ExternalPropertyValue::Type::String: {
      WriteString(value.ValueString());
      break;
    }
    case ExternalPropertyValue::Type::List: {
      const auto &list = value.ValueList();
      WriteMarker(Marker::TYPE_LIST);
      WriteSize(this, list.size());
      for (const auto &item : list) {
        WriteExternalPropertyValue(item);
      }
      break;
    }
    case ExternalPropertyValue::Type::Map: {
      const auto &map = value.ValueMap();
      WriteMarker(Marker::TYPE_MAP);
      WriteSize(this, map.size());
      for (const auto &item : map) {
        WriteString(item.first);
        WriteExternalPropertyValue(item.second);
      }
      break;
    }
    case ExternalPropertyValue::Type::TemporalData: {
      const auto temporal_data = value.ValueTemporalData();
      WriteMarker(Marker::TYPE_TEMPORAL_DATA);
      WriteUint(static_cast<uint64_t>(temporal_data.type));
      WriteUint(utils::MemcpyCast<uint64_t>(temporal_data.microseconds));
      break;
    }
    case ExternalPropertyValue::Type::ZonedTemporalData: {
      const auto zoned_temporal_data = value.ValueZonedTemporalData();
      WriteMarker(Marker::TYPE_ZONED_TEMPORAL_DATA);
      WriteUint(static_cast<uint64_t>(zoned_temporal_data.type));
      WriteUint(utils::MemcpyCast<uint64_t>(zoned_temporal_data.IntMicroseconds()));
      if (zoned_temporal_data.timezone.InTzDatabase()) {
        WriteString(zoned_temporal_data.timezone.TimezoneName());
      } else {
        WriteUint(zoned_temporal_data.timezone.DefiningOffset());
      }
      break;
    }
    case ExternalPropertyValue::Type::Enum: {
      WriteEnum(value.ValueEnum());
      break;
    }
    case ExternalPropertyValue::Type::Point2d: {
      WritePoint2d(value.ValuePoint2d());
      break;
    }
    case ExternalPropertyValue::Type::Point3d: {
      WritePoint3d(value.ValuePoint3d());
      break;
    }
  }
}

template <typename FileType>
uint64_t Encoder<FileType>::GetPosition() {
  return file_.GetPosition();
}

template <typename FileType>
void Encoder<FileType>::SetPosition(uint64_t position) {
  file_.SetPosition(FileType::Position::SET, position);
}

template <typename FileType>
void Encoder<FileType>::Sync() {
  file_.Sync();
}

template <typename FileType>
void Encoder<FileType>::Finalize() {
  file_.Sync();
  file_.Close();
}

template <typename FileType>
void Encoder<FileType>::DisableFlushing() requires std::same_as<FileType, utils::OutputFile> {
  file_.DisableFlushing();
}

template <typename FileType>
void Encoder<FileType>::EnableFlushing() requires std::same_as<FileType, utils::OutputFile> {
  file_.EnableFlushing();
}

template <typename FileType>
void Encoder<FileType>::TryFlushing() requires std::same_as<FileType, utils::OutputFile> {
  file_.TryFlushing();
}

template <typename FileType>
std::pair<const uint8_t *, size_t> Encoder<FileType>::CurrentFileBuffer() const {
  return file_.CurrentBuffer();
}

template <typename FileType>
size_t Encoder<FileType>::GetSize() {
  return file_.GetSize();
}

template class Encoder<utils::OutputFile>;
template class Encoder<utils::NonConcurrentOutputFile>;

//////////////////////////
// Decoder implementation.
//////////////////////////

namespace {

constexpr bool isValidMarkerValue(uint8_t v) {
  // build lookup bitmaps
  // 0b1100'0000 - top two bits, bitmap selector (2^2 == 4), hence 4 bitmaps
  // 0b0011'0000 - bottom 6 bits, bitmap position (2^6 == 64), hence uint64_t
  constexpr auto validMarkerBitMaps = std::invoke([] {
    auto arr = std::array<uint64_t, 4>{};
    for (auto const valid_marker : kMarkersAll) {
      auto const as_u8 = static_cast<uint8_t>(valid_marker);
      arr[as_u8 >> 6UL] |= (1UL << (as_u8 & 0x3FUL));
    }
    return arr;
  });
  return validMarkerBitMaps[v >> 6UL] & (1UL << (v & 0x3FUL));
}

std::optional<Marker> CastToMarker(uint8_t value) {
  if (isValidMarkerValue(value)) return static_cast<Marker>(value);
  return std::nullopt;
}

std::optional<uint64_t> ReadSize(Decoder *decoder) {
  uint64_t size;
  if (!decoder->Read(reinterpret_cast<uint8_t *>(&size), sizeof(size))) return std::nullopt;
  size = utils::LittleEndianToHost(size);
  return size;
}
}  // namespace

std::optional<uint64_t> Decoder::Initialize(const std::filesystem::path &path, const std::string &magic) {
  if (!file_.Open(path)) return std::nullopt;
  std::string file_magic(magic.size(), '\0');
  if (!Read(reinterpret_cast<uint8_t *>(file_magic.data()), file_magic.size())) return std::nullopt;
  if (file_magic != magic) return std::nullopt;
  uint64_t version_encoded;
  if (!Read(reinterpret_cast<uint8_t *>(&version_encoded), sizeof(version_encoded))) return std::nullopt;
  return utils::LittleEndianToHost(version_encoded);
}

bool Decoder::Read(uint8_t *data, size_t size) { return file_.Read(data, size); }

bool Decoder::Peek(uint8_t *data, size_t size) { return file_.Peek(data, size); }

std::optional<Marker> Decoder::PeekMarker() {
  uint8_t value;
  if (!Peek(&value, sizeof(value))) return std::nullopt;
  return CastToMarker(value);
}

std::optional<Marker> Decoder::ReadMarker() {
  uint8_t value;
  if (!Read(&value, sizeof(value))) return std::nullopt;
  return CastToMarker(value);
}

std::optional<bool> Decoder::ReadBool() {
  auto marker = ReadMarker();
  if (!marker || *marker != Marker::TYPE_BOOL) return std::nullopt;
  auto value = ReadMarker();
  if (!value || (*value != Marker::VALUE_FALSE && *value != Marker::VALUE_TRUE)) return std::nullopt;
  return *value == Marker::VALUE_TRUE;
}

std::optional<uint64_t> Decoder::ReadUint() {
  auto marker = ReadMarker();
  if (!marker || *marker != Marker::TYPE_INT) return std::nullopt;
  uint64_t value;
  if (!Read(reinterpret_cast<uint8_t *>(&value), sizeof(value))) return std::nullopt;
  return utils::LittleEndianToHost(value);
}

std::optional<double> Decoder::ReadDouble() {
  auto marker = ReadMarker();
  if (!marker || *marker != Marker::TYPE_DOUBLE) return std::nullopt;
  uint64_t value_int;
  if (!Read(reinterpret_cast<uint8_t *>(&value_int), sizeof(value_int))) return std::nullopt;
  value_int = utils::LittleEndianToHost(value_int);
  return utils::MemcpyCast<double>(value_int);
}

std::optional<std::string> Decoder::ReadString() {
  auto marker = ReadMarker();
  if (!marker || *marker != Marker::TYPE_STRING) return std::nullopt;
  auto size = ReadSize(this);
  if (!size) return std::nullopt;
  std::string value(*size, '\0');
  if (!Read(reinterpret_cast<uint8_t *>(value.data()), *size)) return std::nullopt;
  return value;
}

std::optional<Enum> Decoder::ReadEnumValue() {
  auto marker = ReadMarker();
  if (!marker || *marker != Marker::TYPE_ENUM) return std::nullopt;
  // NOLINTNEXTLINE(cppcoreguidelines-init-variables)
  uint64_t etype;
  if (!Read(reinterpret_cast<uint8_t *>(&etype), sizeof(etype))) return std::nullopt;
  etype = utils::LittleEndianToHost(etype);
  // NOLINTNEXTLINE(cppcoreguidelines-init-variables)
  uint64_t evalue;
  if (!Read(reinterpret_cast<uint8_t *>(&evalue), sizeof(evalue))) return std::nullopt;
  evalue = utils::LittleEndianToHost(evalue);
  return Enum{EnumTypeId{etype}, EnumValueId{evalue}};
}

std::optional<Point2d> Decoder::ReadPoint2dValue() {
  auto marker = ReadMarker();
  if (!marker || *marker != Marker::TYPE_POINT_2D) return std::nullopt;

  const auto srid = ReadUint();
  if (!srid) return std::nullopt;

  auto crs = SridToCrs(Srid{*srid});
  if (!crs) return std::nullopt;
  if (!valid2d(*crs)) return std::nullopt;

  const auto x = ReadDouble();
  if (!x) return std::nullopt;

  const auto y = ReadDouble();
  if (!y) return std::nullopt;

  return Point2d{*crs, *x, *y};
}

std::optional<Point3d> Decoder::ReadPoint3dValue() {
  auto marker = ReadMarker();
  if (!marker || *marker != Marker::TYPE_POINT_3D) return std::nullopt;

  const auto srid = ReadUint();
  if (!srid) return std::nullopt;

  auto crs = SridToCrs(Srid{*srid});
  if (!crs) return std::nullopt;
  if (!valid3d(*crs)) return std::nullopt;

  const auto x = ReadDouble();
  if (!x) return std::nullopt;

  const auto y = ReadDouble();
  if (!y) return std::nullopt;

  const auto z = ReadDouble();
  if (!z) return std::nullopt;

  return Point3d{*crs, *x, *y, *z};
}

namespace {
std::optional<TemporalData> ReadTemporalData(Decoder &decoder) {
  const auto inner_marker = decoder.ReadMarker();
  if (!inner_marker || *inner_marker != Marker::TYPE_TEMPORAL_DATA) return std::nullopt;

  const auto type = decoder.ReadUint();
  if (!type) return std::nullopt;

  const auto microseconds = decoder.ReadUint();
  if (!microseconds) return std::nullopt;

  return TemporalData{static_cast<TemporalType>(*type), utils::MemcpyCast<int64_t>(*microseconds)};
}

std::optional<ZonedTemporalData> ReadZonedTemporalData(Decoder &decoder) {
  const auto inner_marker = decoder.ReadMarker();
  if (!inner_marker || *inner_marker != Marker::TYPE_ZONED_TEMPORAL_DATA) return std::nullopt;

  const auto type = decoder.ReadUint();
  if (!type) return std::nullopt;

  const auto microseconds = decoder.ReadUint();
  if (!microseconds) return std::nullopt;

  auto marker = decoder.PeekMarker();
  if (!marker) return std::nullopt;
  switch (*marker) {
    case Marker::TYPE_STRING: {
      auto timezone_name = decoder.ReadString();
      if (!timezone_name) return std::nullopt;
      return ZonedTemporalData{static_cast<ZonedTemporalType>(*type),
                               utils::AsSysTime(utils::MemcpyCast<int64_t>(*microseconds)),
                               utils::Timezone(*timezone_name)};
    }
    case Marker::TYPE_INT: {
      auto offset_minutes = decoder.ReadUint();
      if (!offset_minutes) return std::nullopt;
      return ZonedTemporalData{static_cast<ZonedTemporalType>(*type),
                               utils::AsSysTime(utils::MemcpyCast<int64_t>(*microseconds)),
                               utils::Timezone(std::chrono::minutes{*offset_minutes})};
    }
    default:
      break;
  }
  return std::nullopt;
}
}  // namespace

std::optional<ExternalPropertyValue> Decoder::ReadExternalPropertyValue() {
  auto pv_marker = ReadMarker();
  if (!pv_marker || *pv_marker != Marker::TYPE_PROPERTY_VALUE) return std::nullopt;

  auto marker = PeekMarker();
  if (!marker) return std::nullopt;
  switch (*marker) {
    case Marker::TYPE_NULL: {
      auto inner_marker = ReadMarker();
      if (!inner_marker || *inner_marker != Marker::TYPE_NULL) return std::nullopt;
      return ExternalPropertyValue();
    }
    case Marker::TYPE_BOOL: {
      auto value = ReadBool();
      if (!value) return std::nullopt;
      return ExternalPropertyValue(*value);
    }
    case Marker::TYPE_INT: {
      auto value = ReadUint();
      if (!value) return std::nullopt;
      return ExternalPropertyValue(utils::MemcpyCast<int64_t>(*value));
    }
    case Marker::TYPE_DOUBLE: {
      auto value = ReadDouble();
      if (!value) return std::nullopt;
      return ExternalPropertyValue(*value);
    }
    case Marker::TYPE_STRING: {
      auto value = ReadString();
      if (!value) return std::nullopt;
      return ExternalPropertyValue(std::move(*value));
    }
    case Marker::TYPE_LIST: {
      auto inner_marker = ReadMarker();
      if (!inner_marker || *inner_marker != Marker::TYPE_LIST) return std::nullopt;
      auto size = ReadSize(this);
      if (!size) return std::nullopt;
      std::vector<ExternalPropertyValue> value;
      value.reserve(*size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto item = ReadExternalPropertyValue();
        if (!item) return std::nullopt;
        value.emplace_back(std::move(*item));
      }
      return ExternalPropertyValue(std::move(value));
    }
    case Marker::TYPE_MAP: {
      auto inner_marker = ReadMarker();
      if (!inner_marker || *inner_marker != Marker::TYPE_MAP) return std::nullopt;
      auto size = ReadSize(this);
      if (!size) return std::nullopt;
      auto value = ExternalPropertyValue::map_t{};
      do_reserve(value, *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto key = ReadString();
        if (!key) return std::nullopt;
        auto item = ReadExternalPropertyValue();
        if (!item) return std::nullopt;
        value.emplace(std::move(*key), std::move(*item));
      }
      return ExternalPropertyValue(std::move(value));
    }
    case Marker::TYPE_TEMPORAL_DATA: {
      const auto maybe_temporal_data = ReadTemporalData(*this);
      if (!maybe_temporal_data) return std::nullopt;
      return ExternalPropertyValue(*maybe_temporal_data);
    }
    case Marker::TYPE_ZONED_TEMPORAL_DATA: {
      const auto maybe_zoned_temporal_data = ReadZonedTemporalData(*this);
      if (!maybe_zoned_temporal_data) return std::nullopt;
      return ExternalPropertyValue(*maybe_zoned_temporal_data);
    }
    case Marker::TYPE_ENUM: {
      const auto maybe_enum_value = ReadEnumValue();
      if (!maybe_enum_value) return std::nullopt;
      return ExternalPropertyValue(*maybe_enum_value);
    }
    case Marker::TYPE_POINT_2D: {
      const auto maybe_point_2d_value = ReadPoint2dValue();
      if (!maybe_point_2d_value) return std::nullopt;
      return ExternalPropertyValue(*maybe_point_2d_value);
    }
    case Marker::TYPE_POINT_3D: {
      const auto maybe_point_3d_value = ReadPoint3dValue();
      if (!maybe_point_3d_value) return std::nullopt;
      return ExternalPropertyValue(*maybe_point_3d_value);
    }

    case Marker::TYPE_PROPERTY_VALUE:
    case Marker::SECTION_VERTEX:
    case Marker::SECTION_EDGE:
    case Marker::SECTION_MAPPER:
    case Marker::SECTION_METADATA:
    case Marker::SECTION_INDICES:
    case Marker::SECTION_CONSTRAINTS:
    case Marker::SECTION_DELTA:
    case Marker::SECTION_EPOCH_HISTORY:
    case Marker::SECTION_EDGE_INDICES:
    case Marker::SECTION_OFFSETS:
    case Marker::SECTION_ENUMS:
    case Marker::DELTA_VERTEX_CREATE:
    case Marker::DELTA_VERTEX_DELETE:
    case Marker::DELTA_VERTEX_ADD_LABEL:
    case Marker::DELTA_VERTEX_REMOVE_LABEL:
    case Marker::DELTA_VERTEX_SET_PROPERTY:
    case Marker::DELTA_EDGE_CREATE:
    case Marker::DELTA_EDGE_DELETE:
    case Marker::DELTA_EDGE_SET_PROPERTY:
    case Marker::DELTA_TRANSACTION_START:
    case Marker::DELTA_TRANSACTION_END:
    case Marker::DELTA_LABEL_INDEX_CREATE:
    case Marker::DELTA_LABEL_INDEX_DROP:
    case Marker::DELTA_LABEL_INDEX_STATS_SET:
    case Marker::DELTA_LABEL_INDEX_STATS_CLEAR:
    case Marker::DELTA_LABEL_PROPERTIES_INDEX_STATS_SET:
    case Marker::DELTA_LABEL_PROPERTIES_INDEX_STATS_CLEAR:
    case Marker::DELTA_LABEL_PROPERTIES_INDEX_CREATE:
    case Marker::DELTA_LABEL_PROPERTIES_INDEX_DROP:
    case Marker::DELTA_EDGE_INDEX_CREATE:
    case Marker::DELTA_EDGE_INDEX_DROP:
    case Marker::DELTA_EDGE_PROPERTY_INDEX_CREATE:
    case Marker::DELTA_EDGE_PROPERTY_INDEX_DROP:
    case Marker::DELTA_GLOBAL_EDGE_PROPERTY_INDEX_CREATE:
    case Marker::DELTA_GLOBAL_EDGE_PROPERTY_INDEX_DROP:
    case Marker::DELTA_TEXT_INDEX_CREATE:
    case Marker::DELTA_TEXT_INDEX_DROP:
    case Marker::DELTA_EXISTENCE_CONSTRAINT_CREATE:
    case Marker::DELTA_EXISTENCE_CONSTRAINT_DROP:
    case Marker::DELTA_UNIQUE_CONSTRAINT_CREATE:
    case Marker::DELTA_UNIQUE_CONSTRAINT_DROP:
    case Marker::DELTA_ENUM_CREATE:
    case Marker::DELTA_ENUM_ALTER_ADD:
    case Marker::DELTA_ENUM_ALTER_UPDATE:
    case Marker::DELTA_POINT_INDEX_CREATE:
    case Marker::DELTA_POINT_INDEX_DROP:
    case Marker::DELTA_VECTOR_INDEX_CREATE:
    case Marker::DELTA_VECTOR_EDGE_INDEX_CREATE:
    case Marker::DELTA_VECTOR_INDEX_DROP:
    case Marker::DELTA_TYPE_CONSTRAINT_CREATE:
    case Marker::DELTA_TYPE_CONSTRAINT_DROP:
    case Marker::VALUE_FALSE:
    case Marker::VALUE_TRUE:
      return std::nullopt;
  }
}

bool Decoder::SkipString() {
  auto marker = ReadMarker();
  if (!marker || *marker != Marker::TYPE_STRING) return false;
  auto maybe_size = ReadSize(this);
  if (!maybe_size) return false;

  const uint64_t kBufferSize = 262144;
  uint8_t buffer[kBufferSize];
  uint64_t size = *maybe_size;
  while (size > 0) {
    uint64_t to_read = size < kBufferSize ? size : kBufferSize;
    if (!Read(reinterpret_cast<uint8_t *>(&buffer), to_read)) return false;
    size -= to_read;
  }

  return true;
}

bool Decoder::SkipExternalPropertyValue() {
  auto pv_marker = ReadMarker();
  if (!pv_marker || *pv_marker != Marker::TYPE_PROPERTY_VALUE) return false;

  auto marker = PeekMarker();
  if (!marker) return false;
  switch (*marker) {
    case Marker::TYPE_NULL: {
      auto inner_marker = ReadMarker();
      return inner_marker && *inner_marker == Marker::TYPE_NULL;
    }
    case Marker::TYPE_BOOL: {
      return !!ReadBool();
    }
    case Marker::TYPE_INT: {
      return !!ReadUint();
    }
    case Marker::TYPE_DOUBLE: {
      return !!ReadDouble();
    }
    case Marker::TYPE_STRING: {
      return SkipString();
    }
    case Marker::TYPE_LIST: {
      auto inner_marker = ReadMarker();
      if (!inner_marker || *inner_marker != Marker::TYPE_LIST) return false;
      auto size = ReadSize(this);
      if (!size) return false;
      for (uint64_t i = 0; i < *size; ++i) {
        if (!SkipExternalPropertyValue()) return false;
      }
      return true;
    }
    case Marker::TYPE_MAP: {
      auto inner_marker = ReadMarker();
      if (!inner_marker || *inner_marker != Marker::TYPE_MAP) return false;
      auto size = ReadSize(this);
      if (!size) return false;
      for (uint64_t i = 0; i < *size; ++i) {
        if (!SkipString()) return false;
        if (!SkipExternalPropertyValue()) return false;
      }
      return true;
    }
    case Marker::TYPE_TEMPORAL_DATA: {
      return !!ReadTemporalData(*this);
    }
    case Marker::TYPE_ZONED_TEMPORAL_DATA: {
      return !!ReadZonedTemporalData(*this);
    }
    case Marker::TYPE_ENUM: {
      return !!ReadEnumValue();
    }
    case Marker::TYPE_POINT_2D: {
      return !!ReadPoint2dValue();
    }
    case Marker::TYPE_POINT_3D: {
      return !!ReadPoint3dValue();
    }

    case Marker::TYPE_PROPERTY_VALUE:
    case Marker::SECTION_VERTEX:
    case Marker::SECTION_EDGE:
    case Marker::SECTION_MAPPER:
    case Marker::SECTION_METADATA:
    case Marker::SECTION_INDICES:
    case Marker::SECTION_CONSTRAINTS:
    case Marker::SECTION_DELTA:
    case Marker::SECTION_EPOCH_HISTORY:
    case Marker::SECTION_EDGE_INDICES:
    case Marker::SECTION_OFFSETS:
    case Marker::SECTION_ENUMS:
    case Marker::DELTA_VERTEX_CREATE:
    case Marker::DELTA_VERTEX_DELETE:
    case Marker::DELTA_VERTEX_ADD_LABEL:
    case Marker::DELTA_VERTEX_REMOVE_LABEL:
    case Marker::DELTA_VERTEX_SET_PROPERTY:
    case Marker::DELTA_EDGE_CREATE:
    case Marker::DELTA_EDGE_DELETE:
    case Marker::DELTA_EDGE_SET_PROPERTY:
    case Marker::DELTA_TRANSACTION_START:
    case Marker::DELTA_TRANSACTION_END:
    case Marker::DELTA_LABEL_INDEX_CREATE:
    case Marker::DELTA_LABEL_INDEX_DROP:
    case Marker::DELTA_LABEL_INDEX_STATS_SET:
    case Marker::DELTA_LABEL_INDEX_STATS_CLEAR:
    case Marker::DELTA_LABEL_PROPERTIES_INDEX_STATS_SET:
    case Marker::DELTA_LABEL_PROPERTIES_INDEX_STATS_CLEAR:
    case Marker::DELTA_LABEL_PROPERTIES_INDEX_CREATE:
    case Marker::DELTA_LABEL_PROPERTIES_INDEX_DROP:
    case Marker::DELTA_EDGE_INDEX_CREATE:
    case Marker::DELTA_EDGE_INDEX_DROP:
    case Marker::DELTA_EDGE_PROPERTY_INDEX_CREATE:
    case Marker::DELTA_EDGE_PROPERTY_INDEX_DROP:
    case Marker::DELTA_GLOBAL_EDGE_PROPERTY_INDEX_CREATE:
    case Marker::DELTA_GLOBAL_EDGE_PROPERTY_INDEX_DROP:
    case Marker::DELTA_TEXT_INDEX_CREATE:
    case Marker::DELTA_TEXT_INDEX_DROP:
    case Marker::DELTA_EXISTENCE_CONSTRAINT_CREATE:
    case Marker::DELTA_EXISTENCE_CONSTRAINT_DROP:
    case Marker::DELTA_UNIQUE_CONSTRAINT_CREATE:
    case Marker::DELTA_UNIQUE_CONSTRAINT_DROP:
    case Marker::DELTA_ENUM_CREATE:
    case Marker::DELTA_ENUM_ALTER_ADD:
    case Marker::DELTA_ENUM_ALTER_UPDATE:
    case Marker::DELTA_POINT_INDEX_CREATE:
    case Marker::DELTA_POINT_INDEX_DROP:
    case Marker::DELTA_VECTOR_INDEX_CREATE:
    case Marker::DELTA_VECTOR_EDGE_INDEX_CREATE:
    case Marker::DELTA_VECTOR_INDEX_DROP:
    case Marker::DELTA_TYPE_CONSTRAINT_CREATE:
    case Marker::DELTA_TYPE_CONSTRAINT_DROP:
    case Marker::VALUE_FALSE:
    case Marker::VALUE_TRUE:
      return false;
  }
}

std::optional<uint64_t> Decoder::GetSize() { return file_.GetSize(); }

std::optional<uint64_t> Decoder::GetPosition() { return file_.GetPosition(); }

bool Decoder::SetPosition(uint64_t position) { return !!file_.SetPosition(utils::InputFile::Position::SET, position); }

}  // namespace memgraph::storage::durability
