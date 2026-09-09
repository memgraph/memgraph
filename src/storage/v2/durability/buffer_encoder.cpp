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

#include "storage/v2/durability/buffer_encoder.hpp"

#include <algorithm>
#include <bit>
#include <variant>

#include "storage/v2/durability/marker.hpp"
#include "storage/v2/point.hpp"
#include "storage/v2/temporal.hpp"
#include "utils/endian.hpp"

namespace memgraph::storage::durability {

namespace {
// The smallest allocation a buffer starts with; growth is geometric from there so a large transaction is charged
// a bounded number of times.
constexpr uint64_t kInitialCapacity = 64;
}  // namespace

BufferEncoder::BufferEncoder(TxnAllocPolicy policy) : policy_{policy}, buffer_{BudgetAllocator<uint8_t>{policy}} {}

void BufferEncoder::Write(const uint8_t *data, uint64_t size) {
  auto const needed = buffer_.size() + size;
  if (needed > buffer_.capacity()) {
    // The allocator charges the replacement while the old allocation is still charged and releases the old charge
    // only when the vector frees it, so a refused growth leaves the old buffer and its charge intact.
    buffer_.reserve(std::max({2 * buffer_.capacity(), needed, kInitialCapacity}));
  }
  buffer_.insert(buffer_.end(), data, data + size);
  crc_acc_.Update(data, size);
}

void BufferEncoder::WriteSize(uint64_t size) {
  size = utils::HostToLittleEndian(size);
  Write(reinterpret_cast<const uint8_t *>(&size), sizeof(size));
}

void BufferEncoder::WriteMarker(Marker marker) {
  auto value = static_cast<uint8_t>(marker);
  Write(&value, sizeof(value));
}

void BufferEncoder::WriteBool(bool const value) {
  WriteMarker(Marker::TYPE_BOOL);
  if (value) {
    WriteMarker(Marker::VALUE_TRUE);
  } else {
    WriteMarker(Marker::VALUE_FALSE);
  }
}

void BufferEncoder::WriteUint(uint64_t value) {
  value = utils::HostToLittleEndian(value);
  WriteMarker(Marker::TYPE_INT);
  Write(reinterpret_cast<const uint8_t *>(&value), sizeof(value));
}

uint32_t BufferEncoder::WriteCrc() {
  WriteMarker(Marker::TYPE_INT);

  auto const value = CrcAccValue();
  auto const wire = utils::HostToLittleEndian(static_cast<uint64_t>(value));
  Write(reinterpret_cast<const uint8_t *>(&wire), sizeof(wire));
  return value;
}

void BufferEncoder::WriteDouble(double value) {
  auto value_uint = std::bit_cast<uint64_t>(value);
  value_uint = utils::HostToLittleEndian(value_uint);
  WriteMarker(Marker::TYPE_DOUBLE);
  Write(reinterpret_cast<const uint8_t *>(&value_uint), sizeof(value_uint));
}

void BufferEncoder::WriteString(const std::string_view value) {
  WriteMarker(Marker::TYPE_STRING);
  WriteSize(value.size());
  Write(reinterpret_cast<const uint8_t *>(value.data()), value.size());
}

void BufferEncoder::WriteEnum(storage::Enum value) {
  WriteMarker(Marker::TYPE_ENUM);
  auto etype = utils::HostToLittleEndian(value.type_id().value_of());
  Write(reinterpret_cast<const uint8_t *>(&etype), sizeof(etype));
  auto evalue = utils::HostToLittleEndian(value.value_id().value_of());
  Write(reinterpret_cast<const uint8_t *>(&evalue), sizeof(evalue));
}

void BufferEncoder::WritePoint2d(storage::Point2d value) {
  WriteMarker(Marker::TYPE_POINT_2D);
  WriteUint(CrsToSrid(value.crs()).value_of());
  WriteDouble(value.x());
  WriteDouble(value.y());
}

void BufferEncoder::WritePoint3d(storage::Point3d value) {
  WriteMarker(Marker::TYPE_POINT_3D);
  WriteUint(CrsToSrid(value.crs()).value_of());
  WriteDouble(value.x());
  WriteDouble(value.y());
  WriteDouble(value.z());
}

void BufferEncoder::WriteExternalPropertyValue(const ExternalPropertyValue &value) {
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
      WriteUint(std::bit_cast<uint64_t>(value.ValueInt()));
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
      WriteSize(list.size());
      for (const auto &item : list) {
        WriteExternalPropertyValue(item);
      }
      break;
    }
    case ExternalPropertyValue::Type::NumericList: {
      const auto &list = value.ValueNumericList();
      WriteMarker(Marker::TYPE_LIST);
      WriteSize(list.size());
      for (const auto &item : list) {
        WriteMarker(Marker::TYPE_PROPERTY_VALUE);
        if (std::holds_alternative<int>(item)) {
          WriteUint(static_cast<uint64_t>(std::get<int>(item)));
        } else {
          WriteDouble(std::get<double>(item));
        }
      }
      break;
    }
    case ExternalPropertyValue::Type::IntList: {
      const auto &list = value.ValueIntList();
      WriteMarker(Marker::TYPE_LIST);
      WriteSize(list.size());
      for (const auto &item : list) {
        WriteMarker(Marker::TYPE_PROPERTY_VALUE);
        WriteUint(static_cast<uint64_t>(item));
      }
      break;
    }
    case ExternalPropertyValue::Type::DoubleList: {
      const auto &list = value.ValueDoubleList();
      WriteMarker(Marker::TYPE_LIST);
      WriteSize(list.size());
      for (const auto &item : list) {
        WriteMarker(Marker::TYPE_PROPERTY_VALUE);
        WriteDouble(item);
      }
      break;
    }
    case ExternalPropertyValue::Type::Map: {
      const auto &map = value.ValueMap();
      WriteMarker(Marker::TYPE_MAP);
      WriteSize(map.size());
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
      WriteUint(std::bit_cast<uint64_t>(temporal_data.microseconds));
      break;
    }
    case ExternalPropertyValue::Type::ZonedTemporalData: {
      const auto zoned_temporal_data = value.ValueZonedTemporalData();
      WriteMarker(Marker::TYPE_ZONED_TEMPORAL_DATA);
      WriteUint(static_cast<uint64_t>(zoned_temporal_data.type));
      WriteUint(std::bit_cast<uint64_t>(zoned_temporal_data.IntMicroseconds()));
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
    case ExternalPropertyValue::Type::VectorIndexId: {
      const auto &vector_index_ids = value.ValueVectorIndexIds();
      WriteMarker(Marker::TYPE_VECTOR_INDEX_ID);
      WriteSize(vector_index_ids.size());
      for (const auto &id : vector_index_ids) {
        WriteString(id);
      }
      const auto &list = value.ValueVectorIndexList();
      WriteSize(list.size());
      for (auto item : list) {
        WriteDouble(item);
      }
      break;
    }
  }
}

auto BufferEncoder::GetPosition() -> uint64_t { return buffer_.size(); }

auto BufferEncoder::charged_bytes() const -> uint64_t {
  // The vector holds exactly one allocation of capacity() elements, which is what the allocator charged.
  return policy_.budget != nullptr ? buffer_.capacity() : 0;
}

}  // namespace memgraph::storage::durability
