// Copyright 2021 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <type_traits>

#include "communication/bolt/v1/codes.hpp"
#include "communication/bolt/v1/value.hpp"
#include "utils/cast.hpp"
#include "utils/endian.hpp"

static_assert(std::is_same_v<std::uint8_t, char> || std::is_same_v<std::uint8_t, unsigned char>,
              "communication::bolt::Encoder requires uint8_t to be "
              "implemented as char or unsigned char.");

namespace communication::bolt {

/**
 * Bolt BaseEncoder. Has public interfaces for writing Bolt encoded data.
 * Supported types are: Null, Bool, Int, Double, String, List, Map, Vertex,
 * Edge, Date, LocalDate, LocalDateTime, Duration.
 *
 * The purpose of this class is to stream bolt data into the given Buffer.
 *
 * @tparam Buffer the output buffer that should be used
 */
template <typename Buffer>
class BaseEncoder {
 public:
  explicit BaseEncoder(Buffer &buffer) : buffer_(buffer) {}

  void WriteRAW(const uint8_t *data, uint64_t len) { buffer_.Write(data, len); }

  void WriteRAW(const char *data, uint64_t len) { WriteRAW((const uint8_t *)data, len); }

  void WriteRAW(const uint8_t data) { WriteRAW(&data, 1); }

  void WriteNull() { WriteRAW(utils::UnderlyingCast(Marker::Null)); }

  void WriteBool(const bool &value) {
    if (value)
      WriteRAW(utils::UnderlyingCast(Marker::True));
    else
      WriteRAW(utils::UnderlyingCast(Marker::False));
  }

  void WriteInt(const int64_t &value) {
    if (value >= -16L && value < 128L) {
      WriteRAW(static_cast<uint8_t>(value));
    } else if (value >= -128L && value < -16L) {
      WriteRAW(utils::UnderlyingCast(Marker::Int8));
      WriteRAW(static_cast<uint8_t>(value));
    } else if (value >= -32768L && value < 32768L) {
      WriteRAW(utils::UnderlyingCast(Marker::Int16));
      WritePrimitiveValue(static_cast<int16_t>(value));
    } else if (value >= -2147483648L && value < 2147483648L) {
      WriteRAW(utils::UnderlyingCast(Marker::Int32));
      WritePrimitiveValue(static_cast<int32_t>(value));
    } else {
      WriteRAW(utils::UnderlyingCast(Marker::Int64));
      WritePrimitiveValue(value);
    }
  }

  void WriteDouble(const double &value) {
    WriteRAW(utils::UnderlyingCast(Marker::Float64));
    uint64_t tmp = utils::MemcpyCast<uint64_t>(value);
    WritePrimitiveValue(tmp);
  }

  void WriteTypeSize(const size_t size, const uint8_t typ) {
    if (size <= 15) {
      uint8_t len = size;
      len &= 0x0F;
      WriteRAW(utils::UnderlyingCast(MarkerTiny[typ]) + len);
    } else if (size <= 255) {
      uint8_t len = size;
      WriteRAW(utils::UnderlyingCast(Marker8[typ]));
      WriteRAW(len);
    } else if (size <= 65535) {
      uint16_t len = size;
      WriteRAW(utils::UnderlyingCast(Marker16[typ]));
      WritePrimitiveValue(len);
    } else {
      uint32_t len = size;
      WriteRAW(utils::UnderlyingCast(Marker32[typ]));
      WritePrimitiveValue(len);
    }
  }

  void WriteString(const std::string &value) {
    WriteTypeSize(value.size(), MarkerString);
    WriteRAW(value.c_str(), value.size());
  }

  void WriteList(const std::vector<Value> &value) {
    WriteTypeSize(value.size(), MarkerList);
    for (auto &x : value) WriteValue(x);
  }

  void WriteMap(const std::map<std::string, Value> &value) {
    WriteTypeSize(value.size(), MarkerMap);
    for (auto &x : value) {
      WriteString(x.first);
      WriteValue(x.second);
    }
  }

  void WriteVertex(const Vertex &vertex) {
    WriteRAW(utils::UnderlyingCast(Marker::TinyStruct) + 3);
    WriteRAW(utils::UnderlyingCast(Signature::Node));
    WriteInt(vertex.id.AsInt());

    // write labels
    const auto &labels = vertex.labels;
    WriteTypeSize(labels.size(), MarkerList);
    for (const auto &label : labels) WriteString(label);

    // write properties
    const auto &props = vertex.properties;
    WriteTypeSize(props.size(), MarkerMap);
    for (const auto &prop : props) {
      WriteString(prop.first);
      WriteValue(prop.second);
    }
  }

  void WriteEdge(const Edge &edge, bool unbound = false) {
    WriteRAW(utils::UnderlyingCast(Marker::TinyStruct) + (unbound ? 3 : 5));
    WriteRAW(utils::UnderlyingCast(unbound ? Signature::UnboundRelationship : Signature::Relationship));

    WriteInt(edge.id.AsInt());
    if (!unbound) {
      WriteInt(edge.from.AsInt());
      WriteInt(edge.to.AsInt());
    }

    WriteString(edge.type);

    const auto &props = edge.properties;
    WriteTypeSize(props.size(), MarkerMap);
    for (const auto &prop : props) {
      WriteString(prop.first);
      WriteValue(prop.second);
    }
  }

  void WriteEdge(const UnboundedEdge &edge) {
    WriteRAW(utils::UnderlyingCast(Marker::TinyStruct) + 3);
    WriteRAW(utils::UnderlyingCast(Signature::UnboundRelationship));

    WriteInt(edge.id.AsInt());

    WriteString(edge.type);

    const auto &props = edge.properties;
    WriteTypeSize(props.size(), MarkerMap);
    for (const auto &prop : props) {
      WriteString(prop.first);
      WriteValue(prop.second);
    }
  }

  void WritePath(const Path &path) {
    WriteRAW(utils::UnderlyingCast(Marker::TinyStruct) + 3);
    WriteRAW(utils::UnderlyingCast(Signature::Path));
    WriteTypeSize(path.vertices.size(), MarkerList);
    for (auto &v : path.vertices) WriteVertex(v);
    WriteTypeSize(path.edges.size(), MarkerList);
    for (auto &e : path.edges) WriteEdge(e);
    WriteTypeSize(path.indices.size(), MarkerList);
    for (auto &i : path.indices) WriteInt(i);
  }

  void WriteDate(const utils::Date &date) {
    WriteRAW(utils::UnderlyingCast(Marker::TinyStruct1));
    WriteRAW(utils::UnderlyingCast(Signature::Date));
    WriteInt(date.DaysSinceEpoch());
  }

  void WriteLocalTime(const utils::LocalTime &local_time) {
    WriteRAW(utils::UnderlyingCast(Marker::TinyStruct1));
    WriteRAW(utils::UnderlyingCast(Signature::LocalTime));
    WriteInt(local_time.NanosecondsSinceEpoch());
  }

  void WriteLocalDateTime(const utils::LocalDateTime &local_date_time) {
    WriteRAW(utils::UnderlyingCast(Marker::TinyStruct2));
    WriteRAW(utils::UnderlyingCast(Signature::LocalDateTime));
    WriteInt(local_date_time.SecondsSinceEpoch());
    WriteInt(local_date_time.SubSecondsAsNanoseconds());
  }

  void WriteDuration(const utils::Duration &duration) {
    WriteRAW(utils::UnderlyingCast(Marker::TinyStruct4));
    WriteRAW(utils::UnderlyingCast(Signature::Duration));
    // This shall always be zero because internally we store microseconds
    // and converting months to microseconds is an approximation. However,
    // for the encoder, we implement ReadInt() to support the neo4j driver.
    WriteInt(0);
    WriteInt(duration.Days());
    WriteInt(duration.SubDaysAsSeconds());
    WriteInt(duration.SubSecondsAsNanoseconds());
  }

  void WriteValue(const Value &value) {
    switch (value.type()) {
      case Value::Type::Null:
        WriteNull();
        break;
      case Value::Type::Bool:
        WriteBool(value.ValueBool());
        break;
      case Value::Type::Int:
        WriteInt(value.ValueInt());
        break;
      case Value::Type::Double:
        WriteDouble(value.ValueDouble());
        break;
      case Value::Type::String:
        WriteString(value.ValueString());
        break;
      case Value::Type::List:
        WriteList(value.ValueList());
        break;
      case Value::Type::Map:
        WriteMap(value.ValueMap());
        break;
      case Value::Type::Vertex:
        WriteVertex(value.ValueVertex());
        break;
      case Value::Type::Edge:
        WriteEdge(value.ValueEdge());
        break;
      case Value::Type::UnboundedEdge:
        WriteEdge(value.ValueUnboundedEdge());
        break;
      case Value::Type::Path:
        WritePath(value.ValuePath());
        break;
      case Value::Type::Date:
        WriteDate(value.ValueDate());
        break;
      case Value::Type::LocalTime:
        WriteLocalTime(value.ValueLocalTime());
        break;
      case Value::Type::LocalDateTime:
        WriteLocalDateTime(value.ValueLocalDateTime());
        break;
      case Value::Type::Duration:
        WriteDuration(value.ValueDuration());
        break;
    }
  }

 protected:
  Buffer &buffer_;

 private:
  template <class T>
  void WritePrimitiveValue(T value) {
    value = utils::HostToBigEndian(value);
    WriteRAW(reinterpret_cast<const uint8_t *>(&value), sizeof(value));
  }
};

}  // namespace communication::bolt
