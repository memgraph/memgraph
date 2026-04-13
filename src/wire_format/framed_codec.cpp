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

#include "wire_format/framed_codec.hpp"

#include "slk/serialization.hpp"
#include "slk/streams.hpp"

namespace memgraph::wire_format {

// ---- Writing ----

void WriteMarker(Marker marker, slk::Builder *builder) { slk::Save(marker, builder); }

void WriteBool(bool value, slk::Builder *builder) {
  WriteMarker(Marker::TYPE_BOOL, builder);
  slk::Save(value, builder);
}

void WriteUint(uint64_t value, slk::Builder *builder) {
  WriteMarker(Marker::TYPE_INT, builder);
  slk::Save(value, builder);
}

void WriteDouble(double value, slk::Builder *builder) {
  WriteMarker(Marker::TYPE_DOUBLE, builder);
  slk::Save(value, builder);
}

void WriteString(std::string_view value, slk::Builder *builder) {
  WriteMarker(Marker::TYPE_STRING, builder);
  slk::Save(value, builder);
}

// ---- Reading ----

std::optional<Marker> ReadMarker(slk::Reader *reader) {
  Marker marker;
  slk::Load(&marker, reader);
  return marker;
}

std::optional<bool> ReadBool(slk::Reader *reader) {
  if (auto marker = ReadMarker(reader); !marker || *marker != Marker::TYPE_BOOL) return std::nullopt;
  bool value;
  slk::Load(&value, reader);
  return value;
}

std::optional<uint64_t> ReadUint(slk::Reader *reader) {
  if (auto marker = ReadMarker(reader); !marker || *marker != Marker::TYPE_INT) return std::nullopt;
  uint64_t value;
  slk::Load(&value, reader);
  return value;
}

std::optional<double> ReadDouble(slk::Reader *reader) {
  if (auto marker = ReadMarker(reader); !marker || *marker != Marker::TYPE_DOUBLE) return std::nullopt;
  double value;
  slk::Load(&value, reader);
  return value;
}

std::optional<std::string> ReadString(slk::Reader *reader) {
  if (auto marker = ReadMarker(reader); !marker || *marker != Marker::TYPE_STRING) return std::nullopt;
  std::string value;
  slk::Load(&value, reader);
  return std::move(value);
}

}  // namespace memgraph::wire_format
