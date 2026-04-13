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

void WriteUint(uint64_t value, slk::Builder *builder) {
  WriteMarker(Marker::TYPE_INT, builder);
  slk::Save(value, builder);
}

void WriteString(std::string_view value, slk::Builder *builder) {
  WriteMarker(Marker::TYPE_STRING, builder);
  slk::Save(value, builder);
}

// ---- Reading ----

Marker ReadMarker(slk::Reader *reader) {
  Marker marker;
  slk::Load(&marker, reader);
  return marker;
}

std::optional<uint64_t> ReadUint(slk::Reader *reader) {
  if (ReadMarker(reader) != Marker::TYPE_INT) return std::nullopt;
  uint64_t value;
  slk::Load(&value, reader);
  return value;
}

std::optional<std::string> ReadString(slk::Reader *reader) {
  if (ReadMarker(reader) != Marker::TYPE_STRING) return std::nullopt;
  std::string value;
  slk::Load(&value, reader);
  return value;
}

}  // namespace memgraph::wire_format
