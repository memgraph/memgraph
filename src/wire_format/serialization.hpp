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

#pragma once

/// Inline read/write helpers for marker-framed SLK streams.
///
/// Wire format: one Marker byte identifying the type, followed by
/// the SLK-encoded payload. This is the same framing used by
/// durability (snapshot/WAL) and replication (RPC file transfer).
///
/// Error model: functions throw slk::SlkReaderException on truncated
/// input. Read functions return std::nullopt when the marker byte
/// is present but indicates a different type than expected (the
/// marker byte is consumed either way).

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "slk/serialization.hpp"
#include "slk/streams.hpp"
#include "wire_format/marker.hpp"

namespace memgraph::wire_format {

// ---- Writing (marker + slk payload) ----

inline void WriteMarker(Marker marker, slk::Builder *builder) { slk::Save(marker, builder); }

inline void WriteUint(uint64_t value, slk::Builder *builder) {
  WriteMarker(Marker::TYPE_INT, builder);
  slk::Save(value, builder);
}

inline void WriteString(std::string_view value, slk::Builder *builder) {
  WriteMarker(Marker::TYPE_STRING, builder);
  slk::Save(value, builder);
}

// ---- Reading (marker + slk payload) ----

inline Marker ReadMarker(slk::Reader *reader) {
  Marker marker;
  slk::Load(&marker, reader);
  return marker;
}

inline std::optional<uint64_t> ReadUint(slk::Reader *reader) {
  if (ReadMarker(reader) != Marker::TYPE_INT) return std::nullopt;
  uint64_t value;
  slk::Load(&value, reader);
  return value;
}

inline std::optional<std::string> ReadString(slk::Reader *reader) {
  if (ReadMarker(reader) != Marker::TYPE_STRING) return std::nullopt;
  std::string value;
  slk::Load(&value, reader);
  return value;
}

}  // namespace memgraph::wire_format
