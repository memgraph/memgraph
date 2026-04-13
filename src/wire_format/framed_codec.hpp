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

/// Marker-framed codec for basic types over SLK streams.
///
/// The wire format is: one Marker byte identifying the type, followed
/// by the SLK-encoded payload. This is the same framing used by the
/// durability (snapshot/WAL) and replication layers.
///
/// This library provides read/write helpers for primitive types
/// (bool, uint64, double, string) so that code outside mg-storage-v2
/// can participate in the framed protocol without depending on the
/// full storage serialisation hierarchy.

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "storage/v2/durability/marker.hpp"

namespace memgraph::slk {
class Reader;
class Builder;
}  // namespace memgraph::slk

namespace memgraph::wire_format {

using Marker = storage::durability::Marker;

// ---- Writing (marker + slk payload) ----

void WriteMarker(Marker marker, slk::Builder *builder);
void WriteBool(bool value, slk::Builder *builder);
void WriteUint(uint64_t value, slk::Builder *builder);
void WriteDouble(double value, slk::Builder *builder);
void WriteString(std::string_view value, slk::Builder *builder);

// ---- Reading (marker + slk payload) ----

std::optional<Marker> ReadMarker(slk::Reader *reader);
std::optional<bool> ReadBool(slk::Reader *reader);
std::optional<uint64_t> ReadUint(slk::Reader *reader);
std::optional<double> ReadDouble(slk::Reader *reader);
std::optional<std::string> ReadString(slk::Reader *reader);

}  // namespace memgraph::wire_format
