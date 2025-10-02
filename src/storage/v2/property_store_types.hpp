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

#pragma once

#include <cstdint>

namespace memgraph::storage {
// All of these values must have the lowest 4 bits set to zero because they are
// used to store property ID size (2 bits) and payload size OR size of payload size indicator (2 bits).
enum class PropertyStoreType : uint8_t {
  EMPTY = 0x00,  // Special value used to indicate end of buffer.
  NONE = 0x10,   // NONE used instead of NULL because NULL is defined to
                 // something...
  BOOL = 0x20,
  INT = 0x30,
  DOUBLE = 0x40,
  STRING = 0x50,
  LIST = 0x60,
  MAP = 0x70,
  TEMPORAL_DATA = 0x80,
  ZONED_TEMPORAL_DATA = 0x90,
  OFFSET_ZONED_TEMPORAL_DATA = 0xA0,
  ENUM = 0xB0,
  POINT = 0xC0,
  VECTOR = 0xD0,  // Indicates that list is stored in the vector index
};
}  // namespace memgraph::storage
