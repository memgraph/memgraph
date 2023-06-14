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

#pragma once

#include <cstdint>

namespace memgraph::communication::bolt {

inline constexpr uint8_t kPreamble[4] = {0x60, 0x60, 0xB0, 0x17};

enum class Signature : uint8_t {
  Noop = 0x00,
  Init = 0x01,
  LogOn = 0x6A,
  LogOff = 0x6B,
  AckFailure = 0x0E,  // only v1
  Reset = 0x0F,
  Goodbye = 0x02,

  Run = 0x10,
  Discard = 0x2F,
  Pull = 0x3F,
  Begin = 0x11,
  Commit = 0x12,
  Rollback = 0x13,
  Route = 0x66,

  Record = 0x71,
  Success = 0x70,
  Ignored = 0x7E,
  Failure = 0x7F,

  Node = 0x4E,
  Relationship = 0x52,
  Path = 0x50,
  UnboundRelationship = 0x72,

  /// Temporal data types
  Date = 0x44,
  Duration = 0x45,
  LocalDateTime = 0x64,
  LocalTime = 0x74,
};

enum class Marker : uint8_t {
  TinyString = 0x80,
  TinyList = 0x90,
  TinyMap = 0xA0,
  TinyStruct = 0xB0,

  // TinyStructX represents the value of TinyStruct + X
  // This is defined to make decoding easier. To check if a marker is equal
  // to TinyStruct + 1 you should use something like:
  //   underlying_cast(marker) == underlying_cast(Marker::TinyStruct) + 1
  // This way you can just use:
  //   marker == Marker::TinyStruct1
  TinyStruct1 = 0xB1,
  TinyStruct2 = 0xB2,
  TinyStruct3 = 0xB3,
  TinyStruct4 = 0xB4,
  TinyStruct5 = 0xB5,

  Null = 0xC0,
  Float64 = 0xC1,

  False = 0xC2,
  True = 0xC3,

  Int8 = 0xC8,
  Int16 = 0xC9,
  Int32 = 0xCA,
  Int64 = 0xCB,

  String8 = 0xD0,
  String16 = 0xD1,
  String32 = 0xD2,

  List8 = 0xD4,
  List16 = 0xD5,
  List32 = 0xD6,

  Map8 = 0xD8,
  Map16 = 0xD9,
  Map32 = 0xDA,

  Struct8 = 0xDC,
  Struct16 = 0xDD,
};

inline constexpr uint8_t MarkerString = 0, MarkerList = 1, MarkerMap = 2;
inline constexpr Marker MarkerTiny[3] = {Marker::TinyString, Marker::TinyList, Marker::TinyMap};
inline constexpr Marker Marker8[3] = {Marker::String8, Marker::List8, Marker::Map8};
inline constexpr Marker Marker16[3] = {Marker::String16, Marker::List16, Marker::Map16};
inline constexpr Marker Marker32[3] = {Marker::String32, Marker::List32, Marker::Map32};
}  // namespace memgraph::communication::bolt
