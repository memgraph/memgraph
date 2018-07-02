#pragma once

#include <cstdint>

namespace communication::bolt {

static constexpr uint8_t kPreamble[4] = {0x60, 0x60, 0xB0, 0x17};
static constexpr uint8_t kProtocol[4] = {0x00, 0x00, 0x00, 0x01};

enum class Signature : uint8_t {
  Init = 0x01,
  AckFailure = 0x0E,
  Reset = 0x0F,

  Run = 0x10,
  DiscardAll = 0x2F,
  PullAll = 0x3F,

  Record = 0x71,
  Success = 0x70,
  Ignored = 0x7E,
  Failure = 0x7F,

  Node = 0x4E,
  Relationship = 0x52,
  Path = 0x50,
  UnboundRelationship = 0x72,
};

enum class Marker : uint8_t {
  TinyString = 0x80,
  TinyList = 0x90,
  TinyMap = 0xA0,
  TinyStruct = 0xB0,

  // TinyStructX represents the value of TinyStruct + X
  // This is defined to make decoding easier. To check if a marker is equal
  // to TinyStruct + 1 you should use something like:
  //   underyling_cast(marker) == underyling_cast(Marker::TinyStruct) + 1
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

static constexpr uint8_t MarkerString = 0, MarkerList = 1, MarkerMap = 2;
static constexpr Marker MarkerTiny[3] = {Marker::TinyString, Marker::TinyList,
                                         Marker::TinyMap};
static constexpr Marker Marker8[3] = {Marker::String8, Marker::List8,
                                      Marker::Map8};
static constexpr Marker Marker16[3] = {Marker::String16, Marker::List16,
                                       Marker::Map16};
static constexpr Marker Marker32[3] = {Marker::String32, Marker::List32,
                                       Marker::Map32};
}  // namespace communication::bolt
