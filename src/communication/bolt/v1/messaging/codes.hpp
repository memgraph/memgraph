#pragma once

#include "utils/types/byte.hpp"
#include "utils/underlying_cast.hpp"

namespace communication::bolt {

enum class MessageCode : byte {
  Init = 0x01,
  AckFailure = 0x0E,
  Reset = 0x0F,

  Run = 0x10,
  DiscardAll = 0x2F,
  PullAll = 0x3F,

  Record = 0x71,
  Success = 0x70,
  Ignored = 0x7E,
  Failure = 0x7F
};

inline bool operator==(byte value, MessageCode code) {
  return value == underlying_cast(code);
}

inline bool operator==(MessageCode code, byte value) {
  return operator==(value, code);
}

inline bool operator!=(byte value, MessageCode code) {
  return !operator==(value, code);
}

inline bool operator!=(MessageCode code, byte value) {
  return operator!=(value, code);
}
}
