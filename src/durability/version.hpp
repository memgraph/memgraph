#pragma once

#include <array>
#include <cstdint>

namespace durability {

constexpr std::array<uint8_t, 4> kMagicNumber{{'M', 'G', 's', 'n'}};

// The current default version of snapshot and WAL enconding / decoding.
constexpr int64_t kVersion{2};
}
