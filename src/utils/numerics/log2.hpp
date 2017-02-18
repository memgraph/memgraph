#pragma once

#include <cstddef>

namespace utils {

constexpr size_t log2(size_t n) { return ((n < 2) ? 0 : 1 + log2(n >> 1)); }
}
