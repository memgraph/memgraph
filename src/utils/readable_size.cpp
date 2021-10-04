// Copyright 2021 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "utils/readable_size.hpp"

#include <array>

#include <fmt/format.h>

namespace utils {

std::string GetReadableSize(double size) {
  // TODO (antonio2368): Add support for base 1000 (KB, GB, TB...)
  constexpr std::array units = {"B", "KiB", "MiB", "GiB", "TiB"};
  constexpr double delimiter = 1024;

  size_t i = 0;
  for (; i + 1 < units.size() && size >= delimiter; ++i) {
    size /= delimiter;
  }

  // bytes don't need decimals
  if (i == 0) {
    return fmt::format("{:.0f}{}", size, units[i]);
  }

  return fmt::format("{:.2f}{}", size, units[i]);
}

}  // namespace utils
