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

#include <filesystem>
#include <random>
#include <sstream>
#include <string_view>

namespace tests {

auto generate_unique_temp_directory(std::string_view prefix) -> std::filesystem::path {
  namespace fs = std::filesystem;
  std::random_device rd;
  std::mt19937_64 gen(rd());
  std::uniform_int_distribution<uint64_t> dis;

  std::stringstream ss;
  ss << prefix << "_" << std::hex << dis(gen);  // e.g., testSuite_5f4dcc3b
  return fs::temp_directory_path() / ss.str();
}
}  // namespace tests
