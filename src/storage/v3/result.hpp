// Copyright 2022 Memgraph Ltd.
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
#include <string>
#include <string_view>
#include <type_traits>

#include "common/errors.hpp"
#include "utils/result.hpp"

namespace memgraph::storage::v3 {

static_assert(std::is_same_v<uint8_t, unsigned char>);

struct ShardError {
  ShardError(common::ErrorCode code, std::string message, std::string source)
      : code{code}, message{std::move(message)}, source{std::move(source)} {}

  ShardError(common::ErrorCode code, std::string source) : code{code}, source{std::move(source)} {}

  common::ErrorCode code;
  // TODO Maybe add category
  std::string message;
  std::string source;

  inline friend bool operator==(const ShardError &lhs, const ShardError &rhs) { return lhs.code == rhs.code; }
};

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define SHARD_ERROR(...) memgraph::storage::v3::ShardError(__VA_ARGS__, fmt::format("{}:{}", __FILE__, __LINE__))

template <class TValue>
using ShardResult = utils::BasicResult<ShardError, TValue>;

}  // namespace memgraph::storage::v3
