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
#include <experimental/source_location>
#include <string>
#include <string_view>
#include <type_traits>

#include "common/errors.hpp"
#include "utils/result.hpp"

namespace memgraph::storage::v3 {

static_assert(std::is_same_v<uint8_t, unsigned char>);

struct ShardError {
  ShardError(common::ErrorCode code, std::string message, const std::experimental::source_location location)
      : code{code}, message{std::move(message)}, source{fmt::format("{}:{}", location.file_name(), location.line())} {}

  ShardError(common::ErrorCode code, const std::experimental::source_location location)
      : code{code}, source{fmt::format("{}:{}", location.file_name(), location.line())} {}

  common::ErrorCode code;
  std::string message;
  std::string source;

  inline friend bool operator==(const ShardError &lhs, const ShardError &rhs) { return lhs.code == rhs.code; }

  inline friend bool operator==(const ShardError &lhs, const common::ErrorCode rhs) { return lhs.code == rhs; }
};

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define SHARD_ERROR(error, ...)                                                                                        \
  ({                                                                                                                   \
    using ErrorCode = memgraph::common::ErrorCode;                                                                     \
    memgraph::storage::v3::ShardError(error, GET_MESSAGE(__VA_ARGS__), std::experimental::source_location::current()); \
  })

template <class TValue>
using ShardResult = utils::BasicResult<ShardError, TValue>;

}  // namespace memgraph::storage::v3
