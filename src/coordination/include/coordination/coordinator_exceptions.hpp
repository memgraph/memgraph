// Copyright 2024 Memgraph Ltd.
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

#ifdef MG_ENTERPRISE

#include "utils/exceptions.hpp"

namespace memgraph::coordination {
class CoordinatorRegisterInstanceException final : public utils::BasicException {
 public:
  explicit CoordinatorRegisterInstanceException(const std::string_view what) noexcept
      : BasicException("Failed to create instance: " + std::string(what)) {}

  template <class... Args>
  explicit CoordinatorRegisterInstanceException(fmt::format_string<Args...> fmt, Args &&...args) noexcept
      : CoordinatorRegisterInstanceException(fmt::format(fmt, std::forward<Args>(args)...)) {}

  SPECIALIZE_GET_EXCEPTION_NAME(CoordinatorRegisterInstanceException)
};

class RaftServerStartException final : public utils::BasicException {
 public:
  explicit RaftServerStartException(std::string_view what) noexcept : BasicException(what) {}

  template <class... Args>
  explicit RaftServerStartException(fmt::format_string<Args...> fmt, Args &&...args) noexcept
      : RaftServerStartException(fmt::format(fmt, std::forward<Args>(args)...)) {}

  SPECIALIZE_GET_EXCEPTION_NAME(RaftServerStartException)
};

class RaftAddServerException final : public utils::BasicException {
 public:
  explicit RaftAddServerException(std::string_view what) noexcept : BasicException(what) {}

  template <class... Args>
  explicit RaftAddServerException(fmt::format_string<Args...> fmt, Args &&...args) noexcept
      : RaftAddServerException(fmt::format(fmt, std::forward<Args>(args)...)) {}

  SPECIALIZE_GET_EXCEPTION_NAME(RaftAddServerException)
};

}  // namespace memgraph::coordination
#endif
