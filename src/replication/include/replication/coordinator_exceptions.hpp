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

namespace memgraph::replication {
class CoordinatorFailoverException final : public utils::BasicException {
 public:
  explicit CoordinatorFailoverException(const std::string_view what) noexcept
      : BasicException("Failover didn't complete successfully: " + std::string(what)) {}

  template <class... Args>
  explicit CoordinatorFailoverException(fmt::format_string<Args...> fmt, Args &&...args) noexcept
      : CoordinatorFailoverException(fmt::format(fmt, std::forward<Args>(args)...)) {}

  SPECIALIZE_GET_EXCEPTION_NAME(CoordinatorFailoverException)
};

}  // namespace memgraph::replication
#endif
