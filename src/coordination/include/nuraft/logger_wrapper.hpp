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

#include "nuraft/log_level.hpp"
#include "nuraft/logger.hpp"

#include <source_location>
#include <string>

namespace memgraph::coordination {

class LoggerWrapper {
 public:
  explicit LoggerWrapper(Logger *logger);

  void Log(nuraft_log_level level, std::string const &log_line,
           std::source_location location = std::source_location::current());

 private:
  Logger *logger_;
};

static_assert(std::is_trivially_copyable_v<LoggerWrapper>, "LoggerWrapper must be trivially copyable");

}  // namespace memgraph::coordination

#endif
