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

#ifdef MG_ENTERPRISE

#include "coordination/logger_wrapper.hpp"

namespace memgraph::coordination {

LoggerWrapper::LoggerWrapper(Logger *logger) : logger_(logger) {}

void LoggerWrapper::Log(nuraft_log_level level, std::string const &log_line, std::source_location location) const {
  logger_->put_details(static_cast<int>(level), location.file_name(), location.function_name(), location.line(),
                       log_line);
}

}  // namespace memgraph::coordination

#endif
