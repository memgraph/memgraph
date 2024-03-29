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

#include <execinfo.h>
#include <spdlog/spdlog.h>
#include <iostream>

#include "utils/stacktrace.hpp"

namespace memgraph::utils {

/**
 * Dump stacktrace to the stream and abort the probram. For more details
 * about the abort please take a look at
 * http://en.cppreference.com/w/cpp/utility/program/abort.
 */
inline void TerminateHandler(std::ostream &stream) noexcept {
  if (auto exc = std::current_exception()) {
    try {
      std::rethrow_exception(exc);
    } catch (std::exception &ex) {
      stream << ex.what() << std::endl << std::endl;
      utils::Stacktrace stacktrace;
      stacktrace.dump(stream);
    }
  }

  // Flush all the logs
  spdlog::shutdown();
  std::abort();
}

inline void TerminateHandler() noexcept { TerminateHandler(std::cout); }

}  // namespace memgraph::utils
