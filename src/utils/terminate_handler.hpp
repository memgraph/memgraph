// Copyright 2026 Memgraph Ltd.
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
#include <unistd.h>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <iosfwd>
#include <iostream>

#include "utils/build_info.hpp"
#include "utils/stacktrace.hpp"

namespace memgraph::utils {

/// Exit status a shell reports for a process killed by SIGABRT.
inline constexpr int kAbortExitStatus = 128 + SIGABRT;

/// Abort the process. As PID 1 of a PID namespace (a container entrypoint) the kernel discards the SIGABRT that
/// std::abort() raises against itself, and glibc then falls through to a trap instruction, so the process dies with
/// an arch-dependent SIGTRAP (status 133 on aarch64) instead of SIGABRT. Exit with the SIGABRT status directly there.
[[noreturn]] inline void AbortProcess(bool const is_init) noexcept {
  std::cerr.flush();
  std::fflush(nullptr);
  if (is_init) {
    ::_exit(kAbortExitStatus);
  }
  std::abort();
}

/**
 * Dump stacktrace to the stream and abort the program. For more details
 * about the abort please take a look at
 * http://en.cppreference.com/w/cpp/utility/program/abort.
 */
inline void TerminateHandler(std::ostream &stream) noexcept {
  const auto build_info = GetBuildInfo();
  stream << "Memgraph " << build_info.version
         << " (build-id: " << (build_info.build_id.empty() ? "unknown" : build_info.build_id)
         << ", build-type: " << build_info.build_name << ")\n";
  if (auto exc = std::current_exception()) {
    try {
      std::rethrow_exception(exc);
    } catch (std::exception &ex) {
      stream << ex.what() << "\n\n";
      utils::Stacktrace stacktrace;
      stacktrace.dump(stream);
    }
  }
  stream.flush();
  AbortProcess(::getpid() == 1);
}

inline void TerminateHandler() noexcept { TerminateHandler(std::cerr); }

}  // namespace memgraph::utils
