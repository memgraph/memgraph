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

#undef SPDLOG_ACTIVE_LEVEL
#ifndef NDEBUG
#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_TRACE
#else
#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_OFF
#endif
#include <array>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>

#include <fmt/format.h>
// NOTE: fmt 9+ introduced fmt/std.h, it's important because of, e.g., std::path formatting. toolchain-v4 has fmt 8,
// the guard is here because of fmt 8 compatibility.
#if FMT_VERSION > 90000
#include <fmt/std.h>
#endif
#include <spdlog/fmt/ostr.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <boost/preprocessor/comparison/equal.hpp>
#include <boost/preprocessor/control/if.hpp>
#include <boost/preprocessor/variadic/size.hpp>

namespace memgraph::logging {

// TODO (antonio2368): Replace with std::source_location when it's supported by
// compilers
[[noreturn]] inline void AssertFailed(const char *file_name, int line_num, const char *expr,
                                      const std::string &message) {
  spdlog::critical(
      "\nAssertion failed in file {} at line {}."
      "\n\tExpression: '{}'"
      "{}",
      file_name, line_num, expr, !message.empty() ? fmt::format("\n\tMessage: '{}'", message) : "");
  std::terminate();
}

#define GET_MESSAGE(...) \
  BOOST_PP_IF(BOOST_PP_EQUAL(BOOST_PP_VARIADIC_SIZE(__VA_ARGS__), 0), "", fmt::format(__VA_ARGS__))

#define MG_ASSERT(expr, ...)                                                                  \
  do {                                                                                        \
    if (expr) [[likely]] {                                                                    \
      (void)0;                                                                                \
    } else {                                                                                  \
      ::memgraph::logging::AssertFailed(__FILE__, __LINE__, #expr, GET_MESSAGE(__VA_ARGS__)); \
    }                                                                                         \
  } while (false)

#ifndef NDEBUG
#define DMG_ASSERT(expr, ...) MG_ASSERT(expr, __VA_ARGS__)
#else
#define DMG_ASSERT(...) \
  do {                  \
  } while (false)
#endif

template <typename... Args>
void Fatal(const char *msg, const Args &...msg_args) {
  spdlog::critical(msg, msg_args...);
  std::terminate();
}

#define LOG_FATAL(...)             \
  do {                             \
    spdlog::critical(__VA_ARGS__); \
    std::terminate();              \
  } while (0)

#ifndef NDEBUG
#define DLOG_FATAL(...) LOG_FATAL(__VA_ARGS__)
#else
#define DLOG_FATAL(...) \
  do {                  \
  } while (false)
#endif

inline void RedirectToStderr() { spdlog::set_default_logger(spdlog::stderr_color_mt("stderr")); }

// /// Use it for operations that must successfully finish.
inline void AssertRocksDBStatus(const auto &status) { MG_ASSERT(status.ok(), "rocksdb: {}", status.ToString()); }

inline bool CheckRocksDBStatus(const auto &status) {
  if (!status.ok()) [[unlikely]] {
    spdlog::error("rocksdb: {}", status.ToString());
  }
  return status.ok();
}

std::string MaskSensitiveInformation(const std::string &input);
}  // namespace memgraph::logging
