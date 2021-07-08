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

#include <spdlog/fmt/ostr.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include "utils/likely.hpp"

namespace logging {
#ifndef NDEBUG
// TODO (antonio2368): Replace with std::source_location when it's supported by
// compilers
template <typename... Args>
void AssertFailed(const char *file_name, int line_num, const char *expr, const Args &...msg_args) {
  std::optional<std::string> message;
  if constexpr (sizeof...(msg_args) > 0) {
    message.emplace(fmt::format(msg_args...));
  }

  spdlog::critical(
      "\nAssertion failed in file {} at line {}."
      "\n\tExpression: '{}'"
      "{}",
      file_name, line_num, expr, message ? fmt::format("\n\tMessage: '{}'", *message) : "");
  std::terminate();
}

// TODO (antonio2368): Replace with attribute [[likely]] when it's supported by
// compilers
#define MG_ASSERT(expr, ...) \
  LIKELY(!!(expr))           \
  ? (void)0 : ::logging::AssertFailed(__FILE__, __LINE__, #expr, ##__VA_ARGS__)
#define DMG_ASSERT(expr, ...) MG_ASSERT(expr, __VA_ARGS__)
#else
template <typename... Args>
void AssertFailed(const Args &...msg_args) {
  if constexpr (sizeof...(msg_args) > 0) {
    spdlog::critical("Assertion failed with message: '{}'", fmt::format(msg_args...).c_str());
  } else {
    spdlog::critical("Assertion failed");
  }
  std::terminate();
}

#define MG_ASSERT(expr, ...) LIKELY(!!(expr)) ? (void)0 : ::logging::AssertFailed(__VA_ARGS__)
#define DMG_ASSERT(...)
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
#define DLOG_FATAL(...)
#endif

inline void RedirectToStderr() { spdlog::set_default_logger(spdlog::stderr_color_mt("stderr")); }
}  // namespace logging
