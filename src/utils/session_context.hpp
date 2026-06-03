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

#include <fmt/format.h>
#include <spdlog/common.h>
#include <spdlog/spdlog.h>
#include <cstdint>
#include <iterator>
#include <string>
#include <string_view>
#include <utility>

namespace memgraph::logging {

// Per-session log state. Only the bolt dispatcher thread touches this; the TLS
// guard (ScopedSessionLog) is installed there and workers never inherit it.
class SessionLogContext {
 public:
  // Toggled by SET SESSION TRACE; events emit at INFO.
  void SetTrace(bool on) noexcept { trace_enabled_ = on; }

  bool trace_enabled() const noexcept { return trace_enabled_; }

  void SetSessionUuid(std::string uuid) { session_uuid_ = std::move(uuid); }

  void SetUser(std::string user) { user_ = std::move(user); }

  void ClearUser() { user_.clear(); }

  void SetTxId(uint64_t tx_id) noexcept { tx_id_ = tx_id; }

  void ClearTxId() noexcept { tx_id_ = 0; }

  std::string_view session_uuid() const noexcept { return session_uuid_; }

  void AppendTraceTags(fmt::memory_buffer &out) const {
    fmt::format_to(std::back_inserter(out), "[session={}]", session_uuid_);
    if (!user_.empty()) fmt::format_to(std::back_inserter(out), " [user={}]", user_);
    fmt::format_to(std::back_inserter(out), " [tx={}]", tx_id_);
  }

 private:
  std::string session_uuid_;
  std::string user_;
  uint64_t tx_id_ = 0;
  bool trace_enabled_ = false;
};

namespace detail {
inline thread_local SessionLogContext *current_session_log = nullptr;

// Cheap-first gate: TLS → toggle → spdlog level. Returns ctx if a trace would emit.
inline SessionLogContext *ActiveTraceContext() noexcept {
  auto *ctx = current_session_log;
  if (ctx == nullptr || !ctx->trace_enabled()) return nullptr;
  if (!spdlog::default_logger_raw()->should_log(spdlog::level::info)) return nullptr;
  return ctx;
}
}  // namespace detail

// RAII guard installed in Session::Execute_ per bolt message.
class ScopedSessionLog {
 public:
  explicit ScopedSessionLog(SessionLogContext *ctx) noexcept : prev_(std::exchange(detail::current_session_log, ctx)) {}

  ~ScopedSessionLog() noexcept { detail::current_session_log = prev_; }

  ScopedSessionLog(const ScopedSessionLog &) = delete;
  ScopedSessionLog &operator=(const ScopedSessionLog &) = delete;
  ScopedSessionLog(ScopedSessionLog &&) = delete;
  ScopedSessionLog &operator=(ScopedSessionLog &&) = delete;

  static SessionLogContext *Current() noexcept { return detail::current_session_log; }

 private:
  SessionLogContext *prev_;
};

// Caller-side gate for expensive args (JSON dumps, plan prints): Emit's args evaluate before its inner gate.
inline bool IsSessionTraceEnabled() noexcept { return detail::ActiveTraceContext() != nullptr; }

// No-op on threads without a guard (worker pool, GC, NuRaft).
template <typename... Args>
void EmitSessionTraceEvent(fmt::format_string<Args...> fmt_str, Args &&...args) {
  auto *ctx = detail::ActiveTraceContext();
  if (ctx == nullptr) return;
  fmt::memory_buffer buf;
  ctx->AppendTraceTags(buf);
  buf.push_back(' ');
  fmt::format_to(std::back_inserter(buf), fmt_str, std::forward<Args>(args)...);
  spdlog::info(std::string_view{buf.data(), buf.size()});
}

// Pre-formatted overload: no "{}" placeholder; braces in msg are emitted verbatim.
inline void EmitSessionTraceEvent(std::string_view msg) {
  auto *ctx = detail::ActiveTraceContext();
  if (ctx == nullptr) return;
  fmt::memory_buffer buf;
  ctx->AppendTraceTags(buf);
  buf.push_back(' ');
  buf.append(msg.data(), msg.data() + msg.size());
  spdlog::info(std::string_view{buf.data(), buf.size()});
}

}  // namespace memgraph::logging
