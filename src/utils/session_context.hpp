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
#include <atomic>
#include <cstdint>
#include <iterator>
#include <string>
#include <string_view>
#include <utility>

namespace memgraph::logging {

// Per-session log state. Mutated and read only on the bolt thread; relaxed atomics suffice.
class SessionLogContext {
 public:
  // SET SESSION TRACE ON|OFF toggle. Events emit at INFO.
  void SetTrace(bool on) noexcept { trace_enabled_.store(on, std::memory_order_relaxed); }

  bool trace_enabled() const noexcept { return trace_enabled_.load(std::memory_order_relaxed); }

  void SetSessionUuid(std::string uuid) { session_uuid_ = std::move(uuid); }

  void SetUser(std::string user) { user_ = std::move(user); }

  void ClearUser() { user_.clear(); }

  void SetTxId(uint64_t tx_id) noexcept { tx_id_ = tx_id; }

  void ClearTxId() noexcept { tx_id_ = 0; }

  std::string_view session_uuid() const noexcept { return session_uuid_; }

  // Rebuilt every call (no cache). user omitted in userless mode.
  void AppendTraceTags(fmt::memory_buffer &out) const {
    fmt::format_to(std::back_inserter(out), "[session={}]", session_uuid_);
    if (!user_.empty()) fmt::format_to(std::back_inserter(out), " [user={}]", user_);
    fmt::format_to(std::back_inserter(out), " [tx={}]", tx_id_);
  }

 private:
  std::string session_uuid_;
  std::string user_;
  uint64_t tx_id_ = 0;
  std::atomic<bool> trace_enabled_{false};
};

namespace detail {
inline thread_local SessionLogContext *current_session_log = nullptr;
inline thread_local fmt::memory_buffer log_assembly_buf;  // reused across emits

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

// Gate *expensive* arg construction (JSON dumps, plan prints) on this — Emit's args
// evaluate before its own inner gate.
inline bool IsSessionTraceEnabled() noexcept { return detail::ActiveTraceContext() != nullptr; }

// No-op on threads without a guard (worker pool, GC, NuRaft) — don't emit inside worker lambdas.
template <typename... Args>
void EmitSessionTraceEvent(fmt::format_string<Args...> fmt_str, Args &&...args) {
  auto *ctx = detail::ActiveTraceContext();
  if (ctx == nullptr) return;
  auto &buf = detail::log_assembly_buf;
  buf.clear();
  ctx->AppendTraceTags(buf);
  buf.push_back(' ');
  fmt::format_to(std::back_inserter(buf), fmt_str, std::forward<Args>(args)...);
  spdlog::default_logger_raw()->log(spdlog::level::info, std::string_view{buf.data(), buf.size()});
}

}  // namespace memgraph::logging
