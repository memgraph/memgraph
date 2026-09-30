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
#include <functional>
#include <iterator>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
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

  // Effective auth user (empty when unauthenticated); shared with the trace tags.
  std::string_view user() const noexcept { return user_; }

  void AppendTraceTags(fmt::memory_buffer &out) const {
    fmt::format_to(std::back_inserter(out), "[session={}]", session_uuid_);
    if (!user_.empty()) fmt::format_to(std::back_inserter(out), " [user={}]", user_);
    fmt::format_to(std::back_inserter(out), " [tx={}]", tx_id_);
  }

  // Per-session overlay for runtime settings. Values are stored as raw strings;
  // the flags::run_time::GetEffective* readers parse them to the right type.
  void SetSetting(std::string_view key, std::string value) {
    session_settings_overlay_.insert_or_assign(std::string{key}, std::move(value));
  }

  void ResetSetting(std::string_view key) {
    // unordered_map::erase has no transparent overload; find (which does) then erase by iterator.
    if (auto it = session_settings_overlay_.find(key); it != session_settings_overlay_.end()) {
      session_settings_overlay_.erase(it);
    }
  }

  // noexcept relies on transparent lookup: find(string_view) must not allocate a temporary
  // std::string key (which could throw bad_alloc). Keep StringHash/equal_to<> transparent.
  std::optional<std::string_view> GetSetting(std::string_view key) const noexcept {
    auto it = session_settings_overlay_.find(key);
    if (it == session_settings_overlay_.end()) return std::nullopt;
    return std::string_view{it->second};
  }

 private:
  // Transparent hashing so reads/resets look up by string_view without allocating
  // a temporary std::string on every query.
  struct StringHash {
    using is_transparent = void;

    std::size_t operator()(std::string_view s) const noexcept { return std::hash<std::string_view>{}(s); }

    std::size_t operator()(const std::string &s) const noexcept { return std::hash<std::string_view>{}(s); }
  };

  std::string session_uuid_;
  std::string user_;
  uint64_t tx_id_ = 0;
  bool trace_enabled_ = false;
  std::unordered_map<std::string, std::string, StringHash, std::equal_to<>> session_settings_overlay_;
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

// Emit a [slow-query] line at WARN. Caller gates on the duration threshold first
// so plan rendering is never paid for under the threshold. Defined in the .cpp
// to keep this widely-included header light.
void EmitSlowQueryLog(std::string_view user, std::string_view db, std::string_view query, int64_t duration_ms,
                      std::optional<std::string_view> plan);

// Emit a [failed-query] line at ERROR. Caller gates on session attachment and the
// log.failed_queries flag first.
void EmitFailedQueryLog(std::string_view user, std::string_view db, std::string_view query, std::string_view error);

}  // namespace memgraph::logging
