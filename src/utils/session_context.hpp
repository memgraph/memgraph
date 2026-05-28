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
#include <iterator>
#include <string>
#include <string_view>
#include <utility>

namespace memgraph::logging {

// Per-session log state (tag prefix + trace toggle). Mutated and read only on the
// session's bolt thread; cross-message happens-before comes from the task-pool handoff.
class SessionLogContext {
 public:
  // SET SESSION TRACE ON|OFF toggle. Events emit at INFO, so --log-level still filters.
  void SetTrace(bool on) noexcept { trace_enabled_.store(on, std::memory_order_relaxed); }

  bool trace_enabled() const noexcept { return trace_enabled_.load(std::memory_order_relaxed); }

  void SetSessionUuid(std::string uuid) { session_uuid_ = std::move(uuid); }

  void SetUser(std::string user) { user_ = std::move(user); }

  void ClearUser() { user_.clear(); }

  void SetTxId(std::string tx_id) { tx_id_ = std::move(tx_id); }

  void ClearTxId() { tx_id_.clear(); }

  std::string_view session_uuid() const noexcept { return session_uuid_; }

  // Compose tags into `out`, omitting empty fields. Caller has already passed the emit gate.
  void AppendPrefixTo(fmt::memory_buffer &out) const {
    AppendTag(out, "[session=", session_uuid_);
    AppendTag(out, "[user=", user_);
    AppendTag(out, "[tx=", tx_id_);
  }

 private:
  static void AppendTag(fmt::memory_buffer &out, std::string_view label, std::string_view value) {
    if (value.empty()) return;
    if (out.size() != 0) out.push_back(' ');
    out.append(label.data(), label.data() + label.size());
    out.append(value.data(), value.data() + value.size());
    out.push_back(']');
  }

  std::string session_uuid_;
  std::string user_;
  std::string tx_id_;
  std::atomic<bool> trace_enabled_{false};  // relaxed everywhere: lone toggle, no data dep on the tag strings
};

namespace detail {
inline thread_local SessionLogContext *current_session_log = nullptr;
inline thread_local fmt::memory_buffer log_assembly_buf;  // reused across emits, no per-call alloc

// Returns the active trace context if a trace event would emit, nullptr otherwise.
// Cheap checks first: TLS load → relaxed toggle → spdlog level check. The spdlog
// atomic load is skipped on the common path (worker thread with no guard, or trace off).
inline SessionLogContext *ActiveTraceContext() noexcept {
  auto *ctx = current_session_log;
  if (ctx == nullptr || !ctx->trace_enabled()) return nullptr;
  if (!spdlog::default_logger_raw()->should_log(spdlog::level::info)) return nullptr;
  return ctx;
}
}  // namespace detail

// RAII guard installed by Session::Execute(); sets the thread-local, restores on dtor.
class ScopedSessionLog {
 public:
  explicit ScopedSessionLog(SessionLogContext *ctx) noexcept : prev_(detail::current_session_log) {
    detail::current_session_log = ctx;
  }

  ~ScopedSessionLog() noexcept { detail::current_session_log = prev_; }

  ScopedSessionLog(const ScopedSessionLog &) = delete;
  ScopedSessionLog &operator=(const ScopedSessionLog &) = delete;
  ScopedSessionLog(ScopedSessionLog &&) = delete;
  ScopedSessionLog &operator=(ScopedSessionLog &&) = delete;

  static SessionLogContext *Current() noexcept { return detail::current_session_log; }

 private:
  SessionLogContext *prev_;
};

// True iff a trace event would actually emit. Gate construction of *expensive* trace args
// (JSON / plan-tree dumps) on this — fn args are evaluated before EmitSessionTraceEvent's
// inner gate, so without this they'd be built even when the event would be dropped.
inline bool IsSessionTraceEnabled() noexcept { return detail::ActiveTraceContext() != nullptr; }

// Emit a structured trace event tagged with [session=..][user=..][tx=..]. Gated by SET
// SESSION TRACE ON and --log-level (events emit at INFO). Only threads under a
// ScopedSessionLog guard have a context — calls from guard-less threads (e.g. parallel
// operator workers) silently no-op, so don't add emits inside worker lambdas.
template <typename... Args>
void EmitSessionTraceEvent(fmt::format_string<Args...> fmt_str, Args &&...args) {
  auto *ctx = detail::ActiveTraceContext();
  if (ctx == nullptr) return;
  auto &buf = detail::log_assembly_buf;
  buf.clear();
  ctx->AppendPrefixTo(buf);
  if (buf.size() != 0) buf.push_back(' ');
  fmt::format_to(std::back_inserter(buf), fmt_str, std::forward<Args>(args)...);
  spdlog::default_logger_raw()->log(spdlog::level::info, std::string_view{buf.data(), buf.size()});
}

}  // namespace memgraph::logging
