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

#pragma once

#include <chrono>
#include <functional>
#include <optional>
#include <string>

#ifdef MG_ENTERPRISE

#include <fmt/core.h>
#include <nlohmann/json_fwd.hpp>

#include "utils/exceptions.hpp"
#include "utils/scheduler.hpp"

// Forward declarations
namespace memgraph::storage {
class Storage;
}  // namespace memgraph::storage

namespace memgraph::storage {

struct InterpreterContext;

namespace ttl {

class TtlException : public utils::BasicException {
 public:
  using BasicException::BasicException;
  SPECIALIZE_GET_EXCEPTION_NAME(TtlException)
};

class TtlMissingIndexException : public TtlException {
 public:
  enum class IndexType { LABEL_PROPERTY, EDGE_PROPERTY };

  TtlMissingIndexException(IndexType index_type, std::string_view details)
      : TtlException("TTL missing required index: " + std::string(details)), index_type_(index_type) {}

  IndexType GetIndexType() const { return index_type_; }

 private:
  IndexType index_type_;
};

struct TtlInfo {
  std::optional<std::chrono::microseconds> period;
  std::optional<std::chrono::system_clock::time_point> start_time;
  bool should_run_edge_ttl{false};

  TtlInfo() = default;

  TtlInfo(std::optional<std::chrono::microseconds> period,
          std::optional<std::chrono::system_clock::time_point> start_time, bool should_run_edge_ttl)
      : period{period}, start_time{start_time}, should_run_edge_ttl{should_run_edge_ttl} {}

  TtlInfo(std::string_view period_sv, std::string_view start_time_sv, bool should_run_edge_ttl)
      : should_run_edge_ttl{should_run_edge_ttl} {
    if (!period_sv.empty()) {
      period = ParsePeriod(period_sv);
    }
    if (!start_time_sv.empty()) {
      if (!period) {
        period = std::chrono::days(1);  // Default period with start time is a day
      }
      start_time = ParseStartTime(start_time_sv);
    }
  }

  std::string ToString() const {
    std::string str;
    if (period) {
      str += " every " + StringifyPeriod(*period);
    }
    if (start_time) {
      str += " at " + StringifyStartTime(*start_time);
    }
    if (should_run_edge_ttl) {
      str += " (edge TTL enabled)";
    }
    return str;
  }

  /**
   * @brief Parse a period string into microseconds
   *
   * @param sv
   * @return std::chrono::microseconds
   */
  static std::chrono::microseconds ParsePeriod(std::string_view sv);

  /**
   * @brief Convert microseconds to a string representation
   *
   * @param us
   * @return std::string
   */
  static std::string StringifyPeriod(std::chrono::microseconds us);

  /**
   * @brief Parse a start time string into a time point
   *
   * @param sv
   * @return std::chrono::system_clock::time_point
   */
  static std::chrono::system_clock::time_point ParseStartTime(std::string_view sv);

  /**
   * @brief Convert a time point to a string representation
   *
   * @param st
   * @return std::string
   */
  static std::string StringifyStartTime(std::chrono::system_clock::time_point st);

  explicit operator bool() const { return period || start_time; }
};

inline bool operator==(const TtlInfo &lhs, const TtlInfo &rhs) {
  return lhs.period == rhs.period && lhs.start_time == rhs.start_time;
}

/**
 * @brief Time-to-live handler.
 *
 * Interface is mutex protected, only so that coordinator can resume thread is paused.
 * TTL queries are uniquely accessed, to the interface doesn't need protecting from the user side.
 * However, if instance becomes a REPLICA, the thread is paused until reverted beck to MAIN.
 *
 */
class TTL final {
 public:
  explicit TTL(Storage *storage_ptr) : storage_ptr_(storage_ptr) {}
  ~TTL() = default;

  TTL(const TTL &) = delete;
  TTL(TTL &&) = delete;
  TTL &operator=(const TTL &) = delete;
  TTL &operator=(TTL &&) = delete;

  /**
   * @brief Setup and start the TTL background job
   *
   * @param period The TTL period (how often to run), defaults to 1 day if not provided
   * @param start_time Optional start time for the TTL job
   */
  void SetInterval(std::optional<std::chrono::microseconds> period = std::nullopt,
                   std::optional<std::chrono::system_clock::time_point> start_time = std::nullopt);

  /**
   * @brief Configure TTL with edge TTL setting
   *
   * @param should_run_edge_ttl Whether to enable edge TTL
   */
  void Configure(bool should_run_edge_ttl);

  /**
   * @brief Get the current TTL configuration
   *
   * @return TtlInfo
   */
  TtlInfo Config() const { /*TODO what is !enabled_? should we return a pair?*/ return info_; }

  /**
   * @brief Shutdown TTL (stop background job)
   *
   */
  void Shutdown() { ttl_.Stop(); }

  /**
   * @brief Check if TTL is enabled
   *
   * @return true if enabled, false otherwise
   */
  bool Enabled() const { return enabled_; }

  /**
   * @brief Enable the TTL feature.
   *
   */
  void Enable() { enabled_ = true; }

  /**
   * @brief Returns whether TTL is running. @note: Paused TTL still counts as running.
   *
   */
  bool Running() { return ttl_.IsRunning(); }

  bool Paused() const { return ttl_.IsPaused(); }

  /**
   * @brief Disable the TTL feature.
   *
   */
  void Disable() {
    enabled_ = false;
    info_ = {};
    ttl_.Stop();
  }

  /**
   * @brief TTL's background job should be paused in case instance becomes a REPLICA.
   *
   */
  void Pause() { ttl_.Pause(); }

  /**
   * @brief Use Resume() to restart once MAIN.
   *
   */
  void Resume() { ttl_.Resume(); }

  /**
   * @brief Set the function to check if this is a main instance
   *
   * @param check_fn Function that returns true if this is a main instance, false if replica
   */
  void SetUserCheck(std::function<bool()> check_fn) { user_check_.Update(std::move(check_fn)); }

 private:
  utils::Scheduler ttl_;  //!< background thread
  TtlInfo info_{};        //!< configuration
  bool enabled_{false};   //!< feature enabler
  Storage *storage_ptr_{};

  // User-defined function to check if this is a main instance
  // Returns true if this is a main instance, false if replica
  struct UserCheck {
    UserCheck() = default;
    explicit UserCheck(std::function<bool()> check_fn) : check_fn(std::move(check_fn)) {}

    void Update(std::function<bool()> check_fn) {
      auto lock = std::lock_guard(mutex_);
      this->check_fn = std::move(check_fn);
    }

    bool operator()() const {
      auto lock = std::lock_guard(mutex_);
      return check_fn();
    }

   private:
    std::function<bool()> check_fn{[]() { return true; }};
    mutable std::mutex mutex_;
  };

  UserCheck user_check_;
};

}  // namespace ttl
}  // namespace memgraph::storage

#else  // MG_ENTERPRISE

// Forward declarations
namespace memgraph::storage {
class Storage;
}  // namespace memgraph::storage

namespace memgraph::storage::ttl {

struct TtlInfo {
  TtlInfo() = default;
  TtlInfo(std::string_view, std::string_view) {}
  TtlInfo(std::optional<std::chrono::microseconds>, std::optional<std::chrono::system_clock::time_point>) {}
  TtlInfo(std::string_view, std::string_view, bool) {}
  TtlInfo(std::optional<std::chrono::microseconds>, std::optional<std::chrono::system_clock::time_point>, bool) {}
  std::string ToString() const { return ""; }
  explicit operator bool() const { return false; }
};

/**
 * @brief Empty TTL implementation for simpler interface in community code
 *
 */
class TTL final {
  std::optional<std::chrono::microseconds> period_{};
  std::optional<std::chrono::system_clock::time_point> start_time_{};

 public:
  explicit TTL(Storage * /*storage_ptr*/) {}
  void Shutdown() {}
  void Stop() {}
  void Pause() {}
  void Resume() {}
  bool Enabled() const { return false; }
  bool Running() { return false; }
  bool Paused() const { return false; }
  void Enable() {}
  void Disable() {}
  void Configure(bool) {}
  TtlInfo Config() const { return TtlInfo{}; }
  void SetInterval(std::optional<std::chrono::microseconds> = std::nullopt,
                   std::optional<std::chrono::system_clock::time_point> = std::nullopt) {}
  void SetUserCheck(std::function<bool()>) {}
  bool Restore() { return false; }
};

}  // namespace memgraph::storage::ttl

#endif  // MG_ENTERPRISE
