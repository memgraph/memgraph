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

#include <filesystem>
#include <string>

#ifdef MG_ENTERPRISE

#include <fmt/core.h>
#include <chrono>
#include <nlohmann/json_fwd.hpp>
#include <optional>

#include "kvstore/kvstore.hpp"
#include "utils/exceptions.hpp"
#include "utils/scheduler.hpp"

namespace memgraph::query {

struct InterpreterContext;

namespace ttl {

class TtlException : public utils::BasicException {
 public:
  using BasicException::BasicException;
  SPECIALIZE_GET_EXCEPTION_NAME(TtlException)
};

struct TtlInfo {
  std::optional<std::chrono::microseconds> period;
  std::optional<std::chrono::system_clock::time_point> start_time;

  TtlInfo() = default;

  TtlInfo(std::optional<std::chrono::microseconds> period,
          std::optional<std::chrono::system_clock::time_point> start_time)
      : period{period}, start_time{start_time} {}

  TtlInfo(std::string_view period_sv, std::string_view start_time_sv) {
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
    return str;
  }

  /**
   * @brief
   *
   * @param sv
   * @return std::chrono::microseconds
   */
  static std::chrono::microseconds ParsePeriod(std::string_view sv);

  /**
   * @brief
   *
   * @param us
   * @return std::string
   */
  static std::string StringifyPeriod(std::chrono::microseconds us);

  /**
   * @brief
   *
   * @param sv
   * @return std::chrono::system_clock::time_point
   */
  static std::chrono::system_clock::time_point ParseStartTime(std::string_view sv);

  /**
   * @brief
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
  explicit TTL(std::filesystem::path directory) : storage_{directory} {}

  ~TTL() = default;

  TTL(const TTL &) = delete;
  TTL(TTL &&) = delete;
  TTL &operator=(const TTL &) = delete;
  TTL &operator=(TTL &&) = delete;

  /**
   * @brief Restore from durable data.
   *
   * @tparam TDbAccess
   * @param db
   * @param interpreter_context
   * @return true
   * @return false
   */
  template <typename TDbAccess>
  bool Restore(TDbAccess db, InterpreterContext *interpreter_context);

  /**
   * @brief Configure the TTL's background job period and time of execution.
   *
   * @param ttl_info
   */
  void Configure(TtlInfo ttl_info) {
    if (!enabled_) {
      throw TtlException("TTL not enabled!");
    }
    if (ttl_.IsRunning()) {
      throw TtlException("TTL already running!");
    }
    if (!ttl_info.period) {
      throw TtlException("TTL requires a defined period");
    }
    info_ = ttl_info;
    Persist_();
  }

  TtlInfo Config() const { return info_; }

  /**
   * @brief Setup TTL background job. Configuration should have already be present.
   *
   * @tparam TDbAccess
   * @param db
   * @param interpreter_context
   */
  template <typename TDbAccess>
  void Setup(TDbAccess db, InterpreterContext *interpreter_context, bool should_run_edge_ttl) {
    Setup_(db, interpreter_context, should_run_edge_ttl);
  }

  /**
   * @brief Stop background thread, but leave configuration as is.
   *
   */
  void Stop() {
    ttl_.Stop();
    Persist_();
  }

  /**
   * @brief Stops TTL without affecting the durable data. Use for destruction only.
   */
  void Shutdown() { ttl_.Stop(); }

  bool Enabled() const { return enabled_; }

  /**
   * @brief Enable the TTL feature.
   *
   */
  void Enable() {
    enabled_ = true;
    Persist_();
  }

  /**
   * @brief Returns whether TTL is running. @note: Paused TTL still counts as running.
   *
   */
  bool Running() { return ttl_.IsRunning(); }

  /**
   * @brief Disable the TTL feature.
   *
   */
  void Disable() {
    enabled_ = false;
    info_ = {};
    ttl_.Stop();
    Persist_();
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

 private:
  void Persist_() {
    std::map<std::string, std::string> data;
    data["version"] = "1.0";
    data["enabled"] = enabled_ ? "true" : "false";
    data["running"] = ttl_.IsRunning() ? "true" : "false";
    data["period"] = info_.period ? TtlInfo::StringifyPeriod(*info_.period) : "";
    data["start_time"] = info_.start_time ? TtlInfo::StringifyStartTime(*info_.start_time) : "";

    if (!storage_.PutMultiple(data)) {
      throw TtlException{"Couldn't persist TTL data"};
    }
  }

  template <typename TDbAccess>
  void Setup_(TDbAccess db, InterpreterContext *interpreter_context, bool should_run_edge_ttl);

  utils::Scheduler ttl_;      //!< background thread
  TtlInfo info_{};            //!< configuration
  bool enabled_{false};       //!< feature enabler
  kvstore::KVStore storage_;  //!< durability
};

}  // namespace ttl
}  // namespace memgraph::query

#else  // MG_ENTERPRISE

namespace memgraph::query::ttl {
/**
 * @brief Empty TTL implementation for simpler interface in community code
 *
 */
class TTL final {
 public:
  explicit TTL(std::filesystem::path directory) {}
  void Shutdown() {}
  void Stop() {}
  void Pause() {}
  void Resume() {}
};
}  // namespace memgraph::query::ttl

#endif  // MG_ENTERPRISE
