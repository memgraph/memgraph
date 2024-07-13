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

#include <filesystem>
#include <string>

#ifdef MG_ENTERPRISE

#include <fmt/core.h>
#include <chrono>
#include <json/json.hpp>
#include <optional>

#include "query/typed_value.hpp"
#include "utils/exceptions.hpp"
#include "utils/rw_lock.hpp"
#include "utils/scheduler.hpp"
#include "utils/synchronized.hpp"
#include "utils/temporal.hpp"

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

  explicit operator bool() const { return period || start_time; }

  static std::chrono::microseconds ParsePeriod(std::string_view sv) {
    if (sv.empty()) return {};
    utils::DurationParameters param;
    int val = 0;
    for (const auto c : sv) {
      if (isdigit(c)) {
        val = val * 10 + (int)(c - '0');
      } else {
        switch (tolower(c)) {
          case 'd':
            param.day = val;
            break;
          case 'h':
            param.hour = val;
            break;
          case 'm':
            param.minute = val;
            break;
          case 's':
            param.second = val;
            break;
          default:
            throw TtlException("Badly defined period. Use integers and 'd', 'h', 'm' and 's' to define it.");
        }
        val = 0;
      }
    }
    return std::chrono::microseconds{utils::Duration(param).microseconds};
  }

  // We do not support microseconds, but are aligning to the timestamp() values
  static std::string StringifyPeriod(std::chrono::microseconds us) {
    std::string res;
    if (const auto di = GetPart<std::chrono::days>(us)) {
      res += fmt::format("{}d", di);
    }
    if (const auto hi = GetPart<std::chrono::hours>(us)) {
      res += fmt::format("{}h", hi);
    }
    if (const auto mi = GetPart<std::chrono::minutes>(us)) {
      res += fmt::format("{}m", mi);
    }
    if (const auto si = GetPart<std::chrono::seconds>(us)) {
      res += fmt::format("{}s", si);
    }
    return res;
  }

  /**
   * @brief From user's local time to system time. Uses timezone
   *
   * @param sv
   * @return std::chrono::system_clock::time_point
   */
  static std::chrono::system_clock::time_point ParseStartTime(std::string_view sv) {
    try {
      // Midnight might be a problem...
      const auto now =
          std::chrono::year_month_day{std::chrono::floor<std::chrono::days>(std::chrono::system_clock::now())};
      utils::DateParameters date{static_cast<int>(now.year()), static_cast<unsigned>(now.month()),
                                 static_cast<unsigned>(now.day())};
      auto [time, _] = utils::ParseLocalTimeParameters(sv);
      utils::ZonedDateTimeParameters zdt{date, time, utils::Timezone(std::chrono::current_zone()->name())};
      // Have to convert user's input (his local time) to system time
      // Using microseconds in order to be aligned with timestamp()
      return utils::ZonedDateTime(zdt).SysTimeSinceEpoch();
    } catch (const utils::temporal::InvalidArgumentException &e) {
      throw TtlException(e.what());
    }
  }

  /**
   *
   * @brief From system clock to user's local time. Uses timezone
   *
   * @param st
   * @return std::string
   */
  static std::string StringifyStartTime(std::chrono::system_clock::time_point st) {
    std::chrono::zoned_time zt(std::chrono::current_zone(), st);
    auto epoch = zt.get_local_time().time_since_epoch();
    /* just consume and through away */
    GetPart<std::chrono::days>(epoch);
    /* what we are actually interested in */
    const auto h = GetPart<std::chrono::hours>(epoch);
    const auto m = GetPart<std::chrono::minutes>(epoch);
    const auto s = GetPart<std::chrono::seconds>(epoch);
    return fmt::format("{:02d}:{:02d}:{:02d}", h, m, s);
  }

  template <typename T>
  static int GetPart(auto &current) {
    int whole_part = std::chrono::duration_cast<T>(current).count();
    current -= T{whole_part};
    return whole_part;
  }
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

  template <typename TDbAccess>
  bool Restore(TDbAccess db, InterpreterContext *interpreter_context);

  void Configure(TtlInfo ttl_info) {
    const std::lock_guard<std::mutex> lock{mtx_};
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

  TtlInfo Config() const {
    const std::lock_guard<std::mutex> lock{mtx_};
    return info_;
  }

  template <typename TDbAccess>
  void Execute(TDbAccess db, InterpreterContext *interpreter_context) {
    const std::lock_guard<std::mutex> lock{mtx_};
    Execute_(db, interpreter_context);
  }

  void Stop() {
    const std::lock_guard<std::mutex> lock{mtx_};
    ttl_.Stop();
    Persist_();
  }

  /**
   * @brief Stops TTL without affecting the durable data. Use when destruction only.
   */
  void Shutdown() {
    const std::lock_guard<std::mutex> lock{mtx_};
    ttl_.Stop();
  }

  bool Enabled() const {
    const std::lock_guard<std::mutex> lock{mtx_};
    return enabled_;
  }

  void Enable() {
    const std::lock_guard<std::mutex> lock{mtx_};
    enabled_ = true;
    Persist_();
  }

  bool Running() {
    const std::lock_guard<std::mutex> lock{mtx_};
    return ttl_.IsRunning();
  }

  void Disable() {
    const std::lock_guard<std::mutex> lock{mtx_};
    enabled_ = false;
    info_ = {};
    ttl_.Stop();
    Persist_();
  }

  void Resume() {
    const std::lock_guard<std::mutex> lock{mtx_};
    if (ttl_.IsRunning()) ttl_.Resume();
  }

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
  void Execute_(TDbAccess db, InterpreterContext *interpreter_context);

  mutable std::mutex mtx_;
  utils::Scheduler ttl_;
  TtlInfo info_{};
  bool enabled_{false};
  kvstore::KVStore storage_;
};

}  // namespace ttl
}  // namespace memgraph::query

#else  // MG_ENTERPRISE

namespace memgraph::query::ttl {
/**
 * @brief Empty TTL implementation for simpler interface in community mode
 *
 */
class TTL final {
 public:
  explicit TTL(std::filesystem::path directory) {}
  void Shutdown() {}
  void Stop() {}
  void Resume() {}
};
}  // namespace memgraph::query::ttl

#endif  // MG_ENTERPRISE
