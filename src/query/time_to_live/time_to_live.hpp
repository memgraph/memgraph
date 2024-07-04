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
  std::optional<std::chrono::microseconds> start_time;  // from epoch

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

  static std::chrono::microseconds ParseStartTime(std::string_view sv) {
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
      return std::chrono::microseconds{utils::ZonedDateTime(zdt).SysMicrosecondsSinceEpoch()};
    } catch (const utils::temporal::InvalidArgumentException &e) {
      throw TtlException(e.what());
    }
  }
};

class TTL final {
 public:
  ~TTL() { Stop(); }

  // explicit TTL(std::filesystem::path directory);
  // void RestoreTTL(TDbAccess db, InterpreterContext *interpreter_context);

  template <typename TDbAccess>
  void Execute(TtlInfo ttl_info, /*std::shared_ptr<QueryUserOrRole> owner,*/ TDbAccess db,
               InterpreterContext *interpreter_context);

  void Stop() {
    auto ttl_locked = ttl_.Lock();
    ttl_locked->Stop();
  }

  void Enable() { enabled_ = true; }

  void Disable() {
    Stop();
    enabled_ = false;
  }

 private:
  using SynchronizedTtl = utils::Synchronized<utils::Scheduler, utils::WritePrioritizedRWLock>;
  SynchronizedTtl ttl_;
  TtlInfo info_;
  bool enabled_{false};

  // void Persist() {
  // const std::string stream_name = status.name;
  // if (!storage_.Put(stream_name, nlohmann::json(std::move(status)).dump())) {
  //   throw StreamsException{"Couldn't persist stream data for stream '{}'", stream_name};
  // }
  // }

  // kvstore::KVStore storage_;
};

}  // namespace ttl
}  // namespace memgraph::query
