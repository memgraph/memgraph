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
  TypedValue period;
  TypedValue start_time;

  static TypedValue ParsePeriod(std::string_view sv) {
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
    return TypedValue(utils::Duration(param));
  }

  static TypedValue ParseStartTime(std::string_view sv) {
    auto [param, _] = utils::ParseLocalTimeParameters(sv);
    return TypedValue(utils::LocalTime(param));
  }
};

class TTL final {
 public:
  ~TTL() { Stop(); }

  // explicit TTL(std::filesystem::path directory);
  // void RestoreTTL(TDbAccess db, InterpreterContext *interpreter_context);

  template <typename TDbAccess>
  void Create(TtlInfo ttl_info, /*std::shared_ptr<QueryUserOrRole> owner,*/ TDbAccess db,
              InterpreterContext *interpreter_context);

  void Stop() {
    auto ttl_locked = ttl_.Lock();
    ttl_locked->Stop();
  }

 private:
  using SynchronizedTtl = utils::Synchronized<utils::Scheduler, utils::WritePrioritizedRWLock>;
  SynchronizedTtl ttl_;
  TtlInfo info_;

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
