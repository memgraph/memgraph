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

#include "telemetry/telemetry.hpp"

#include <chrono>
#include <filesystem>
#include <utility>

#include <fmt/format.h>

#include "communication/bolt/metrics.hpp"
#include "query/plan/operator.hpp"
#include "requests/requests.hpp"
#include "telemetry/collectors.hpp"
#include "utils/event_counter.hpp"
#include "utils/event_map.hpp"
#include "utils/event_trigger.hpp"
#include "utils/file.hpp"
#include "utils/logging.hpp"
#include "utils/system_info.hpp"
#include "utils/timestamp.hpp"
#include "utils/uuid.hpp"
#include "version.hpp"

#include <mutex>

namespace {
constexpr auto kFirstShotAfter = std::chrono::seconds{60};
}  // namespace

namespace memgraph::telemetry {

constexpr auto kMaxBatchSize{100};

Telemetry::Telemetry(std::string url, std::filesystem::path storage_directory, std::string uuid, std::string machine_id,
                     bool const ssl, std::filesystem::path root_directory,
                     std::chrono::duration<int64_t> refresh_interval, const uint64_t send_every_n)
    : url_(std::move(url)),
      uuid_(std::move(uuid)),
      machine_id_(std::move(machine_id)),
      ssl_(ssl),
      send_every_n_(send_every_n),
      storage_(std::move(storage_directory)) {
  StoreData("startup", utils::GetSystemInfo());
  AddCollector("resources", [&, root_directory = std::move(root_directory)]() -> nlohmann::json {
    return GetResourceUsage(root_directory);
  });
  AddCollector("uptime", [&]() -> nlohmann::json { return GetUptime(); });
  AddCollector("query", [&]() -> nlohmann::json {
    return {
        {"first_successful_query",
         metrics::global_one_shot_events[metrics::OneShotEvents::kFirstSuccessfulQueryTs].load()},
        {"first_failed_query", metrics::global_one_shot_events[metrics::OneShotEvents::kFirstFailedQueryTs].load()}};
  });
  scheduler_.Pause();  // Don't run until all collects have been added
  scheduler_.SetInterval(
      std::min(kFirstShotAfter, refresh_interval));  // use user-defined interval if shorter than first shot
  scheduler_.Run("Telemetry", [this, final_interval = refresh_interval,
                               update_interval = kFirstShotAfter < refresh_interval]() mutable {
    CollectData();
    // First run after 60s; all subsequent runs at the user-defined interval
    if (update_interval) {
      update_interval = false;
      scheduler_.SetInterval(final_interval);
    }
  });
}

void Telemetry::Start() { scheduler_.Resume(); }

void Telemetry::AddCollector(const std::string &name, FuncSig &func) {
  auto guard = std::lock_guard{lock_};
  collectors_.emplace_back(name, func);
}

Telemetry::~Telemetry() {
  scheduler_.Stop();
  CollectData("shutdown");
}

void Telemetry::StoreData(const nlohmann::json &event, const nlohmann::json &data) {
  nlohmann::json payload = {{"run_id", uuid_},
                            {"type", "telemetry"},
                            {"machine_id", machine_id_},
                            {"event", event},
                            {"data", data},
                            {"timestamp", utils::Timestamp::Now().SecWithNsecSinceTheEpoch()},
                            {"ssl", ssl_},
                            {"version", version_string}};
  storage_.Put(fmt::format("{}:{}", uuid_, event.dump()), payload.dump());
}

void Telemetry::SendData() {
  std::vector<std::string> keys;
  nlohmann::json payload = nlohmann::json::array();

  int count = 0;
  for (auto it = storage_.begin(); it != storage_.end() && count < kMaxBatchSize; ++it, ++count) {
    keys.push_back(it->first);
    try {
      payload.push_back(nlohmann::json::parse(it->second));
    } catch (const nlohmann::json::parse_error &e) {
      SPDLOG_WARN("Couldn't convert {} to json", it->second);
    }
  }

  if (requests::RequestPostJson(url_, payload,
                                /* timeout_in_seconds = */ 2 * 60)) {
    for (const auto &key : keys) {
      if (!storage_.Delete(key)) {
        SPDLOG_WARN(
            "Couldn't delete key {}"
            " from telemetry storage!",
            key);
      }
    }
  }
}

void Telemetry::CollectData(const std::string &event) {
  nlohmann::json data = nlohmann::json::object();
  {
    auto guard = std::lock_guard{lock_};
    for (auto &collector : collectors_) {
      try {
        auto res = collector.second();
        if (res.has_value()) {
          data[collector.first] = std::move(*res);
        }
      } catch (std::exception &e) {
        spdlog::warn(fmt::format(
            "Unknown exception occurred on in telemetry server {}, please contact support on https://memgr.ph/unknown ",
            e.what()));
      }
    }
  }
  if (event.empty()) {
    StoreData(num_++, data);
  } else {
    StoreData(event, data);
  }
  if (num_ % send_every_n_ == 0 || event == "shutdown") {
    SendData();
  }
}
nlohmann::json Telemetry::GetUptime() const { return timer_.Elapsed().count(); }

void Telemetry::AddQueryModuleCollector() {
  AddCollector("query_module_counters",
               []() -> nlohmann::json { return memgraph::query::plan::CallProcedure::GetAndResetCounters(); });
}
void Telemetry::AddEventsCollector() {
  AddCollector("event_counters", []() -> nlohmann::json {
    nlohmann::json ret;
    for (size_t i = 0; i < memgraph::metrics::CounterEnd(); ++i) {
      ret[memgraph::metrics::GetCounterName(i)] = memgraph::metrics::global_counters[i].load(std::memory_order_relaxed);
    }
    return ret;
  });
}
void Telemetry::AddClientCollector() {
  AddCollector("client", []() -> nlohmann::json { return memgraph::communication::bolt_metrics.ToJson(); });
}

#ifdef MG_ENTERPRISE
void Telemetry::AddDatabaseCollector(dbms::DbmsHandler &dbms_handler) {
  AddCollector("database", [&dbms_handler]() -> nlohmann::json {
    const auto &infos = dbms_handler.Info();
    auto dbs = nlohmann::json::array();
    for (const auto &db_info : infos) {
      dbs.push_back(memgraph::dbms::ToJson(db_info));
    }
    return dbs;
  });
}
#else
#endif

void Telemetry::AddStorageCollector(dbms::DbmsHandler &dbms_handler, memgraph::auth::SynchedAuth &auth) {
  AddCollector("storage", [&dbms_handler, &auth]() -> nlohmann::json {
    auto stats = dbms_handler.Stats();
    stats.users = auth->AllUsers().size();
    return ToJson(stats);
  });
}

void Telemetry::AddExceptionCollector() {
  AddCollector("exception", []() -> nlohmann::json { return memgraph::metrics::global_counters_map.ToJson(); });
}

void Telemetry::AddReplicationCollector(
    utils::Synchronized<replication::ReplicationState, utils::RWSpinLock> const &repl_state) {
  // Optional because only main returns telemetry json data, replica returns empty o
  AddCollector("replication",
               [&repl_state]() -> std::optional<nlohmann::json> { return repl_state.ReadLock()->GetTelemetryJson(); });
}

#ifdef MG_ENTERPRISE
void Telemetry::AddCoordinatorCollector(std::optional<coordination::CoordinatorState> const &coordinator_state) {
  // Both leader and followers return the data
  AddCollector("coordination", [&coordinator_state]() -> std::optional<nlohmann::json> {
    if (coordinator_state.has_value() && coordinator_state->IsCoordinator())
      return coordinator_state->GetTelemetryJson();
    return std::nullopt;
  });
}
#endif

}  // namespace memgraph::telemetry
