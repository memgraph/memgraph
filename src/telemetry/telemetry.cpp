// Copyright 2023 Memgraph Ltd.
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

#include <filesystem>
#include <utility>

#include <fmt/format.h>

#include "communication/bolt/metrics.hpp"
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

namespace memgraph::telemetry {

constexpr auto kMaxBatchSize{100};

Telemetry::Telemetry(std::string url, std::filesystem::path storage_directory, std::string uuid, std::string machine_id,
                     bool ssl, std::filesystem::path root_directory, std::chrono::duration<int64_t> refresh_interval,
                     const uint64_t send_every_n)
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
  scheduler_.Run("Telemetry", refresh_interval, [&] { CollectData(); });
}

void Telemetry::AddCollector(const std::string &name, const std::function<const nlohmann::json(void)> &func) {
  std::lock_guard<std::mutex> guard(lock_);
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
    std::lock_guard<std::mutex> guard(lock_);
    for (auto &collector : collectors_) {
      data[collector.first] = collector.second();
    }
  }
  if (event == "") {
    StoreData(num_++, data);
  } else {
    StoreData(event, data);
  }
  if (num_ % send_every_n_ == 0 || event == "shutdown") {
    SendData();
  }
}

const nlohmann::json Telemetry::GetUptime() { return timer_.Elapsed().count(); }

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

void Telemetry::AddStorageCollector(
    dbms::DbmsHandler &dbms_handler,
    memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> &auth) {
  AddCollector("storage", [&dbms_handler, &auth]() -> nlohmann::json {
    auto stats = dbms_handler.Stats();
    stats.users = auth->AllUsers().size();
    return ToJson(stats);
  });
}

void Telemetry::AddExceptionCollector() {
  AddCollector("exception", []() -> nlohmann::json { return memgraph::metrics::global_counters_map.ToJson(); });
}

void Telemetry::AddReplicationCollector() {
  // TODO Waiting for the replication refactor to be done before implementing the telemetry
  AddCollector("replication", []() -> nlohmann::json { return {{"async", -1}, {"sync", -1}}; });
}

}  // namespace memgraph::telemetry
