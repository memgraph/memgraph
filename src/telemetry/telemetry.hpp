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

#include <mutex>
#include <string>

#include <json/json.hpp>

#include "dbms/dbms_handler.hpp"
#include "kvstore/kvstore.hpp"
#include "utils/scheduler.hpp"
#include "utils/timer.hpp"

namespace memgraph::telemetry {

/**
 * This class implements the telemetry collector service. It periodically scapes
 * all registered collectors and stores their data. With periodically scraping
 * the collectors the service collects machine information in the constructor
 * and stores it. Also, it calls all collectors once more in the destructor so
 * that final stats can be collected. All data is stored persistently. If there
 * is no internet connection the data will be sent when the internet connection
 * is reestablished. If there is an issue with the internet connection that
 * won't effect normal operation of the service (it won't crash).
 */
class Telemetry final {
 public:
  Telemetry(std::string url, std::filesystem::path storage_directory, std::string uuid, std::string machine_id,
            bool ssl, std::filesystem::path root_directory,
            std::chrono::duration<int64_t> refresh_interval = std::chrono::minutes(10), uint64_t send_every_n = 10);

  // Generic/user-defined collector
  void AddCollector(const std::string &name, const std::function<const nlohmann::json(void)> &func);

  // Specialized collectors
  void AddStorageCollector(dbms::DbmsHandler &dbms_handler, memgraph::auth::SynchedAuth &auth,
                           memgraph::replication::ReplicationState &repl_state);

#ifdef MG_ENTERPRISE
  void AddDatabaseCollector(dbms::DbmsHandler &dbms_handler, replication::ReplicationState &repl_state);
#else
  void AddDatabaseCollector() {
    AddCollector("database", []() -> nlohmann::json { return nlohmann::json::array(); });
  }
#endif
  void AddClientCollector();
  void AddEventsCollector();
  void AddQueryModuleCollector();
  void AddExceptionCollector();
  void AddReplicationCollector();

  ~Telemetry();

  Telemetry(const Telemetry &) = delete;
  Telemetry(Telemetry &&) = delete;
  Telemetry &operator=(const Telemetry &) = delete;
  Telemetry &operator=(Telemetry &&) = delete;

 private:
  void StoreData(const nlohmann::json &event, const nlohmann::json &data);
  void SendData();
  void CollectData(const std::string &event = "");

  const nlohmann::json GetUptime();

  const std::string url_;
  const std::string uuid_;
  const std::string machine_id_;
  const bool ssl_;
  uint64_t num_{0};
  utils::Scheduler scheduler_;
  utils::Timer timer_;

  const uint64_t send_every_n_;

  std::mutex lock_;
  std::vector<std::pair<std::string, std::function<const nlohmann::json(void)>>> collectors_;

  kvstore::KVStore storage_;
};

}  // namespace memgraph::telemetry
