// Copyright 2021 Memgraph Ltd.
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

#include <fmt/format.h>

#include "requests/requests.hpp"
#include "telemetry/collectors.hpp"
#include "telemetry/system_info.hpp"
#include "utils/file.hpp"
#include "utils/logging.hpp"
#include "utils/timestamp.hpp"
#include "utils/uuid.hpp"

namespace telemetry {
namespace {
std::string GetMachineId() {
#ifdef MG_TELEMETRY_ID_OVERRIDE
  return MG_TELEMETRY_ID_OVERRIDE;
#else
  // We assume we're on linux and we need to read the machine id from /etc/machine-id
  const auto machine_id_lines = utils::ReadLines("/etc/machine-id");
  if (machine_id_lines.size() != 1) {
    return "UNKNOWN";
  }
  return machine_id_lines[0];
#endif
}
}  // namespace

const int kMaxBatchSize = 100;

Telemetry::Telemetry(std::string url, std::filesystem::path storage_directory,
                     std::chrono::duration<int64_t> refresh_interval, const uint64_t send_every_n)
    : url_(std::move(url)),
      uuid_(utils::GenerateUUID()),
      machine_id_(GetMachineId()),
      send_every_n_(send_every_n),
      storage_(std::move(storage_directory)) {
  StoreData("startup", GetSystemInfo());
  AddCollector("resources", GetResourceUsage);
  AddCollector("uptime", [&]() -> nlohmann::json { return GetUptime(); });
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
                            {"machine_id", machine_id_},
                            {"event", event},
                            {"data", data},
                            {"timestamp", utils::Timestamp::Now().SecWithNsecSinceTheEpoch()}};
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

}  // namespace telemetry
