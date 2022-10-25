// Copyright 2022 Memgraph Ltd.
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
#include <string>

namespace memgraph::license {

class LicenseInfoSender final {
 public:
  LicenseInfoSender(std::string url, std::filesystem::path storage_directory,
                    std::chrono::duration<int64_t> request_frequency = std::chrono::hours(10));

 private:
  void StoreData(const nlohmann::json &event, const nlohmann::json &data);
  void SendData();
  void CollectData(const std::string &event = "");

  const std::string url_;
  const std::string uuid_;
  const std::string machine_id_;
  uint64_t num_{0};
  utils::Scheduler scheduler_;
  utils::Timer timer_;

  const uint64_t send_every_n_;

  std::mutex lock_;
  std::vector<std::pair<std::string, std::function<const nlohmann::json(void)>>> collectors_;

  kvstore::KVStore storage_;
};

}  // namespace memgraph::license
