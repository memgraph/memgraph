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

#include "license/license_sender.hpp"

#include <spdlog/spdlog.h>
#include <cstdint>

#include "requests/requests.hpp"
#include "utils/memory_tracker.hpp"
#include "utils/stat.hpp"
#include "utils/synchronized.hpp"
#include "utils/system_info.hpp"
#include "utils/timestamp.hpp"

namespace memgraph::license {

LicenseInfoSender::LicenseInfoSender(std::string url, std::string uuid, std::string machine_id, int64_t memory_limit,
                                     utils::Synchronized<std::optional<LicenseInfo>, utils::SpinLock> &license_info,
                                     std::chrono::seconds request_frequency)
    : url_{std::move(url)},
      uuid_{std::move(uuid)},
      machine_id_{std::move(machine_id)},
      memory_limit_{memory_limit},
      license_info_{license_info} {
  scheduler_.Run("LicenseCheck", request_frequency, [&] { SendData(); });
}

LicenseInfoSender::~LicenseInfoSender() { scheduler_.Stop(); }

void LicenseInfoSender::SendData() {
  nlohmann::json data = nlohmann::json::object();

  license_info_.WithLock([&data, this](const auto &license_info) mutable {
    if (license_info && !license_info->organization_name.empty()) {
      const auto memory_info = utils::GetMemoryInfo();
      const auto memory_res = utils::GetMemoryUsage();
      data = {{"run_id", uuid_},
              {"machine_id", machine_id_},
              {"type", "license-check"},
              {"license_type", LicenseTypeToString(license_info->license.type)},
              {"license_key", license_info->license_key},
              {"organization", license_info->organization_name},
              {"valid", license_info->is_valid},
              {"physical_memory_size", memory_info.memory},
              {"swap_memory_size", memory_info.swap},
              {"memory_usage", memory_res},
              {"runtime_memory_limit", memory_limit_},
              {"license_memory_limit", license_info->license.memory_limit},
              {"timestamp", utils::Timestamp::Now().SecWithNsecSinceTheEpoch()}};
    }
  });

  if (data.empty()) {
    return;
  }
  if (!requests::RequestPostJson(url_, data,
                                 /* timeout_in_seconds = */ 2 * 60)) {
    spdlog::trace("Cannot send license information, enable {} availability!", url_);
  }
}

}  // namespace memgraph::license
