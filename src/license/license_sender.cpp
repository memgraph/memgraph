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

#include "license/license_sender.hpp"

#include "requests/requests.hpp"
#include "utils/synchronized.hpp"
#include "utils/timestamp.hpp"

namespace memgraph::license {

LicenseInfoSender::LicenseInfoSender(std::string url, std::string uuid, std::string machine_id,
                                     utils::Synchronized<std::optional<LicenseInfo>, utils::SpinLock> &license_info,
                                     std::chrono::duration<int64_t> request_frequency)
    : url_{std::move(url)}, uuid_{std::move(uuid)}, machine_id_{std::move(machine_id)}, license_info_{license_info} {
  scheduler_.Run("LicenseCheck", request_frequency, [&] { SendData(); });
}

LicenseInfoSender::~LicenseInfoSender() { scheduler_.Stop(); }

void LicenseInfoSender::SendData() {
  nlohmann::json data = nlohmann::json::object();

  license_info_.WithLock([&data, this](const auto &license_info) mutable {
    if (license_info && !license_info->organization_name.empty()) {
      data = {{"run_id", uuid_},
              {"machine_id", machine_id_},
              {"type", "license-check"},
              {"license_type", LicenseTypeToString(license_info->license.type)},
              {"organization", license_info->organization_name},
              {"license_key", license_info->license_key},
              {"valid", fmt::format("{}", license_info->is_valid)},
              {"timestamp", utils::Timestamp::Now().SecWithNsecSinceTheEpoch()}};
    }
  });

  if (data.empty()) {
    return;
  }
  requests::RequestPostJson(url_, data,
                            /* timeout_in_seconds = */ 2 * 60);
}

}  // namespace memgraph::license
