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

#include <exception>
#include <string>

#include <gflags/gflags.h>

#include "license/license.hpp"
#include "license/license_sender.hpp"
#include "requests/requests.hpp"
#include "spdlog/spdlog.h"
#include "utils/logging.hpp"
#include "utils/synchronized.hpp"
#include "utils/system_info.hpp"
#include "utils/uuid.hpp"

DEFINE_string(endpoint, "http://127.0.0.1:5500/", "Endpoint that should be used for the test.");
DEFINE_string(license_type, "enterprise", "License type; can be oem or enterprise.");
DEFINE_int64(interval, 1, "Interval used for reporting telemetry in seconds.");
DEFINE_int64(duration, 10, "Duration of the test in seconds.");

memgraph::license::LicenseType StringToLicenseType(const std::string_view license_type) {
  if (license_type == "enterprise") {
    return memgraph::license::LicenseType::ENTERPRISE;
  }
  if (license_type == "oem") {
    return memgraph::license::LicenseType::OEM;
  }
  spdlog::critical("Invalid license type!");
  std::terminate();
}

int main(int argc, char **argv) {
  gflags::SetVersionString("license-info");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  memgraph::requests::Init();

  memgraph::license::License license{"Memgraph", 0, 0, StringToLicenseType(FLAGS_license_type)};
  memgraph::utils::Synchronized<std::optional<memgraph::license::LicenseInfo>, memgraph::utils::SpinLock> license_info{
      memgraph::license::LicenseInfo{"mg-testkey", "Memgraph"}};
  license_info.WithLock([license = std::move(license)](auto &license_info) {
    license_info->license = license;
    license_info->is_valid = true;
  });

  memgraph::license::LicenseInfoSender license_sender(FLAGS_endpoint, memgraph::utils::GenerateUUID(),
                                                      memgraph::utils::GetMachineId(), 10000000, license_info,
                                                      std::chrono::seconds(FLAGS_interval));
  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_duration));

  return 0;
}
