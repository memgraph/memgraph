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

#pragma once

#include <chrono>
#include <cstdint>
#include <string>

#include <json/json.hpp>

#include "license/license.hpp"
#include "utils/scheduler.hpp"
#include "utils/timer.hpp"

namespace memgraph::license {

class LicenseInfoSender final {
 public:
  LicenseInfoSender(std::string url, std::string uuid, std::string machine_id, int64_t memory_limit,
                    utils::Synchronized<std::optional<LicenseInfo>, utils::SpinLock> &license_info,
                    std::chrono::seconds request_frequency = std::chrono::seconds(8 * 60 * 60));

  LicenseInfoSender(const LicenseInfoSender &) = delete;
  LicenseInfoSender(LicenseInfoSender &&) noexcept = delete;
  LicenseInfoSender &operator=(const LicenseInfoSender &) = delete;
  LicenseInfoSender &operator=(LicenseInfoSender &&) noexcept = delete;
  ~LicenseInfoSender();

 private:
  void SendData();

  const std::string url_;
  const std::string uuid_;
  const std::string machine_id_;
  const int64_t memory_limit_;

  utils::Synchronized<std::optional<LicenseInfo>, utils::SpinLock> &license_info_;
  utils::Scheduler scheduler_;
};

}  // namespace memgraph::license
