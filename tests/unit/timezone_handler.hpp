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

#include "flags/run_time_configurable.hpp"
#include "utils/settings.hpp"

struct HandleTimezone {
  HandleTimezone() {
    memgraph::utils::global_settings.Initialize("/tmp/utils_temporal");
    memgraph::flags::run_time::Initialize();
  }
  ~HandleTimezone() {
    memgraph::utils::global_settings.SetValue("timezone", "UTC");
    memgraph::utils::global_settings.Finalize();
    std::filesystem::remove_all("/tmp/utils_temporal");
  }

  void Set(std::string_view tz) { memgraph::utils::global_settings.SetValue("timezone", std::string{tz}); }

  // Make sure tests are not using both daylight-saving and standard time
  int64_t GetOffset_us() {
    const auto info = std::chrono::locate_zone(*memgraph::utils::global_settings.GetValue("timezone"))
                          ->get_info(std::chrono::sys_time<std::chrono::seconds>{});
    return std::chrono::duration_cast<std::chrono::microseconds>(info.offset - info.save).count();
  };
};
