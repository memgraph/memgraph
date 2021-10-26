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

#include <gflags/gflags.h>

#include "requests/requests.hpp"
#include "telemetry/telemetry.hpp"

DEFINE_string(endpoint, "http://127.0.0.1:9000/", "Endpoint that should be used for the test.");
DEFINE_int64(interval, 1, "Interval used for reporting telemetry in seconds.");
DEFINE_int64(duration, 10, "Duration of the test in seconds.");
DEFINE_string(storage_directory, "", "Path to the storage directory where to save telemetry data.");

int main(int argc, char **argv) {
  gflags::SetVersionString("telemetry");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  requests::Init();
  telemetry::Telemetry telemetry(FLAGS_endpoint, FLAGS_storage_directory, std::chrono::seconds(FLAGS_interval), 1);

  uint64_t counter = 0;
  telemetry.AddCollector("db", [&counter]() -> nlohmann::json {
    ++counter;
    return {{"vertices", counter}, {"edges", counter}};
  });

  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_duration));

  return 0;
}
