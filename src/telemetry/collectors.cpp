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

#include "telemetry/collectors.hpp"

#include <filesystem>
#include <string>
#include <utility>

#include <sys/types.h>
#include <unistd.h>

#include "utils/file.hpp"
#include "utils/stat.hpp"
#include "utils/string.hpp"

namespace memgraph::telemetry {

const std::pair<const std::string, const double> GetCpuUsage(pid_t pid, pid_t tid = 0) {
  std::string name;
  double cpu = 0;
  std::string path = "";
  if (tid == 0) {
    path = fmt::format("/proc/{}/stat", pid);
  } else {
    path = fmt::format("/proc/{}/task/{}/stat", pid, tid);
  }
  auto stat_data = utils::ReadLines(path);
  if (stat_data.size() >= 1) {
    auto split = utils::Split(stat_data[0]);
    if (split.size() >= 20) {
      int off = 0;
      for (int i = 1; i < split.size(); ++i) {
        if (utils::EndsWith(split[i], ")")) {
          off = i - 1;
          break;
        }
      }
      // These fields are: utime, stime, cutime, cstime.
      // Their description can be found in `man proc` under `/proc/[pid]/stat`.
      for (int i = 14; i <= 17; ++i) {
        cpu += std::stoull(split[i - 1 + off]);
      }
      name = utils::Trim(utils::Join(std::vector<std::string>(split.begin() + 1, split.begin() + 2 + off), " "), "()");
    }
  }
  cpu /= sysconf(_SC_CLK_TCK);
  return {name, cpu};
}

const nlohmann::json GetResourceUsage(std::filesystem::path root_directory) {
  // Get PID of entire process.
  pid_t pid = getpid();

  // Get CPU usage for each thread and total usage.
  nlohmann::json cpu = nlohmann::json::object();
  cpu["threads"] = nlohmann::json::array();

  // Find all threads.
  std::string task_file = fmt::format("/proc/{}/task", pid);
  if (!std::filesystem::exists(task_file)) {
    return nlohmann::json::object();
  }
  for (auto &file : std::filesystem::directory_iterator(task_file)) {
    auto split = utils::Split(file.path().string(), "/");
    if (split.size() < 1) continue;
    pid_t tid = std::stoi(split[split.size() - 1]);
    auto cpu_usage = GetCpuUsage(pid, tid);
    cpu["threads"].push_back({{"name", cpu_usage.first}, {"usage", cpu_usage.second}});
  }
  auto cpu_total = GetCpuUsage(pid);
  cpu["usage"] = cpu_total.second;

  nlohmann::json json;
  json.emplace_back("cpu", cpu);
  json.emplace_back("memory", utils::GetMemoryUsage());
  json.emplace_back("disk", utils::GetDirDiskUsage(root_directory));
  json.emplace_back("vm_max_map_count", utils::GetVmMaxMapCount());
  return json;
}

}  // namespace memgraph::telemetry
