#include "telemetry/system_info.hpp"

#include <string>

#include <sys/utsname.h>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "utils/file.hpp"
#include "utils/string.hpp"

namespace telemetry {

const nlohmann::json GetSystemInfo() {
  // Get `uname`.
  struct utsname info;
  if (uname(&info) != 0) return {};

  // Parse `/etc/os-release`.
  std::string os_name, os_version, os_full;
  auto os_data = utils::ReadLines("/etc/os-release");
  for (auto &row : os_data) {
    auto split = utils::Split(row, "=");
    if (split.size() < 2) continue;
    if (split[0] == "NAME") {
      os_name = utils::Trim(split[1], "\"");
    } else if (split[0] == "VERSION") {
      os_version = utils::Trim(split[1], "\"");
    }
    os_full = fmt::format("{} {}", os_name, os_version);
  }

  // Parse `/proc/cpuinfo`.
  std::string cpu_model;
  uint64_t cpu_count = 0;
  auto cpu_data = utils::ReadLines("/proc/cpuinfo");
  for (auto &row : cpu_data) {
    auto tmp = utils::Trim(row);
    if (tmp == "") {
      ++cpu_count;
    } else if (utils::StartsWith(tmp, "model name")) {
      auto split = utils::Split(tmp, ":");
      if (split.size() != 2) continue;
      cpu_model = utils::Trim(split[1]);
    }
  }

  // Parse `/proc/meminfo`.
  nlohmann::json ret;
  uint64_t memory = 0, swap = 0;
  auto mem_data = utils::ReadLines("/proc/meminfo");
  for (auto &row : mem_data) {
    auto tmp = utils::Trim(row);
    if (utils::StartsWith(tmp, "MemTotal")) {
      auto split = utils::Split(tmp);
      if (split.size() < 2) continue;
      memory = std::stoull(split[1]);
    } else if (utils::StartsWith(tmp, "SwapTotal")) {
      auto split = utils::Split(tmp);
      if (split.size() < 2) continue;
      swap = std::stoull(split[1]);
    }
  }
  memory *= 1024;
  swap *= 1024;

  return {{"architecture", info.machine},
          {"cpu_count", cpu_count},
          {"cpu_model", cpu_model},
          {"kernel", fmt::format("{} {}", info.release, info.version)},
          {"memory", memory},
          {"os", os_full},
          {"swap", swap},
          {"version", gflags::VersionString()}};
}

}  // namespace telemetry
