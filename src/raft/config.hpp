/// @file

#pragma once

#include <chrono>
#include <experimental/filesystem>
#include <ratio>

#include <json/json.hpp>

#include "raft/exceptions.hpp"
#include "utils/file.hpp"
#include "utils/string.hpp"

namespace raft {

/// Configurable Raft parameters.
struct Config {
  std::chrono::milliseconds election_timeout_min;
  std::chrono::milliseconds election_timeout_max;
  std::chrono::milliseconds heartbeat_interval;
  int64_t log_size_snapshot_threshold;

  static Config LoadFromFile(const std::string &raft_config_file) {
    if (!std::experimental::filesystem::exists(raft_config_file))
      throw RaftConfigException(raft_config_file);

    nlohmann::json data;
    try {
      data = nlohmann::json::parse(
          utils::Join(utils::ReadLines(raft_config_file), ""));
    } catch (const nlohmann::json::parse_error &e) {
      throw RaftConfigException(raft_config_file);
    }

    if (!data.is_object()) throw RaftConfigException(raft_config_file);
    if (!data["election_timeout_min"].is_number())
      throw RaftConfigException(raft_config_file);
    if (!data["election_timeout_max"].is_number())
      throw RaftConfigException(raft_config_file);
    if (!data["heartbeat_interval"].is_number())
      throw RaftConfigException(raft_config_file);
    if (!data["log_size_snapshot_threshold"].is_number())
      throw RaftConfigException(raft_config_file);

    return Config{
        std::chrono::duration<int64_t, std::milli>(
            data["election_timeout_min"]),
        std::chrono::duration<int64_t, std::milli>(
            data["election_timeout_max"]),
        std::chrono::duration<int64_t, std::milli>(data["heartbeat_interval"]),
        data["log_size_snapshot_threshold"]};
  }
};

}  // namespace raft
