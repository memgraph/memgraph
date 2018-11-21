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
  std::chrono::milliseconds leader_timeout_min;
  std::chrono::milliseconds leader_timeout_max;
  std::chrono::milliseconds heartbeat_interval;
  std::chrono::milliseconds replicate_timeout;

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
    if (!data["leader_timeout_min"].is_number())
      throw RaftConfigException(raft_config_file);
    if (!data["leader_timeout_max"].is_number())
      throw RaftConfigException(raft_config_file);
    if (!data["heartbeat_interval"].is_number())
      throw RaftConfigException(raft_config_file);
    if (!data["replicate_timeout"].is_number())
      throw RaftConfigException(raft_config_file);

    return Config{std::chrono::duration<int64_t>(data["leader_timeout_min"]),
                  std::chrono::duration<int64_t>(data["leader_timeout_max"]),
                  std::chrono::duration<int64_t>(data["heartbeat_interval"]),
                  std::chrono::duration<int64_t>(data["replicate_timeout"])};
  }
};

}  // namespace raft
