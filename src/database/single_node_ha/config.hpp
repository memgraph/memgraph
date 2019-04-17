/// @file

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace database {

/// Database configuration. Initialized from flags, but modifiable.
struct Config {
  Config();

  // Durability flags.
  std::string durability_directory;
  bool db_recover_on_startup;

  // Misc flags.
  int gc_cycle_sec;
  int query_execution_time_sec;

  // set of properties which will be stored on disk
  std::vector<std::string> properties_on_disk;

  // RPC flags.
  uint16_t rpc_num_client_workers;
  uint16_t rpc_num_server_workers;

  // HA flags.
  std::string coordination_config_file;
  std::string raft_config_file;
  uint16_t server_id;
};
}  // namespace database
