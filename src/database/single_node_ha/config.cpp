#include <limits>

#include "database/single_node_ha/graph_db.hpp"
#include "utils/flag_validation.hpp"
#include "utils/string.hpp"

// Durability flags.
DEFINE_string(
    durability_directory, "durability",
    "Path to directory in which to save snapshots and write-ahead log files.");
DEFINE_bool(db_recover_on_startup, true, "Recover database on startup.");

// Misc flags
DEFINE_int32(query_execution_time_sec, 180,
             "Maximum allowed query execution time. Queries exceeding this "
             "limit will be aborted. Value of -1 means no limit.");
DEFINE_int32(gc_cycle_sec, 30,
             "Amount of time between starts of two cleaning cycles in seconds. "
             "-1 to turn off.");
// Data location.
DEFINE_string(properties_on_disk, "",
              "Property names of properties which will be stored on available "
              "disk. Property names have to be separated with comma (,).");

// RPC flags.
DEFINE_VALIDATED_HIDDEN_int32(
    rpc_num_client_workers, std::max(std::thread::hardware_concurrency(), 1U),
    "Number of client workers (RPC)",
    FLAG_IN_RANGE(1, std::numeric_limits<uint16_t>::max()));
DEFINE_VALIDATED_HIDDEN_int32(
    rpc_num_server_workers, std::max(std::thread::hardware_concurrency(), 1U),
    "Number of server workers (RPC)",
    FLAG_IN_RANGE(1, std::numeric_limits<uint16_t>::max()));

// High availability.
DEFINE_string(
    coordination_config_file, "coordination.json",
    "Path to the file containing coordination configuration in JSON format");

DEFINE_string(raft_config_file, "raft.json",
              "Path to the file containing raft configuration in JSON format");

DEFINE_VALIDATED_int32(
    server_id, 1U, "Id used in the coordination configuration for this machine",
    FLAG_IN_RANGE(1, std::numeric_limits<uint16_t>::max()));

database::Config::Config()
    // Durability flags.
    : durability_directory{FLAGS_durability_directory},
      db_recover_on_startup{FLAGS_db_recover_on_startup},
      // Misc flags.
      gc_cycle_sec{FLAGS_gc_cycle_sec},
      query_execution_time_sec{FLAGS_query_execution_time_sec},
      // Data location.
      properties_on_disk(utils::Split(FLAGS_properties_on_disk, ",")),
      // RPC flags.
      rpc_num_client_workers{
          static_cast<uint16_t>(FLAGS_rpc_num_client_workers)},
      rpc_num_server_workers{
          static_cast<uint16_t>(FLAGS_rpc_num_server_workers)},
      // High availability.
      coordination_config_file{FLAGS_coordination_config_file},
      raft_config_file{FLAGS_raft_config_file},
      server_id{static_cast<uint16_t>(FLAGS_server_id)} {}
