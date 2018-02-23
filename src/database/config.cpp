#include <limits>

#include "database/graph_db.hpp"
#include "utils/flag_validation.hpp"

// Durability flags.
DEFINE_bool(durability_enabled, false,
            "If durability (database persistence) should be enabled");
DEFINE_string(
    durability_directory, "durability",
    "Path to directory in which to save snapshots and write-ahead log files.");
DEFINE_bool(db_recover_on_startup, false, "Recover database on startup.");
DEFINE_VALIDATED_int32(
    snapshot_cycle_sec, 3600,
    "Amount of time between two snapshots, in seconds (min 60).",
    FLAG_IN_RANGE(1, std::numeric_limits<int32_t>::max()));
DEFINE_int32(snapshot_max_retained, -1,
             "Number of retained snapshots, -1 means without limit.");
DEFINE_bool(snapshot_on_exit, false, "Snapshot on exiting the database.");

// Misc flags
DEFINE_int32(query_execution_time_sec, 180,
             "Maximum allowed query execution time. Queries exceeding this "
             "limit will be aborted. Value of -1 means no limit.");
DEFINE_int32(gc_cycle_sec, 30,
             "Amount of time between starts of two cleaning cycles in seconds. "
             "-1 to turn off.");

// Distributed master/worker flags.
DEFINE_HIDDEN_int32(worker_id, 0,
                    "ID of a worker in a distributed system. Igored in "
                    "single-node and distributed-master.");
DEFINE_HIDDEN_string(master_host, "0.0.0.0",
                     "For master node indicates the host served on. For worker "
                     "node indicates the master location.");
DEFINE_VALIDATED_HIDDEN_int32(
    master_port, 0,
    "For master node the port on which to serve. For "
    "worker node indicates the master's port.",
    FLAG_IN_RANGE(0, std::numeric_limits<uint16_t>::max()));
DEFINE_HIDDEN_string(worker_host, "0.0.0.0",
                     "For worker node indicates the host served on. For master "
                     "node this flag is not used.");
DEFINE_VALIDATED_HIDDEN_int32(
    worker_port, 0,
    "For master node it's unused. For worker node "
    "indicates the port on which to serve. If zero (default value), a port is "
    "chosen at random. Sent to the master when registring worker node.",
    FLAG_IN_RANGE(0, std::numeric_limits<uint16_t>::max()));
DEFINE_VALIDATED_HIDDEN_int32(rpc_num_workers,
                              std::max(std::thread::hardware_concurrency(), 1U),
                              "Number of workers (RPC)",
                              FLAG_IN_RANGE(1, INT32_MAX));

database::Config::Config()
    // Durability flags.
    : durability_enabled{FLAGS_durability_enabled},
      durability_directory{FLAGS_durability_directory},
      db_recover_on_startup{FLAGS_db_recover_on_startup},
      snapshot_cycle_sec{FLAGS_snapshot_cycle_sec},
      snapshot_max_retained{FLAGS_snapshot_max_retained},
      snapshot_on_exit{FLAGS_snapshot_on_exit},
      // Misc flags.
      gc_cycle_sec{FLAGS_gc_cycle_sec},
      query_execution_time_sec{FLAGS_query_execution_time_sec},
      rpc_num_workers{FLAGS_rpc_num_workers},
      // Distributed flags.
      worker_id{FLAGS_worker_id},
      master_endpoint{FLAGS_master_host,
                      static_cast<uint16_t>(FLAGS_master_port)},
      worker_endpoint{FLAGS_worker_host,
                      static_cast<uint16_t>(FLAGS_worker_port)} {}
