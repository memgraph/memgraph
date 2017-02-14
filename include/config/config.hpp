#pragma once

#include "utils/config/config.hpp"

#include <string>
#include <set>

namespace config
{

// this class is used as a Definition class of config::Config class from utils
// number of elements shoud be small,
// it depends on implementation of config::Config class
// in other words number of fields in Definition class should be related
// to the number of config keys
class MemgraphConfig
{
public:
    static const char *env_config_key;
    static const char *default_file_path;
    static std::set<std::string> arguments;
};

// -- all possible Memgraph's keys --
constexpr const char *COMPILE_PATH = "compile_path";
constexpr const char *TEMPLATE_CPP_PATH = "template_cpp_path";
constexpr const char *SNAPSHOTS_PATH = "snapshots_path";
constexpr const char *CLEANING_CYCLE_SEC = "cleaning_cycle_sec";
constexpr const char *SNAPSHOT_CYCLE_SEC = "snapshot_cycle_sec";
constexpr const char *MAX_RETAINED_SNAPSHOTS = "max_retained_snapshots";
// -- all possible Memgraph's keys --

inline size_t to_int(std::string &&s) { return stoull(s); }
}

// code uses this define for key access
// _KEY_ is value from all possible keys that are listed above
#define CONFIG_REGISTER_ARGS(ARGC, ARGV) \
  config::Config<config::MemgraphConfig>::instance().register_args(ARGC, ARGV);
#define CONFIG(_KEY_) config::Config<config::MemgraphConfig>::instance()[_KEY_]

#define CONFIG_INTEGER(_KEY_) config::to_int(CONFIG(_KEY_))
