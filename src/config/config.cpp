#include "config/config.hpp"

namespace config
{

const char *MemgraphConfig::env_config_key = "MEMGRAPH_CONFIG";
const char *MemgraphConfig::default_file_path = "/etc/memgraph/config.yaml";

// configuration for arguments which can be passed threw command line
// TODO add support for shortened names
// Example:
// --cleaning_cycle_sec or -ccs, etc.
std::set<std::string> MemgraphConfig::arguments = {
  "cleaning_cycle_sec",
  "snapshot_cycle_sec",
};

}
