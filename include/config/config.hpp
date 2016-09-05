#pragma once

#include "utils/config/config.hpp"

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
};

// -- all possible Memgraph's keys --
constexpr const char *COMPILE_CPU_PATH = "compile_cpu_path";
constexpr const char *TEMPLATE_CPU_CPP_PATH = "template_cpu_cpp_path";
constexpr const char *BARRIER_TEMPLATE_CPU_CPP_PATH =
    "barrier_template_cpu_cpp_path";
// -- all possible Memgraph's keys --

}

// code uses this define for key access
// _KEY_ is value from all possible keys that are listed above
#define CONFIG(_KEY_) config::Config<config::MemgraphConfig>::instance()[_KEY_]
