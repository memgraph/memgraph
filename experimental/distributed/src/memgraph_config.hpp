#pragma once

#include <cstdint>
#include <cassert>
#include <fstream>
#include <utility>
#include <vector>
#include <string>

#include <gflags/gflags.h>

/**
 * About config file
 *
 * Each line contains three strings:
 *   memgraph node id, ip address of the worker, and port of the worker
 * Data on the first line is used to start master.
 * Data on the remaining lines is used to start workers.
 */
DECLARE_string(config_filename);

using MnidT = uint64_t;

struct Config {
  struct NodeConfig {
    MnidT mnid;
    std::string address;
    uint16_t port;
  };

  std::vector<NodeConfig> nodes;
};

/**
 * Parse config file.
 *
 * @return config object.
 */
Config ParseConfig(const std::string &filename = FLAGS_config_filename);
