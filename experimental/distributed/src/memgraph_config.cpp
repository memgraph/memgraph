#include "memgraph_config.hpp"

DEFINE_string(config_filename, "", "File containing list of all processes");

Config ParseConfig(const std::string &filename) {
  std::ifstream file(filename, std::ifstream::in);
  assert(file.good());

  Config config;

  while (file.good()) {
    MnidT mnid;
    std::string address;
    uint16_t port;

    file >> mnid >> address >> port;
    if (file.eof())
      break;

    config.nodes.push_back(Config::NodeConfig{mnid, address, port});
  }

  file.close();
  return config;
}
