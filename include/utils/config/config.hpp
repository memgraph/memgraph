#pragma once

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>

#include <pwd.h>
#include <sys/types.h>
#include <unistd.h>

// TODO: isolate from caller (caller shouldn't know that his dependency is)
// yaml-cpp
#include "utils/command_line/arguments.hpp"
#include "yaml-cpp/yaml.h"

namespace config {
  
template <class Definition>
class Config {
 private:
  using KeyValueMap = std::map<std::string, std::string>;

  KeyValueMap dict;

  void load_configuration(std::string path) {
    try {
      YAML::Node node = YAML::LoadFile(path);
    
      for (YAML::const_iterator it = node.begin(); it != node.end(); ++it) {
        dict[it->first.as<std::string>()] = it->second.as<std::string>();
      }
    } catch (std::exception ex) {
      // configuration doesn't exist or invalid!!!
    }
  }

  Config() {
    // config places:             priority
    //     1. default system         |     (/etc/memgraph/config)
    //     2. default user           |     (/home/user/.memgraph/config)
    //     3. ENV var                |     (PATH)
    //     4. program argument      \ /    (PATH)

    // default system configuration
    load_configuration(Definition::default_file_path);

    // default user configuration
    // fetches user configuration folder
    std::string homedir;
    if ((homedir = getenv("HOME")) == "") {
      homedir = getpwuid(getuid())->pw_dir;
    }
    homedir += "/.memgraph/config.yaml";
    load_configuration(homedir);

    // environment variable configuratoin
    if (const char* env_path = std::getenv(Definition::env_config_key))
      load_configuration(env_path);

    // TODO add program arguments here
    // Define how will we pass program args and which ones are we using and how.
    // ProgramArguments::instance().get_arg();
  }

 public:
  static Config<Definition>& instance() {
    static Config<Definition> config;
    return config;
  }

  std::string operator[](const char* key) {
    return dict[key];
  }
};
}
