#pragma once

#include <cstring>
#include <string>
#include <stdexcept>
#include <cstdlib>

// TODO: isolate from caller (caller shouldn't know that his dependency is)
// yaml-cpp
#include "yaml-cpp/yaml.h"

namespace config
{

template <class Definition>
class Config
{
private:
    YAML::Node _config;

    Config()
    {
        // config places:             priority
        //     1. default system         |     (/etc/name/config)
        //     2. default user           |     (/home/user/.name/config)
        //     3. ENV var                |     (PATH)
        //     4. program argument      \ /    (PATH)
       
        if (const char* env_path = std::getenv(Definition::env_config_key))
        {
            _config = YAML::LoadFile(env_path);
            // TODO: error handling
        } 
        else
        {
            _config = YAML::LoadFile(Definition::default_file_path);
            // TODO: error handling
        }

        // TODO:
        //     * default system
        //     * default user
        //     * program argument
    }

public:
    static Config<Definition>& instance()
    {
        static Config<Definition> config;
        return config;
    }

    void register_program_arguments()
    {
    }

    std::string operator[](const char* key)
    {
        return _config[key].template as<std::string>();
    }
};

}
