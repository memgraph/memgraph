#pragma once

// #define YAML_CPP_DLL
// there are some problems with the yaml-cpp linking, something is missing
// cmake and make have passed fine
// and it seems that everything is included and linked
// the yaml-cpp lib is strange
// TODO debug yaml-cpp installation or write own yaml parser like a boss
// #include "yaml-cpp/yaml.h"

#include <cstring>
#include <string>
#include <stdexcept>

namespace config
{

class Config
{
private:
    // YAML::Node _config;
    Config()
    {
        //  TODO: config places:       priority
        //      1. default system         |
        //      2. default user           |
        //      3. ENV var                |
        //      4. program argument      \ /
        // _config = YAML::LoadFile("config.yaml");
    }

public:
    static Config& instance()
    {
        //  TODO resolve multithreading problems
        static Config config;
        return config;
    }

    std::string operator[](const char* key)
    {
        //  TODO write proper implementation, remove memgraph dependant
        //  stuff from here
 
        if (0 == std::strcmp(key, "compile_cpu_path"))
            return "./compiled/cpu/";

        if (std::strcmp(key, "template_cpu_path") == 0)
            return "./template/template_code_cpu.cpp";

        throw std::runtime_error("implement me");

        //  TODO optimize access
        // return _config[key].as<std::string>();
    }
};

}
