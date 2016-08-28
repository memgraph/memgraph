#pragma once

#include <iostream>
#include <string>
#include <sstream>

#include "fmt/format.h"
#include "storage/model/properties/properties.hpp"
#include "storage/model/properties/traversers/consolewriter.hpp"
#include "storage/model/properties/traversers/jsonwriter.hpp"
#include "utils/types/byte.hpp"
#include "logging/default.hpp"

using std::cout;
using std::endl;

template <class T>
void print_props(const Properties<T> &properties);

#ifdef NDEBUG
#define PRINT_PROPS(_)
#else
#define PRINT_PROPS(_PROPS_) print_props(_PROPS_);
#endif

template <class T>
void cout_properties(const Properties<T> &properties);

void cout_property(const std::string &key, const Property &property);

// this is a nice way how to avoid multiple definition problem with
// headers because it will create a unique namespace for each compilation unit
// http://stackoverflow.com/questions/2727582/multiple-definition-in-header-file
namespace
{

template <typename... Args>
std::string format(const std::string &format_str, const Args &... args)
{
    return fmt::format(format_str, args...);
}

template <typename... Args>
std::string code_line(const std::string &format_str, const Args &... args)
{
    return "\t" + format(format_str, args...) + "\n";
}
}

class CoutSocket
{
public:
    CoutSocket() : logger(logging::log->logger("Cout Socket")) {}

    int write(const std::string& str)
    {
        logger.info(str);
        return str.size();
    }

    int write(const char* data, size_t len)
    {
        logger.info(std::string(data, len));
        return len;
    }

    int write(const byte* data, size_t len)
    {
        std::stringstream ss;
        for (int i = 0; i < len; i++) {
            ss << data[i];
        }
        std::string output(ss.str());
        cout << output << endl;
        logger.info(output);
        return len;
    }

private:
    Logger logger;
};
