#pragma once

#include <iostream>
#include <sstream>
#include <string>
#include <map>

#include "fmt/format.h"
#include "logging/default.hpp"
#include "storage/model/properties/properties.hpp"
#include "storage/model/properties/property.hpp"
#include "storage/model/properties/traversers/consolewriter.hpp"
#include "storage/model/properties/traversers/jsonwriter.hpp"
#include "utils/types/byte.hpp"
#include "utils/exceptions/basic_exception.hpp"

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

template <class T>
void cout_property(const StoredProperty<T> &property);

// this is a nice way how to avoid multiple definition problem with
// headers because it will create a unique namespace for each compilation unit
// http://stackoverflow.com/questions/2727582/multiple-definition-in-header-file
namespace
{

class CodeLineFormatException : public BasicException
{
public:
    using BasicException::BasicException;
};

template <typename... Args>
std::string format(const std::string &format_str, const Args &... args)
{
    return fmt::format(format_str, args...);
}

template <typename... Args>
std::string code_line(const std::string &format_str, const Args &... args)
{
    try {
        return "\t" + format(format_str, args...) + "\n";
    } catch (std::runtime_error &e) {
        throw CodeLineFormatException(std::string(e.what()) + " " + format_str);
    }
}

using name_properties_t = std::vector<std::pair<std::string, Property>>;

auto query_properties(const std::map<std::string, int64_t> &indices,
                      properties_t &values)
{
    name_properties_t properties;
    for (auto &property_index : indices) {
        properties.push_back(
            std::make_pair(std::move(property_index.first),
                           std::move(values[property_index.second])));
    }
    return properties;
}

class CoutSocket
{
public:
    CoutSocket() : logger(logging::log->logger("Cout Socket")) {}

    int write(const std::string &str)
    {
        logger.info(str);
        return str.size();
    }

    int write(const char *data, size_t len)
    {
        logger.info(std::string(data, len));
        return len;
    }

    int write(const byte *data, size_t len)
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

}
