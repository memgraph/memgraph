#pragma once

#include <iostream>
#include <string>

#include "fmt/format.h"
#include "storage/model/properties/properties.hpp"
#include "storage/model/properties/traversers/consolewriter.hpp"
#include "storage/model/properties/traversers/jsonwriter.hpp"

using std::cout;
using std::endl;

void print_props(const Properties &properties)
{
    StringBuffer buffer;
    JsonWriter<StringBuffer> writer(buffer);
    properties.accept(writer);
    cout << buffer.str() << endl;
}

#ifdef DEBUG
#define PRINT_PROPS(_PROPS_) print_props(_PROPS_);
#else
#define PRINT_PROPS(_)
#endif

void cout_properties(const Properties &properties)
{
    ConsoleWriter writer;
    properties.accept(writer);
    cout << "----" << endl;
}

void cout_property(const std::string &key, const Property &property)
{
    ConsoleWriter writer;
    writer.handle(key, property);
    cout << "----" << endl;
}

// wrapper for fmt format
template <typename... Args>
std::string format(const std::string &format_str, const Args &... args)
{
    return fmt::format(format_str, args...);
}

// wrapper for single code line
template <typename... Args>
std::string code_line(const std::string &format_str, const Args &... args)
{
    return "\t" + format(format_str, args...) + "\n";
}


