#pragma once

#include <iostream>
#include <string>

#include "storage/model/properties/properties.hpp"
#include "storage/model/properties/traversers/jsonwriter.hpp"
#include "storage/model/properties/traversers/consolewriter.hpp"

using std::cout;
using std::endl;

std::string LINE(std::string line) { return "\t" + line + "\n"; }

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

void cout_property(const std::string& key, const Property& property)
{
    ConsoleWriter writer;
    writer.handle(key, property);
    cout << "----" << endl;
}
