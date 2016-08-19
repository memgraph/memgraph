#pragma once

#include "storage/model/properties/flags.hpp"

// This namespace names stuff
namespace
{
namespace name
{

std::string variable_property_key(const std::string &property_name, Type type)
{
    return "prop_" + property_name + "_" + type.to_str();
}

std::string unique() { return "unique_" + std::to_string(std::rand()); }
}
}
