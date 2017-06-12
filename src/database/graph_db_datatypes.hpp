#pragma once

#include <string>

namespace GraphDbTypes {
// definitions for what data types are used for a Label, Property, EdgeType
// TODO: Typedefing pointers is terrible astractions, get rid of it.
using Label = const std::string *;
using EdgeType = const std::string *;
using Property = const std::string *;
};
