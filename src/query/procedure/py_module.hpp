/// @file
/// Functions and types for loading Query Modules written in Python.
#pragma once

#include "py/py.hpp"

struct mgp_value;

namespace query::procedure {

py::Object MgpValueToPyObject(const mgp_value &);

}  // namespace query::procedure
