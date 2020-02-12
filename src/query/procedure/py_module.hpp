/// @file
/// Functions and types for loading Query Modules written in Python.
#pragma once

#include "py/py.hpp"

struct mgp_graph;
struct mgp_memory;
struct mgp_value;

namespace query::procedure {

py::Object MgpValueToPyObject(const mgp_value &);

/// Create the _mgp module for use in embedded Python.
///
/// The function is to be used before Py_Initialize via the following code.
///
///     PyImport_AppendInittab("_mgp", &query::procedure::PyInitMgpModule);
PyObject *PyInitMgpModule();

/// Create an instance of _mgp.Graph class.
PyObject *MakePyGraph(const mgp_graph *, mgp_memory *);

}  // namespace query::procedure
