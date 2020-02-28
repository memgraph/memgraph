/// @file
/// Functions and types for loading Query Modules written in Python.
#pragma once

#include "py/py.hpp"

struct mgp_graph;
struct mgp_memory;
struct mgp_module;
struct mgp_value;

namespace query::procedure {

struct PyGraph;

/// Convert an `mgp_value` into a Python object, referencing the given `PyGraph`
/// instance and using the same allocator as the graph.
///
/// Return a non-null `py::Object` instance on success. Otherwise, return a null
/// `py::Object` instance and set the appropriate Python exception.
py::Object MgpValueToPyObject(const mgp_value &value, PyGraph *py_graph);

py::Object MgpValueToPyObject(const mgp_value &value, PyObject *py_graph);

/// Convert a Python object into `mgp_value`, constructing it using the given
/// `mgp_memory` allocator.
///
/// @throw std::bad_alloc
/// @throw std::overflow_error if attempting to convert a Python integer which
///   too large to fit into int64_t.
/// @throw std::invalid_argument if the given Python object cannot be converted
///   to an mgp_value (e.g. a dictionary whose keys aren't strings or an object
///   of unsupported type).
mgp_value *PyObjectToMgpValue(PyObject *, mgp_memory *);

/// Create the _mgp module for use in embedded Python.
///
/// The function is to be used before Py_Initialize via the following code.
///
///     PyImport_AppendInittab("_mgp", &query::procedure::PyInitMgpModule);
PyObject *PyInitMgpModule();

/// Create an instance of _mgp.Graph class.
PyObject *MakePyGraph(const mgp_graph *, mgp_memory *);

/// Import a module with given name in the context of mgp_module.
///
/// This function can only be called when '_mgp' module has been initialized in
/// Python.
///
/// Return nullptr and set appropriate Python exception on failure.
py::Object ImportPyModule(const char *, mgp_module *);

/// Reload already loaded Python module in the context of mgp_module.
///
/// This function can only be called when '_mgp' module has been initialized in
/// Python.
///
/// Return nullptr and set appropriate Python exception on failure.
py::Object ReloadPyModule(PyObject *, mgp_module *);

}  // namespace query::procedure
