#include "query/procedure/py_module.hpp"

#include <sstream>
#include <stdexcept>
#include <string>

#include "query/procedure/mg_procedure_impl.hpp"

namespace query::procedure {

// Set this as a __reduce__ special method on our types to prevent `pickle` and
// `copy` module operations on our types.
PyObject *DisallowPickleAndCopy(PyObject *self, PyObject *Py_UNUSED(ignored)) {
  auto *type = Py_TYPE(self);
  std::stringstream ss;
  ss << "cannot pickle nor copy '" << type->tp_name << "' object";
  const auto &msg = ss.str();
  PyErr_SetString(PyExc_TypeError, msg.c_str());
  return nullptr;
}

// Definitions of types wrapping C API types
//
// These should all be in the private `_mgp` Python module, which will be used
// by the `mgp` to implement the user friendly Python API.

// Wraps mgp_graph in a PyObject.
//
// Executing a `CALL python_module.procedure(...)` in openCypher should
// instantiate exactly 1 mgp_graph instance. We will rely on this assumption in
// order to test for validity of usage. The idea is to clear the `graph` to
// `nullptr` after the execution completes. If a user stored a reference to
// `_mgp.Graph` in their global Python state, then we are no longer working with
// a valid graph so `nullptr` will catch this. `_mgp.Graph` provides `is_valid`
// method for checking this by our higher level API in `mgp` module. Python only
// does shallow copies by default, and we do not provide deep copy of
// `_mgp.Graph`, so this validity concept should work fine.
//
// clang-format off
struct PyGraph {
  PyObject_HEAD
  const mgp_graph *graph;
  mgp_memory *memory;
};
// clang-format on

// clang-format off
struct PyVerticesIterator {
  PyObject_HEAD
  mgp_vertices_iterator *it;
  PyGraph *py_graph;
};
// clang-format on

PyObject *MakePyVertex(const mgp_vertex &vertex, PyGraph *py_graph);

void PyVerticesIteratorDealloc(PyVerticesIterator *self) {
  MG_ASSERT(self->it);
  MG_ASSERT(self->py_graph);
  // Avoid invoking `mgp_vertices_iterator_destroy` if we are not in valid
  // execution context. The query execution should free all memory used during
  // execution, so we may cause a double free issue.
  if (self->py_graph->graph) mgp_vertices_iterator_destroy(self->it);
  Py_DECREF(self->py_graph);
  Py_TYPE(self)->tp_free(self);
}

PyObject *PyVerticesIteratorGet(PyVerticesIterator *self, PyObject *Py_UNUSED(ignored)) {
  MG_ASSERT(self->it);
  MG_ASSERT(self->py_graph);
  MG_ASSERT(self->py_graph->graph);
  const auto *vertex = mgp_vertices_iterator_get(self->it);
  if (!vertex) Py_RETURN_NONE;
  return MakePyVertex(*vertex, self->py_graph);
}

PyObject *PyVerticesIteratorNext(PyVerticesIterator *self, PyObject *Py_UNUSED(ignored)) {
  MG_ASSERT(self->it);
  MG_ASSERT(self->py_graph);
  MG_ASSERT(self->py_graph->graph);
  const auto *vertex = mgp_vertices_iterator_next(self->it);
  if (!vertex) Py_RETURN_NONE;
  return MakePyVertex(*vertex, self->py_graph);
}

static PyMethodDef PyVerticesIteratorMethods[] = {
    {"__reduce__", reinterpret_cast<PyCFunction>(DisallowPickleAndCopy), METH_NOARGS, "__reduce__ is not supported"},
    {"get", reinterpret_cast<PyCFunction>(PyVerticesIteratorGet), METH_NOARGS,
     "Get the current vertex pointed to by the iterator or return None."},
    {"next", reinterpret_cast<PyCFunction>(PyVerticesIteratorNext), METH_NOARGS,
     "Advance the iterator to the next vertex and return it."},
    {nullptr},
};

// clang-format off
static PyTypeObject PyVerticesIteratorType = {
    PyVarObject_HEAD_INIT(nullptr, 0)
    .tp_name = "_mgp.VerticesIterator",
    .tp_basicsize = sizeof(PyVerticesIterator),
    .tp_dealloc = reinterpret_cast<destructor>(PyVerticesIteratorDealloc),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "Wraps struct mgp_vertices_iterator.",
    .tp_methods = PyVerticesIteratorMethods,
};
// clang-format on

// clang-format off
struct PyEdgesIterator {
  PyObject_HEAD
  mgp_edges_iterator *it;
  PyGraph *py_graph;
};
// clang-format on

PyObject *MakePyEdge(const mgp_edge &edge, PyGraph *py_graph);

void PyEdgesIteratorDealloc(PyEdgesIterator *self) {
  MG_ASSERT(self->it);
  MG_ASSERT(self->py_graph);
  // Avoid invoking `mgp_edges_iterator_destroy` if we are not in valid
  // execution context. The query execution should free all memory used during
  // execution, so we may cause a double free issue.
  if (self->py_graph->graph) mgp_edges_iterator_destroy(self->it);
  Py_DECREF(self->py_graph);
  Py_TYPE(self)->tp_free(self);
}

PyObject *PyEdgesIteratorGet(PyEdgesIterator *self, PyObject *Py_UNUSED(ignored)) {
  MG_ASSERT(self->it);
  MG_ASSERT(self->py_graph);
  MG_ASSERT(self->py_graph->graph);
  const auto *edge = mgp_edges_iterator_get(self->it);
  if (!edge) Py_RETURN_NONE;
  return MakePyEdge(*edge, self->py_graph);
}

PyObject *PyEdgesIteratorNext(PyEdgesIterator *self, PyObject *Py_UNUSED(ignored)) {
  MG_ASSERT(self->it);
  MG_ASSERT(self->py_graph);
  MG_ASSERT(self->py_graph->graph);
  const auto *edge = mgp_edges_iterator_next(self->it);
  if (!edge) Py_RETURN_NONE;
  return MakePyEdge(*edge, self->py_graph);
}

static PyMethodDef PyEdgesIteratorMethods[] = {
    {"__reduce__", reinterpret_cast<PyCFunction>(DisallowPickleAndCopy), METH_NOARGS, "__reduce__ is not supported"},
    {"get", reinterpret_cast<PyCFunction>(PyEdgesIteratorGet), METH_NOARGS,
     "Get the current edge pointed to by the iterator or return None."},
    {"next", reinterpret_cast<PyCFunction>(PyEdgesIteratorNext), METH_NOARGS,
     "Advance the iterator to the next edge and return it."},
    {nullptr},
};

// clang-format off
static PyTypeObject PyEdgesIteratorType = {
    PyVarObject_HEAD_INIT(nullptr, 0)
    .tp_name = "_mgp.EdgesIterator",
    .tp_basicsize = sizeof(PyEdgesIterator),
    .tp_dealloc = reinterpret_cast<destructor>(PyEdgesIteratorDealloc),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "Wraps struct mgp_edges_iterator.",
    .tp_methods = PyEdgesIteratorMethods,
};
// clang-format on

PyObject *PyGraphInvalidate(PyGraph *self, PyObject *Py_UNUSED(ignored)) {
  self->graph = nullptr;
  self->memory = nullptr;
  Py_RETURN_NONE;
}

PyObject *PyGraphIsValid(PyGraph *self, PyObject *Py_UNUSED(ignored)) { return PyBool_FromLong(!!self->graph); }

PyObject *MakePyVertex(mgp_vertex *vertex, PyGraph *py_graph);

PyObject *PyGraphGetVertexById(PyGraph *self, PyObject *args) {
  MG_ASSERT(self->graph);
  MG_ASSERT(self->memory);
  static_assert(std::is_same_v<int64_t, long>);
  int64_t id;
  if (!PyArg_ParseTuple(args, "l", &id)) return nullptr;
  auto *vertex = mgp_graph_get_vertex_by_id(self->graph, mgp_vertex_id{id}, self->memory);
  if (!vertex) {
    PyErr_SetString(PyExc_IndexError, "Unable to find the vertex with given ID.");
    return nullptr;
  }
  auto *py_vertex = MakePyVertex(vertex, self);
  if (!py_vertex) mgp_vertex_destroy(vertex);
  return py_vertex;
}

PyObject *PyGraphIterVertices(PyGraph *self, PyObject *Py_UNUSED(ignored)) {
  MG_ASSERT(self->graph);
  MG_ASSERT(self->memory);
  auto *vertices_it = mgp_graph_iter_vertices(self->graph, self->memory);
  if (!vertices_it) {
    PyErr_SetString(PyExc_MemoryError, "Unable to allocate mgp_vertices_iterator.");
    return nullptr;
  }
  auto *py_vertices_it = PyObject_New(PyVerticesIterator, &PyVerticesIteratorType);
  if (!py_vertices_it) {
    mgp_vertices_iterator_destroy(vertices_it);
    return nullptr;
  }
  py_vertices_it->it = vertices_it;
  Py_INCREF(self);
  py_vertices_it->py_graph = self;
  return reinterpret_cast<PyObject *>(py_vertices_it);
}

PyObject *PyGraphMustAbort(PyGraph *self, PyObject *Py_UNUSED(ignored)) {
  MG_ASSERT(self->graph);
  return PyBool_FromLong(mgp_must_abort(self->graph));
}

static PyMethodDef PyGraphMethods[] = {
    {"__reduce__", reinterpret_cast<PyCFunction>(DisallowPickleAndCopy), METH_NOARGS, "__reduce__ is not supported"},
    {"invalidate", reinterpret_cast<PyCFunction>(PyGraphInvalidate), METH_NOARGS,
     "Invalidate the Graph context thus preventing the Graph from being used."},
    {"is_valid", reinterpret_cast<PyCFunction>(PyGraphIsValid), METH_NOARGS,
     "Return True if Graph is in valid context and may be used."},
    {"get_vertex_by_id", reinterpret_cast<PyCFunction>(PyGraphGetVertexById), METH_VARARGS,
     "Get the vertex or raise IndexError."},
    {"iter_vertices", reinterpret_cast<PyCFunction>(PyGraphIterVertices), METH_NOARGS, "Return _mgp.VerticesIterator."},
    {"must_abort", reinterpret_cast<PyCFunction>(PyGraphMustAbort), METH_NOARGS,
     "Check whether the running procedure should abort"},
    {nullptr},
};

// clang-format off
static PyTypeObject PyGraphType = {
    PyVarObject_HEAD_INIT(nullptr, 0)
    .tp_name = "_mgp.Graph",
    .tp_basicsize = sizeof(PyGraph),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "Wraps struct mgp_graph.",
    .tp_methods = PyGraphMethods,
};
// clang-format on

PyObject *MakePyGraph(const mgp_graph *graph, mgp_memory *memory) {
  MG_ASSERT(!graph || (graph && memory));
  auto *py_graph = PyObject_New(PyGraph, &PyGraphType);
  if (!py_graph) return nullptr;
  py_graph->graph = graph;
  py_graph->memory = memory;
  return reinterpret_cast<PyObject *>(py_graph);
}

// clang-format off
struct PyCypherType {
  PyObject_HEAD
  const mgp_type *type;
};
// clang-format on

// clang-format off
static PyTypeObject PyCypherTypeType = {
    PyVarObject_HEAD_INIT(nullptr, 0)
    .tp_name = "_mgp.Type",
    .tp_basicsize = sizeof(PyCypherType),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "Wraps struct mgp_type.",
};
// clang-format on

PyObject *MakePyCypherType(const mgp_type *type) {
  MG_ASSERT(type);
  auto *py_type = PyObject_New(PyCypherType, &PyCypherTypeType);
  if (!py_type) return nullptr;
  py_type->type = type;
  return reinterpret_cast<PyObject *>(py_type);
}

// clang-format off
struct PyQueryProc {
  PyObject_HEAD
  mgp_proc *proc;
};
// clang-format on

PyObject *PyQueryProcAddArg(PyQueryProc *self, PyObject *args) {
  MG_ASSERT(self->proc);
  const char *name = nullptr;
  PyObject *py_type = nullptr;
  if (!PyArg_ParseTuple(args, "sO", &name, &py_type)) return nullptr;
  if (Py_TYPE(py_type) != &PyCypherTypeType) {
    PyErr_SetString(PyExc_TypeError, "Expected a _mgp.Type.");
    return nullptr;
  }
  const auto *type = reinterpret_cast<PyCypherType *>(py_type)->type;
  if (!mgp_proc_add_arg(self->proc, name, type)) {
    PyErr_SetString(PyExc_ValueError, "Invalid call to mgp_proc_add_arg.");
    return nullptr;
  }
  Py_RETURN_NONE;
}

PyObject *PyQueryProcAddOptArg(PyQueryProc *self, PyObject *args) {
  MG_ASSERT(self->proc);
  const char *name = nullptr;
  PyObject *py_type = nullptr;
  PyObject *py_value = nullptr;
  if (!PyArg_ParseTuple(args, "sOO", &name, &py_type, &py_value)) return nullptr;
  if (Py_TYPE(py_type) != &PyCypherTypeType) {
    PyErr_SetString(PyExc_TypeError, "Expected a _mgp.Type.");
    return nullptr;
  }
  const auto *type = reinterpret_cast<PyCypherType *>(py_type)->type;
  mgp_memory memory{self->proc->opt_args.get_allocator().GetMemoryResource()};
  mgp_value *value;
  try {
    value = PyObjectToMgpValue(py_value, &memory);
  } catch (const std::bad_alloc &e) {
    PyErr_SetString(PyExc_MemoryError, e.what());
    return nullptr;
  } catch (const std::overflow_error &e) {
    PyErr_SetString(PyExc_OverflowError, e.what());
    return nullptr;
  } catch (const std::invalid_argument &e) {
    PyErr_SetString(PyExc_ValueError, e.what());
    return nullptr;
  } catch (const std::exception &e) {
    PyErr_SetString(PyExc_RuntimeError, e.what());
    return nullptr;
  }
  MG_ASSERT(value);
  if (!mgp_proc_add_opt_arg(self->proc, name, type, value)) {
    mgp_value_destroy(value);
    PyErr_SetString(PyExc_ValueError, "Invalid call to mgp_proc_add_opt_arg.");
    return nullptr;
  }
  mgp_value_destroy(value);
  Py_RETURN_NONE;
}

PyObject *PyQueryProcAddResult(PyQueryProc *self, PyObject *args) {
  MG_ASSERT(self->proc);
  const char *name = nullptr;
  PyObject *py_type = nullptr;
  if (!PyArg_ParseTuple(args, "sO", &name, &py_type)) return nullptr;
  if (Py_TYPE(py_type) != &PyCypherTypeType) {
    PyErr_SetString(PyExc_TypeError, "Expected a _mgp.Type.");
    return nullptr;
  }
  const auto *type = reinterpret_cast<PyCypherType *>(py_type)->type;
  if (!mgp_proc_add_result(self->proc, name, type)) {
    PyErr_SetString(PyExc_ValueError, "Invalid call to mgp_proc_add_result.");
    return nullptr;
  }
  Py_RETURN_NONE;
}

PyObject *PyQueryProcAddDeprecatedResult(PyQueryProc *self, PyObject *args) {
  MG_ASSERT(self->proc);
  const char *name = nullptr;
  PyObject *py_type = nullptr;
  if (!PyArg_ParseTuple(args, "sO", &name, &py_type)) return nullptr;
  if (Py_TYPE(py_type) != &PyCypherTypeType) {
    PyErr_SetString(PyExc_TypeError, "Expected a _mgp.Type.");
    return nullptr;
  }
  const auto *type = reinterpret_cast<PyCypherType *>(py_type)->type;
  if (!mgp_proc_add_deprecated_result(self->proc, name, type)) {
    PyErr_SetString(PyExc_ValueError, "Invalid call to mgp_proc_add_deprecated_result.");
    return nullptr;
  }
  Py_RETURN_NONE;
}

static PyMethodDef PyQueryProcMethods[] = {
    {"__reduce__", reinterpret_cast<PyCFunction>(DisallowPickleAndCopy), METH_NOARGS, "__reduce__ is not supported"},
    {"add_arg", reinterpret_cast<PyCFunction>(PyQueryProcAddArg), METH_VARARGS,
     "Add a required argument to a procedure."},
    {"add_opt_arg", reinterpret_cast<PyCFunction>(PyQueryProcAddOptArg), METH_VARARGS,
     "Add an optional argument with a default value to a procedure."},
    {"add_result", reinterpret_cast<PyCFunction>(PyQueryProcAddResult), METH_VARARGS,
     "Add a result field to a procedure."},
    {"add_deprecated_result", reinterpret_cast<PyCFunction>(PyQueryProcAddDeprecatedResult), METH_VARARGS,
     "Add a result field to a procedure and mark it as deprecated."},
    {nullptr},
};

// clang-format off
static PyTypeObject PyQueryProcType = {
    PyVarObject_HEAD_INIT(nullptr, 0)
    .tp_name = "_mgp.Proc",
    .tp_basicsize = sizeof(PyQueryProc),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "Wraps struct mgp_proc.",
    .tp_methods = PyQueryProcMethods,
};
// clang-format on

// clang-format off
struct PyQueryModule {
  PyObject_HEAD
  mgp_module *module;
};
// clang-format on

py::Object MgpListToPyTuple(const mgp_list *list, PyGraph *py_graph) {
  MG_ASSERT(list);
  MG_ASSERT(py_graph);
  const size_t len = mgp_list_size(list);
  py::Object py_tuple(PyTuple_New(len));
  if (!py_tuple) return nullptr;
  for (size_t i = 0; i < len; ++i) {
    auto elem = MgpValueToPyObject(*mgp_list_at(list, i), py_graph);
    if (!elem) return nullptr;
    // Explicitly convert `py_tuple`, which is `py::Object`, via static_cast.
    // Then the macro will cast it to `PyTuple *`.
    PyTuple_SET_ITEM(py_tuple.Ptr(), i, elem.Steal());
  }
  return py_tuple;
}

py::Object MgpListToPyTuple(const mgp_list *list, PyObject *py_graph) {
  if (Py_TYPE(py_graph) != &PyGraphType) {
    PyErr_SetString(PyExc_TypeError, "Expected a _mgp.Graph.");
    return nullptr;
  }
  return MgpListToPyTuple(list, reinterpret_cast<PyGraph *>(py_graph));
}

namespace {

std::optional<py::ExceptionInfo> AddRecordFromPython(mgp_result *result, py::Object py_record) {
  py::Object py_mgp(PyImport_ImportModule("mgp"));
  if (!py_mgp) return py::FetchError();
  auto record_cls = py_mgp.GetAttr("Record");
  if (!record_cls) return py::FetchError();
  if (!PyObject_IsInstance(py_record.Ptr(), record_cls.Ptr())) {
    std::stringstream ss;
    ss << "Value '" << py_record << "' is not an instance of 'mgp.Record'";
    const auto &msg = ss.str();
    PyErr_SetString(PyExc_TypeError, msg.c_str());
    return py::FetchError();
  }
  py::Object fields(py_record.GetAttr("fields"));
  if (!fields) return py::FetchError();
  if (!PyDict_Check(fields)) {
    PyErr_SetString(PyExc_TypeError, "Expected 'mgp.Record.fields' to be a 'dict'");
    return py::FetchError();
  }
  py::Object items(PyDict_Items(fields.Ptr()));
  if (!items) return py::FetchError();
  auto *record = mgp_result_new_record(result);
  if (!record) {
    PyErr_NoMemory();
    return py::FetchError();
  }
  Py_ssize_t len = PyList_GET_SIZE(items.Ptr());
  for (Py_ssize_t i = 0; i < len; ++i) {
    auto *item = PyList_GET_ITEM(items.Ptr(), i);
    if (!item) return py::FetchError();
    MG_ASSERT(PyTuple_Check(item));
    auto *key = PyTuple_GetItem(item, 0);
    if (!key) return py::FetchError();
    if (!PyUnicode_Check(key)) {
      std::stringstream ss;
      ss << "Field name '" << py::Object::FromBorrow(key) << "' is not an instance of 'str'";
      const auto &msg = ss.str();
      PyErr_SetString(PyExc_TypeError, msg.c_str());
      return py::FetchError();
    }
    const auto *field_name = PyUnicode_AsUTF8(key);
    if (!field_name) return py::FetchError();
    auto *val = PyTuple_GetItem(item, 1);
    if (!val) return py::FetchError();
    mgp_memory memory{result->rows.get_allocator().GetMemoryResource()};
    mgp_value *field_val{nullptr};
    try {
      // TODO: Make PyObjectToMgpValue set a Python exception instead.
      field_val = PyObjectToMgpValue(val, &memory);
    } catch (const std::exception &e) {
      PyErr_SetString(PyExc_ValueError, e.what());
      return py::FetchError();
    }
    MG_ASSERT(field_val);
    if (!mgp_result_record_insert(record, field_name, field_val)) {
      std::stringstream ss;
      ss << "Unable to insert field '" << py::Object::FromBorrow(key) << "' with value: '"
         << py::Object::FromBorrow(val) << "'; did you set the correct field type?";
      const auto &msg = ss.str();
      PyErr_SetString(PyExc_ValueError, msg.c_str());
      mgp_value_destroy(field_val);
      return py::FetchError();
    }
    mgp_value_destroy(field_val);
  }
  return std::nullopt;
}

std::optional<py::ExceptionInfo> AddMultipleRecordsFromPython(mgp_result *result, py::Object py_seq) {
  Py_ssize_t len = PySequence_Size(py_seq.Ptr());
  if (len == -1) return py::FetchError();
  for (Py_ssize_t i = 0; i < len; ++i) {
    py::Object py_record(PySequence_GetItem(py_seq.Ptr(), i));
    if (!py_record) return py::FetchError();
    auto maybe_exc = AddRecordFromPython(result, py_record);
    if (maybe_exc) return maybe_exc;
  }
  return std::nullopt;
}

void CallPythonProcedure(const py::Object &py_cb, const mgp_list *args, const mgp_graph *graph, mgp_result *result,
                         mgp_memory *memory) {
  auto gil = py::EnsureGIL();

  auto error_to_msg = [](const std::optional<py::ExceptionInfo> &exc_info) -> std::optional<std::string> {
    if (!exc_info) return std::nullopt;
    // Here we tell the traceback formatter to skip the first line of the
    // traceback because that line will always be our wrapper function in our
    // internal `mgp.py` file. With that line skipped, the user will always
    // get only the relevant traceback that happened in his Python code.
    return py::FormatException(*exc_info, /* skip_first_line = */ true);
  };

  auto call = [&](py::Object py_graph) -> std::optional<py::ExceptionInfo> {
    py::Object py_args(MgpListToPyTuple(args, py_graph.Ptr()));
    if (!py_args) return py::FetchError();
    auto py_res = py_cb.Call(py_graph, py_args);
    if (!py_res) return py::FetchError();
    if (PySequence_Check(py_res.Ptr())) {
      return AddMultipleRecordsFromPython(result, py_res);
    } else {
      return AddRecordFromPython(result, py_res);
    }
  };

  auto cleanup = [](py::Object py_graph) {
    // Run `gc.collect` (reference cycle-detection) explicitly, so that we are
    // sure the procedure cleaned up everything it held references to. If the
    // user stored a reference to one of our `_mgp` instances then the
    // internally used `mgp_*` structs will stay unfreed and a memory leak
    // will be reported at the end of the query execution.
    py::Object gc(PyImport_ImportModule("gc"));
    if (!gc) {
      LOG_FATAL(py::FetchError().value());
    }

    if (!gc.CallMethod("collect")) {
      LOG_FATAL(py::FetchError().value());
    }

    // After making sure all references from our side have been cleared,
    // invalidate the `_mgp.Graph` object. If the user kept a reference to one
    // of our `_mgp` instances then this will prevent them from using those
    // objects (whose internal `mgp_*` pointers are now invalid and would cause
    // a crash).
    if (!py_graph.CallMethod("invalidate")) {
      LOG_FATAL(py::FetchError().value());
    }
  };

  // It is *VERY IMPORTANT* to note that this code takes great care not to keep
  // any extra references to any `_mgp` instances (except for `_mgp.Graph`), so
  // as not to introduce extra reference counts and prevent their deallocation.
  // In particular, the `ExceptionInfo` object has a `traceback` field that
  // contains references to the Python frames and their arguments, and therefore
  // our `_mgp` instances as well. Within this code we ensure not to keep the
  // `ExceptionInfo` object alive so that no extra reference counts are
  // introduced. We only fetch the error message and immediately destroy the
  // object.
  std::optional<std::string> maybe_msg;
  {
    py::Object py_graph(MakePyGraph(graph, memory));
    if (py_graph) {
      try {
        maybe_msg = error_to_msg(call(py_graph));
        cleanup(py_graph);
      } catch (...) {
        cleanup(py_graph);
        throw;
      }
    } else {
      maybe_msg = error_to_msg(py::FetchError());
    }
  }

  if (maybe_msg) {
    mgp_result_set_error_msg(result, maybe_msg->c_str());
  }
}

}  // namespace

PyObject *PyQueryModuleAddReadProcedure(PyQueryModule *self, PyObject *cb) {
  MG_ASSERT(self->module);
  if (!PyCallable_Check(cb)) {
    PyErr_SetString(PyExc_TypeError, "Expected a callable object.");
    return nullptr;
  }
  auto py_cb = py::Object::FromBorrow(cb);
  py::Object py_name(py_cb.GetAttr("__name__"));
  const auto *name = PyUnicode_AsUTF8(py_name.Ptr());
  if (!name) return nullptr;
  if (!IsValidIdentifierName(name)) {
    PyErr_SetString(PyExc_ValueError, "Procedure name is not a valid identifier");
    return nullptr;
  }
  auto *memory = self->module->procedures.get_allocator().GetMemoryResource();
  mgp_proc proc(
      name,
      [py_cb](const mgp_list *args, const mgp_graph *graph, mgp_result *result, mgp_memory *memory) {
        CallPythonProcedure(py_cb, args, graph, result, memory);
      },
      memory);
  const auto &[proc_it, did_insert] = self->module->procedures.emplace(name, std::move(proc));
  if (!did_insert) {
    PyErr_SetString(PyExc_ValueError, "Already registered a procedure with the same name.");
    return nullptr;
  }
  auto *py_proc = PyObject_New(PyQueryProc, &PyQueryProcType);
  if (!py_proc) return nullptr;
  py_proc->proc = &proc_it->second;
  return reinterpret_cast<PyObject *>(py_proc);
}

static PyMethodDef PyQueryModuleMethods[] = {
    {"__reduce__", reinterpret_cast<PyCFunction>(DisallowPickleAndCopy), METH_NOARGS, "__reduce__ is not supported"},
    {"add_read_procedure", reinterpret_cast<PyCFunction>(PyQueryModuleAddReadProcedure), METH_O,
     "Register a read-only procedure with this module."},
    {nullptr},
};

// clang-format off
static PyTypeObject PyQueryModuleType = {
    PyVarObject_HEAD_INIT(nullptr, 0)
    .tp_name = "_mgp.Module",
    .tp_basicsize = sizeof(PyQueryModule),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "Wraps struct mgp_module.",
    .tp_methods = PyQueryModuleMethods,
};
// clang-format on

PyObject *MakePyQueryModule(mgp_module *module) {
  MG_ASSERT(module);
  auto *py_query_module = PyObject_New(PyQueryModule, &PyQueryModuleType);
  if (!py_query_module) return nullptr;
  py_query_module->module = module;
  return reinterpret_cast<PyObject *>(py_query_module);
}

PyObject *PyMgpModuleTypeNullable(PyObject *mod, PyObject *obj) {
  if (Py_TYPE(obj) != &PyCypherTypeType) {
    PyErr_SetString(PyExc_TypeError, "Expected a _mgp.Type.");
    return nullptr;
  }
  auto *py_type = reinterpret_cast<PyCypherType *>(obj);
  return MakePyCypherType(mgp_type_nullable(py_type->type));
}

PyObject *PyMgpModuleTypeList(PyObject *mod, PyObject *obj) {
  if (Py_TYPE(obj) != &PyCypherTypeType) {
    PyErr_SetString(PyExc_TypeError, "Expected a _mgp.Type.");
    return nullptr;
  }
  auto *py_type = reinterpret_cast<PyCypherType *>(obj);
  return MakePyCypherType(mgp_type_list(py_type->type));
}

PyObject *PyMgpModuleTypeAny(PyObject *mod, PyObject *Py_UNUSED(ignored)) { return MakePyCypherType(mgp_type_any()); }

PyObject *PyMgpModuleTypeBool(PyObject *mod, PyObject *Py_UNUSED(ignored)) { return MakePyCypherType(mgp_type_bool()); }

PyObject *PyMgpModuleTypeString(PyObject *mod, PyObject *Py_UNUSED(ignored)) {
  return MakePyCypherType(mgp_type_string());
}

PyObject *PyMgpModuleTypeInt(PyObject *mod, PyObject *Py_UNUSED(ignored)) { return MakePyCypherType(mgp_type_int()); }

PyObject *PyMgpModuleTypeFloat(PyObject *mod, PyObject *Py_UNUSED(ignored)) {
  return MakePyCypherType(mgp_type_float());
}

PyObject *PyMgpModuleTypeNumber(PyObject *mod, PyObject *Py_UNUSED(ignored)) {
  return MakePyCypherType(mgp_type_number());
}

PyObject *PyMgpModuleTypeMap(PyObject *mod, PyObject *Py_UNUSED(ignored)) { return MakePyCypherType(mgp_type_map()); }

PyObject *PyMgpModuleTypeNode(PyObject *mod, PyObject *Py_UNUSED(ignored)) { return MakePyCypherType(mgp_type_node()); }

PyObject *PyMgpModuleTypeRelationship(PyObject *mod, PyObject *Py_UNUSED(ignored)) {
  return MakePyCypherType(mgp_type_relationship());
}

PyObject *PyMgpModuleTypePath(PyObject *mod, PyObject *Py_UNUSED(ignored)) { return MakePyCypherType(mgp_type_path()); }

static PyMethodDef PyMgpModuleMethods[] = {
    {"type_nullable", PyMgpModuleTypeNullable, METH_O,
     "Build a type representing either a `null` value or a value of given "
     "type."},
    {"type_list", PyMgpModuleTypeList, METH_O, "Build a type representing a list of values of given type."},
    {"type_any", PyMgpModuleTypeAny, METH_NOARGS, "Get the type representing any value that isn't `null`."},
    {"type_bool", PyMgpModuleTypeBool, METH_NOARGS, "Get the type representing boolean values."},
    {"type_string", PyMgpModuleTypeString, METH_NOARGS, "Get the type representing string values."},
    {"type_int", PyMgpModuleTypeInt, METH_NOARGS, "Get the type representing integer values."},
    {"type_float", PyMgpModuleTypeFloat, METH_NOARGS, "Get the type representing floating-point values."},
    {"type_number", PyMgpModuleTypeNumber, METH_NOARGS, "Get the type representing any number value."},
    {"type_map", PyMgpModuleTypeMap, METH_NOARGS, "Get the type representing map values."},
    {"type_node", PyMgpModuleTypeNode, METH_NOARGS, "Get the type representing graph node values."},
    {"type_relationship", PyMgpModuleTypeRelationship, METH_NOARGS,
     "Get the type representing graph relationship values."},
    {"type_path", PyMgpModuleTypePath, METH_NOARGS,
     "Get the type representing a graph path (walk) from one node to another."},
    {nullptr},
};

// clang-format off
static PyModuleDef PyMgpModule = {
    PyModuleDef_HEAD_INIT,
    .m_name = "_mgp",
    .m_doc = "Contains raw bindings to mg_procedure.h C API.",
    .m_size = -1,
    .m_methods = PyMgpModuleMethods,
};
// clang-format on

// clang-format off
struct PyPropertiesIterator {
  PyObject_HEAD
  mgp_properties_iterator *it;
  PyGraph *py_graph;
};
// clang-format on

void PyPropertiesIteratorDealloc(PyPropertiesIterator *self) {
  MG_ASSERT(self->it);
  MG_ASSERT(self->py_graph);
  // Avoid invoking `mgp_properties_iterator_destroy` if we are not in valid
  // execution context. The query execution should free all memory used during
  // execution, so we may cause a double free issue.
  if (self->py_graph->graph) mgp_properties_iterator_destroy(self->it);
  Py_DECREF(self->py_graph);
  Py_TYPE(self)->tp_free(self);
}

PyObject *PyPropertiesIteratorGet(PyPropertiesIterator *self, PyObject *Py_UNUSED(ignored)) {
  MG_ASSERT(self->it);
  MG_ASSERT(self->py_graph);
  MG_ASSERT(self->py_graph->graph);
  const auto *property = mgp_properties_iterator_get(self->it);
  if (!property) Py_RETURN_NONE;
  py::Object py_name(PyUnicode_FromString(property->name));
  if (!py_name) return nullptr;
  auto py_value = MgpValueToPyObject(*property->value, self->py_graph);
  if (!py_value) return nullptr;
  return PyTuple_Pack(2, py_name.Ptr(), py_value.Ptr());
}

PyObject *PyPropertiesIteratorNext(PyPropertiesIterator *self, PyObject *Py_UNUSED(ignored)) {
  MG_ASSERT(self->it);
  MG_ASSERT(self->py_graph);
  MG_ASSERT(self->py_graph->graph);
  const auto *property = mgp_properties_iterator_next(self->it);
  if (!property) Py_RETURN_NONE;
  py::Object py_name(PyUnicode_FromString(property->name));
  if (!py_name) return nullptr;
  auto py_value = MgpValueToPyObject(*property->value, self->py_graph);
  if (!py_value) return nullptr;
  return PyTuple_Pack(2, py_name.Ptr(), py_value.Ptr());
}

static PyMethodDef PyPropertiesIteratorMethods[] = {
    {"get", reinterpret_cast<PyCFunction>(PyPropertiesIteratorGet), METH_NOARGS,
     "Get the current proprety pointed to by the iterator or return None."},
    {"next", reinterpret_cast<PyCFunction>(PyPropertiesIteratorNext), METH_NOARGS,
     "Advance the iterator to the next property and return it."},
    {nullptr},
};

// clang-format off
static PyTypeObject PyPropertiesIteratorType = {
    PyVarObject_HEAD_INIT(nullptr, 0)
    .tp_name = "_mgp.PropertiesIterator",
    .tp_basicsize = sizeof(PyPropertiesIterator),
    .tp_dealloc = reinterpret_cast<destructor>(PyPropertiesIteratorDealloc),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "Wraps struct mgp_properties_iterator.",
    .tp_methods = PyPropertiesIteratorMethods,
};
// clang-format on

// clang-format off
struct PyEdge {
  PyObject_HEAD
  mgp_edge *edge;
  PyGraph *py_graph;
};
// clang-format on

PyObject *PyEdgeGetTypeName(PyEdge *self, PyObject *Py_UNUSED(ignored)) {
  MG_ASSERT(self);
  MG_ASSERT(self->edge);
  MG_ASSERT(self->py_graph);
  MG_ASSERT(self->py_graph->graph);
  return PyUnicode_FromString(mgp_edge_get_type(self->edge).name);
}

PyObject *PyEdgeFromVertex(PyEdge *self, PyObject *Py_UNUSED(ignored)) {
  MG_ASSERT(self);
  MG_ASSERT(self->edge);
  MG_ASSERT(self->py_graph);
  MG_ASSERT(self->py_graph->graph);
  const auto *vertex = mgp_edge_get_from(self->edge);
  MG_ASSERT(vertex);
  return MakePyVertex(*vertex, self->py_graph);
}

PyObject *PyEdgeToVertex(PyEdge *self, PyObject *Py_UNUSED(ignored)) {
  MG_ASSERT(self);
  MG_ASSERT(self->edge);
  MG_ASSERT(self->py_graph);
  MG_ASSERT(self->py_graph->graph);
  const auto *vertex = mgp_edge_get_to(self->edge);
  MG_ASSERT(vertex);
  return MakePyVertex(*vertex, self->py_graph);
}

void PyEdgeDealloc(PyEdge *self) {
  MG_ASSERT(self->edge);
  MG_ASSERT(self->py_graph);
  // Avoid invoking `mgp_edge_destroy` if we are not in valid execution context.
  // The query execution should free all memory used during execution, so we may
  // cause a double free issue.
  if (self->py_graph->graph) mgp_edge_destroy(self->edge);
  Py_DECREF(self->py_graph);
  Py_TYPE(self)->tp_free(self);
}

PyObject *PyEdgeIsValid(PyEdge *self, PyObject *Py_UNUSED(ignored)) {
  return PyBool_FromLong(self->py_graph && self->py_graph->graph);
}

PyObject *PyEdgeGetId(PyEdge *self, PyObject *Py_UNUSED(ignored)) {
  MG_ASSERT(self);
  MG_ASSERT(self->edge);
  MG_ASSERT(self->py_graph);
  MG_ASSERT(self->py_graph->graph);
  return PyLong_FromLongLong(mgp_edge_get_id(self->edge).as_int);
}

PyObject *PyEdgeIterProperties(PyEdge *self, PyObject *Py_UNUSED(ignored)) {
  MG_ASSERT(self);
  MG_ASSERT(self->edge);
  MG_ASSERT(self->py_graph);
  MG_ASSERT(self->py_graph->graph);
  auto *properties_it = mgp_edge_iter_properties(self->edge, self->py_graph->memory);
  if (!properties_it) {
    PyErr_SetString(PyExc_MemoryError, "Unable to allocate mgp_properties_iterator.");
    return nullptr;
  }
  auto *py_properties_it = PyObject_New(PyPropertiesIterator, &PyPropertiesIteratorType);
  if (!py_properties_it) {
    mgp_properties_iterator_destroy(properties_it);
    return nullptr;
  }
  py_properties_it->it = properties_it;
  Py_INCREF(self->py_graph);
  py_properties_it->py_graph = self->py_graph;
  return reinterpret_cast<PyObject *>(py_properties_it);
}

PyObject *PyEdgeGetProperty(PyEdge *self, PyObject *args) {
  MG_ASSERT(self);
  MG_ASSERT(self->edge);
  MG_ASSERT(self->py_graph);
  MG_ASSERT(self->py_graph->graph);
  const char *prop_name = nullptr;
  if (!PyArg_ParseTuple(args, "s", &prop_name)) return nullptr;
  auto *prop_value = mgp_edge_get_property(self->edge, prop_name, self->py_graph->memory);
  if (!prop_value) {
    PyErr_SetString(PyExc_MemoryError, "Unable to allocate mgp_value for edge property value.");
    return nullptr;
  }
  auto py_prop_value = MgpValueToPyObject(*prop_value, self->py_graph);
  mgp_value_destroy(prop_value);
  return py_prop_value.Steal();
}

static PyMethodDef PyEdgeMethods[] = {
    {"__reduce__", reinterpret_cast<PyCFunction>(DisallowPickleAndCopy), METH_NOARGS, "__reduce__ is not supported."},
    {"is_valid", reinterpret_cast<PyCFunction>(PyEdgeIsValid), METH_NOARGS,
     "Return True if Edge is in valid context and may be used."},
    {"get_id", reinterpret_cast<PyCFunction>(PyEdgeGetId), METH_NOARGS, "Return edge id."},
    {"get_type_name", reinterpret_cast<PyCFunction>(PyEdgeGetTypeName), METH_NOARGS, "Return the edge's type name."},
    {"from_vertex", reinterpret_cast<PyCFunction>(PyEdgeFromVertex), METH_NOARGS, "Return the edge's source vertex."},
    {"to_vertex", reinterpret_cast<PyCFunction>(PyEdgeToVertex), METH_NOARGS, "Return the edge's destination vertex."},
    {"iter_properties", reinterpret_cast<PyCFunction>(PyEdgeIterProperties), METH_NOARGS,
     "Return _mgp.PropertiesIterator for this edge."},
    {"get_property", reinterpret_cast<PyCFunction>(PyEdgeGetProperty), METH_VARARGS,
     "Return edge property with given name."},
    {nullptr},
};

PyObject *PyEdgeRichCompare(PyObject *self, PyObject *other, int op);

// clang-format off
static PyTypeObject PyEdgeType = {
    PyVarObject_HEAD_INIT(nullptr, 0)
    .tp_name = "_mgp.Edge",
    .tp_basicsize = sizeof(PyEdge),
    .tp_dealloc = reinterpret_cast<destructor>(PyEdgeDealloc),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "Wraps struct mgp_edge.",
    .tp_richcompare = PyEdgeRichCompare,
    .tp_methods = PyEdgeMethods,
};
// clang-format on

/// Create an instance of `_mgp.Edge` class.
///
/// The created instance references an existing `_mgp.Graph` instance, which
/// marks the execution context.
PyObject *MakePyEdge(const mgp_edge &edge, PyGraph *py_graph) {
  MG_ASSERT(py_graph);
  MG_ASSERT(py_graph->graph && py_graph->memory);
  auto *edge_copy = mgp_edge_copy(&edge, py_graph->memory);
  if (!edge_copy) {
    PyErr_SetString(PyExc_MemoryError, "Unable to allocate mgp_edge.");
    return nullptr;
  }
  auto *py_edge = PyObject_New(PyEdge, &PyEdgeType);
  if (!py_edge) {
    mgp_edge_destroy(edge_copy);
    return nullptr;
  }
  py_edge->edge = edge_copy;
  py_edge->py_graph = py_graph;
  Py_INCREF(py_graph);
  return reinterpret_cast<PyObject *>(py_edge);
}

PyObject *PyEdgeRichCompare(PyObject *self, PyObject *other, int op) {
  MG_ASSERT(self);
  MG_ASSERT(other);

  if (Py_TYPE(self) != &PyEdgeType || Py_TYPE(other) != &PyEdgeType || op != Py_EQ) {
    Py_RETURN_NOTIMPLEMENTED;
  }

  auto *e1 = reinterpret_cast<PyEdge *>(self);
  auto *e2 = reinterpret_cast<PyEdge *>(other);
  MG_ASSERT(e1->edge);
  MG_ASSERT(e2->edge);

  return PyBool_FromLong(mgp_edge_equal(e1->edge, e2->edge));
}

// clang-format off
struct PyVertex {
  PyObject_HEAD
  mgp_vertex *vertex;
  PyGraph *py_graph;
};
// clang-format on

void PyVertexDealloc(PyVertex *self) {
  MG_ASSERT(self->vertex);
  MG_ASSERT(self->py_graph);
  // Avoid invoking `mgp_vertex_destroy` if we are not in valid execution
  // context. The query execution should free all memory used during
  // execution, so  we may cause a double free issue.
  if (self->py_graph->graph) mgp_vertex_destroy(self->vertex);
  Py_DECREF(self->py_graph);
  Py_TYPE(self)->tp_free(self);
}

PyObject *PyVertexIsValid(PyVertex *self, PyObject *Py_UNUSED(ignored)) {
  return PyBool_FromLong(self->py_graph && self->py_graph->graph);
}

PyObject *PyVertexGetId(PyVertex *self, PyObject *Py_UNUSED(ignored)) {
  MG_ASSERT(self);
  MG_ASSERT(self->vertex);
  MG_ASSERT(self->py_graph);
  MG_ASSERT(self->py_graph->graph);
  return PyLong_FromLongLong(mgp_vertex_get_id(self->vertex).as_int);
}

PyObject *PyVertexLabelsCount(PyVertex *self, PyObject *Py_UNUSED(ignored)) {
  MG_ASSERT(self);
  MG_ASSERT(self->vertex);
  MG_ASSERT(self->py_graph);
  MG_ASSERT(self->py_graph->graph);
  return PyLong_FromSize_t(mgp_vertex_labels_count(self->vertex));
}

PyObject *PyVertexLabelAt(PyVertex *self, PyObject *args) {
  MG_ASSERT(self);
  MG_ASSERT(self->vertex);
  MG_ASSERT(self->py_graph);
  MG_ASSERT(self->py_graph->graph);
  static_assert(std::numeric_limits<Py_ssize_t>::max() <= std::numeric_limits<size_t>::max());
  Py_ssize_t id;
  if (!PyArg_ParseTuple(args, "n", &id)) return nullptr;
  auto label = mgp_vertex_label_at(self->vertex, id);
  if (label.name == nullptr || id < 0) {
    PyErr_SetString(PyExc_IndexError, "Unable to find the label with given ID.");
    return nullptr;
  }
  return PyUnicode_FromString(label.name);
}

PyObject *PyVertexIterInEdges(PyVertex *self, PyObject *Py_UNUSED(ignored)) {
  MG_ASSERT(self);
  MG_ASSERT(self->vertex);
  MG_ASSERT(self->py_graph);
  MG_ASSERT(self->py_graph->graph);
  auto *edges_it = mgp_vertex_iter_in_edges(self->vertex, self->py_graph->memory);
  if (!edges_it) {
    PyErr_SetString(PyExc_MemoryError, "Unable to allocate mgp_edges_iterator for in edges.");
    return nullptr;
  }
  auto *py_edges_it = PyObject_New(PyEdgesIterator, &PyEdgesIteratorType);
  if (!py_edges_it) {
    mgp_edges_iterator_destroy(edges_it);
    return nullptr;
  }
  py_edges_it->it = edges_it;
  Py_INCREF(self->py_graph);
  py_edges_it->py_graph = self->py_graph;
  return reinterpret_cast<PyObject *>(py_edges_it);
}

PyObject *PyVertexIterOutEdges(PyVertex *self, PyObject *Py_UNUSED(ignored)) {
  MG_ASSERT(self);
  MG_ASSERT(self->vertex);
  MG_ASSERT(self->py_graph);
  MG_ASSERT(self->py_graph->graph);
  auto *edges_it = mgp_vertex_iter_out_edges(self->vertex, self->py_graph->memory);
  if (!edges_it) {
    PyErr_SetString(PyExc_MemoryError, "Unable to allocate mgp_edges_iterator for out edges.");
    return nullptr;
  }
  auto *py_edges_it = PyObject_New(PyEdgesIterator, &PyEdgesIteratorType);
  if (!py_edges_it) {
    mgp_edges_iterator_destroy(edges_it);
    return nullptr;
  }
  py_edges_it->it = edges_it;
  Py_INCREF(self->py_graph);
  py_edges_it->py_graph = self->py_graph;
  return reinterpret_cast<PyObject *>(py_edges_it);
}

PyObject *PyVertexIterProperties(PyVertex *self, PyObject *Py_UNUSED(ignored)) {
  MG_ASSERT(self);
  MG_ASSERT(self->vertex);
  MG_ASSERT(self->py_graph);
  MG_ASSERT(self->py_graph->graph);
  auto *properties_it = mgp_vertex_iter_properties(self->vertex, self->py_graph->memory);
  if (!properties_it) {
    PyErr_SetString(PyExc_MemoryError, "Unable to allocate mgp_properties_iterator.");
    return nullptr;
  }
  auto *py_properties_it = PyObject_New(PyPropertiesIterator, &PyPropertiesIteratorType);
  if (!py_properties_it) {
    mgp_properties_iterator_destroy(properties_it);
    return nullptr;
  }
  py_properties_it->it = properties_it;
  Py_INCREF(self->py_graph);
  py_properties_it->py_graph = self->py_graph;
  return reinterpret_cast<PyObject *>(py_properties_it);
}

PyObject *PyVertexGetProperty(PyVertex *self, PyObject *args) {
  MG_ASSERT(self);
  MG_ASSERT(self->vertex);
  MG_ASSERT(self->py_graph);
  MG_ASSERT(self->py_graph->graph);
  const char *prop_name = nullptr;
  if (!PyArg_ParseTuple(args, "s", &prop_name)) return nullptr;
  auto *prop_value = mgp_vertex_get_property(self->vertex, prop_name, self->py_graph->memory);
  if (!prop_value) {
    PyErr_SetString(PyExc_MemoryError, "Unable to allocate mgp_value for vertex property value.");
    return nullptr;
  }
  auto py_prop_value = MgpValueToPyObject(*prop_value, self->py_graph);
  mgp_value_destroy(prop_value);
  return py_prop_value.Steal();
}

static PyMethodDef PyVertexMethods[] = {
    {"__reduce__", reinterpret_cast<PyCFunction>(DisallowPickleAndCopy), METH_NOARGS, "__reduce__ is not supported."},
    {"is_valid", reinterpret_cast<PyCFunction>(PyVertexIsValid), METH_NOARGS,
     "Return True if Vertex is in valid context and may be used."},
    {"get_id", reinterpret_cast<PyCFunction>(PyVertexGetId), METH_NOARGS, "Return vertex id."},
    {"labels_count", reinterpret_cast<PyCFunction>(PyVertexLabelsCount), METH_NOARGS,
     "Return number of lables of a vertex."},
    {"label_at", reinterpret_cast<PyCFunction>(PyVertexLabelAt), METH_VARARGS,
     "Return label of a vertex on a given index."},
    {"iter_in_edges", reinterpret_cast<PyCFunction>(PyVertexIterInEdges), METH_NOARGS,
     "Return _mgp.EdgesIterator for in edges."},
    {"iter_out_edges", reinterpret_cast<PyCFunction>(PyVertexIterOutEdges), METH_NOARGS,
     "Return _mgp.EdgesIterator for out edges."},
    {"iter_properties", reinterpret_cast<PyCFunction>(PyVertexIterProperties), METH_NOARGS,
     "Return _mgp.PropertiesIterator for this vertex."},
    {"get_property", reinterpret_cast<PyCFunction>(PyVertexGetProperty), METH_VARARGS,
     "Return vertex property with given name."},
    {nullptr},
};

PyObject *PyVertexRichCompare(PyObject *self, PyObject *other, int op);

// clang-format off
static PyTypeObject PyVertexType = {
    PyVarObject_HEAD_INIT(nullptr, 0)
    .tp_name = "_mgp.Vertex",
    .tp_basicsize = sizeof(PyVertex),
    .tp_dealloc = reinterpret_cast<destructor>(PyVertexDealloc),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "Wraps struct mgp_vertex.",
    .tp_richcompare = PyVertexRichCompare,
    .tp_methods = PyVertexMethods,
};
// clang-format on

PyObject *MakePyVertex(mgp_vertex *vertex, PyGraph *py_graph) {
  MG_ASSERT(vertex);
  MG_ASSERT(py_graph);
  MG_ASSERT(py_graph->graph && py_graph->memory);
  MG_ASSERT(vertex->GetMemoryResource() == py_graph->memory->impl);
  auto *py_vertex = PyObject_New(PyVertex, &PyVertexType);
  if (!py_vertex) return nullptr;
  py_vertex->vertex = vertex;
  py_vertex->py_graph = py_graph;
  Py_INCREF(py_graph);
  return reinterpret_cast<PyObject *>(py_vertex);
}

PyObject *MakePyVertex(const mgp_vertex &vertex, PyGraph *py_graph) {
  MG_ASSERT(py_graph);
  MG_ASSERT(py_graph->graph && py_graph->memory);
  auto *vertex_copy = mgp_vertex_copy(&vertex, py_graph->memory);
  if (!vertex_copy) {
    PyErr_SetString(PyExc_MemoryError, "Unable to allocate mgp_vertex.");
    return nullptr;
  }
  auto *py_vertex = MakePyVertex(vertex_copy, py_graph);
  if (!py_vertex) mgp_vertex_destroy(vertex_copy);
  return py_vertex;
}

PyObject *PyVertexRichCompare(PyObject *self, PyObject *other, int op) {
  MG_ASSERT(self);
  MG_ASSERT(other);

  if (Py_TYPE(self) != &PyVertexType || Py_TYPE(other) != &PyVertexType || op != Py_EQ) {
    Py_RETURN_NOTIMPLEMENTED;
  }

  auto *v1 = reinterpret_cast<PyVertex *>(self);
  auto *v2 = reinterpret_cast<PyVertex *>(other);
  MG_ASSERT(v1->vertex);
  MG_ASSERT(v2->vertex);

  return PyBool_FromLong(mgp_vertex_equal(v1->vertex, v2->vertex));
}

// clang-format off
struct PyPath {
  PyObject_HEAD
  mgp_path *path;
  PyGraph *py_graph;
};
// clang-format on

void PyPathDealloc(PyPath *self) {
  MG_ASSERT(self->path);
  MG_ASSERT(self->py_graph);
  // Avoid invoking `mgp_path_destroy` if we are not in valid execution
  // context. The query execution should free all memory used during
  // execution, so  we may cause a double free issue.
  if (self->py_graph->graph) mgp_path_destroy(self->path);
  Py_DECREF(self->py_graph);
  Py_TYPE(self)->tp_free(self);
}

PyObject *PyPathIsValid(PyPath *self, PyObject *Py_UNUSED(ignored)) {
  return PyBool_FromLong(self->py_graph && self->py_graph->graph);
}

PyObject *PyPathMakeWithStart(PyTypeObject *type, PyObject *vertex);

PyObject *PyPathExpand(PyPath *self, PyObject *edge) {
  MG_ASSERT(self->path);
  MG_ASSERT(self->py_graph);
  MG_ASSERT(self->py_graph->graph);
  if (Py_TYPE(edge) != &PyEdgeType) {
    PyErr_SetString(PyExc_TypeError, "Expected a _mgp.Edge.");
    return nullptr;
  }
  auto *py_edge = reinterpret_cast<PyEdge *>(edge);
  const auto *to = mgp_edge_get_to(py_edge->edge);
  const auto *from = mgp_edge_get_from(py_edge->edge);
  const auto *last_vertex = mgp_path_vertex_at(self->path, mgp_path_size(self->path));
  if (!mgp_vertex_equal(last_vertex, to) && !mgp_vertex_equal(last_vertex, from)) {
    PyErr_SetString(PyExc_ValueError, "Edge is not a continuation of the path.");
    return nullptr;
  }
  if (!mgp_path_expand(self->path, py_edge->edge)) {
    PyErr_SetString(PyExc_MemoryError, "Unable to expand mgp_path.");
    return nullptr;
  }
  Py_RETURN_NONE;
}

PyObject *PyPathSize(PyPath *self, PyObject *Py_UNUSED(ignored)) {
  MG_ASSERT(self->path);
  MG_ASSERT(self->py_graph);
  MG_ASSERT(self->py_graph->graph);
  return PyLong_FromSize_t(mgp_path_size(self->path));
}

PyObject *PyPathVertexAt(PyPath *self, PyObject *args) {
  MG_ASSERT(self->path);
  MG_ASSERT(self->py_graph);
  MG_ASSERT(self->py_graph->graph);
  static_assert(std::numeric_limits<Py_ssize_t>::max() <= std::numeric_limits<size_t>::max());
  Py_ssize_t i;
  if (!PyArg_ParseTuple(args, "n", &i)) return nullptr;
  const auto *vertex = mgp_path_vertex_at(self->path, i);
  if (!vertex) {
    PyErr_SetString(PyExc_IndexError, "Index is out of range.");
    return nullptr;
  }
  return MakePyVertex(*vertex, self->py_graph);
}

PyObject *PyPathEdgeAt(PyPath *self, PyObject *args) {
  MG_ASSERT(self->path);
  MG_ASSERT(self->py_graph);
  MG_ASSERT(self->py_graph->graph);
  static_assert(std::numeric_limits<Py_ssize_t>::max() <= std::numeric_limits<size_t>::max());
  Py_ssize_t i;
  if (!PyArg_ParseTuple(args, "n", &i)) return nullptr;
  const auto *edge = mgp_path_edge_at(self->path, i);
  if (!edge) {
    PyErr_SetString(PyExc_IndexError, "Index is out of range.");
    return nullptr;
  }
  return MakePyEdge(*edge, self->py_graph);
}

static PyMethodDef PyPathMethods[] = {
    {"__reduce__", reinterpret_cast<PyCFunction>(DisallowPickleAndCopy), METH_NOARGS, "__reduce__ is not supported"},
    {"is_valid", reinterpret_cast<PyCFunction>(PyPathIsValid), METH_NOARGS,
     "Return True if Path is in valid context and may be used."},
    {"make_with_start", reinterpret_cast<PyCFunction>(PyPathMakeWithStart), METH_O | METH_CLASS,
     "Create a path with a starting vertex."},
    {"expand", reinterpret_cast<PyCFunction>(PyPathExpand), METH_O,
     "Append an edge continuing from the last vertex on the path."},
    {"size", reinterpret_cast<PyCFunction>(PyPathSize), METH_NOARGS, "Return the number of edges in a mgp_path."},
    {"vertex_at", reinterpret_cast<PyCFunction>(PyPathVertexAt), METH_VARARGS,
     "Return the vertex from a path at given index."},
    {"edge_at", reinterpret_cast<PyCFunction>(PyPathEdgeAt), METH_VARARGS,
     "Return the edge from a path at given index."},
    {nullptr},
};

// clang-format off
static PyTypeObject PyPathType = {
    PyVarObject_HEAD_INIT(nullptr, 0)
    .tp_name = "_mgp.Path",
    .tp_basicsize = sizeof(PyPath),
    .tp_dealloc = reinterpret_cast<destructor>(PyPathDealloc),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "Wraps struct mgp_path.",
    .tp_methods = PyPathMethods,
};
// clang-format on

PyObject *MakePyPath(mgp_path *path, PyGraph *py_graph) {
  MG_ASSERT(path);
  MG_ASSERT(py_graph->graph && py_graph->memory);
  MG_ASSERT(path->GetMemoryResource() == py_graph->memory->impl);
  auto *py_path = PyObject_New(PyPath, &PyPathType);
  if (!py_path) return nullptr;
  py_path->path = path;
  py_path->py_graph = py_graph;
  Py_INCREF(py_graph);
  return reinterpret_cast<PyObject *>(py_path);
}

PyObject *MakePyPath(const mgp_path &path, PyGraph *py_graph) {
  MG_ASSERT(py_graph);
  MG_ASSERT(py_graph->graph && py_graph->memory);
  auto *path_copy = mgp_path_copy(&path, py_graph->memory);
  if (!path_copy) {
    PyErr_SetString(PyExc_MemoryError, "Unable to allocate mgp_path.");
    return nullptr;
  }
  auto *py_path = MakePyPath(path_copy, py_graph);
  if (!py_path) mgp_path_destroy(path_copy);
  return py_path;
}

PyObject *PyPathMakeWithStart(PyTypeObject *type, PyObject *vertex) {
  if (type != &PyPathType) {
    PyErr_SetString(PyExc_TypeError, "Expected '<class _mgp.Path>' as the first argument.");
    return nullptr;
  }
  if (Py_TYPE(vertex) != &PyVertexType) {
    PyErr_SetString(PyExc_TypeError, "Expected a '_mgp.Vertex' as the second argument.");
    return nullptr;
  }
  auto *py_vertex = reinterpret_cast<PyVertex *>(vertex);
  auto *path = mgp_path_make_with_start(py_vertex->vertex, py_vertex->py_graph->memory);
  if (!path) {
    PyErr_SetString(PyExc_MemoryError, "Unable to allocate mgp_path.");
    return nullptr;
  }
  auto *py_path = MakePyPath(path, py_vertex->py_graph);
  if (!py_path) mgp_path_destroy(path);
  return py_path;
}

PyObject *PyInitMgpModule() {
  PyObject *mgp = PyModule_Create(&PyMgpModule);
  if (!mgp) return nullptr;
  auto register_type = [mgp](auto *type, const auto *name) -> bool {
    if (PyType_Ready(type) < 0) {
      Py_DECREF(mgp);
      return false;
    }
    Py_INCREF(type);
    if (PyModule_AddObject(mgp, name, reinterpret_cast<PyObject *>(type)) < 0) {
      Py_DECREF(type);
      Py_DECREF(mgp);
      return false;
    }
    return true;
  };
  if (!register_type(&PyPropertiesIteratorType, "PropertiesIterator")) return nullptr;
  if (!register_type(&PyVerticesIteratorType, "VerticesIterator")) return nullptr;
  if (!register_type(&PyEdgesIteratorType, "EdgesIterator")) return nullptr;
  if (!register_type(&PyGraphType, "Graph")) return nullptr;
  if (!register_type(&PyEdgeType, "Edge")) return nullptr;
  if (!register_type(&PyQueryProcType, "Proc")) return nullptr;
  if (!register_type(&PyQueryModuleType, "Module")) return nullptr;
  if (!register_type(&PyVertexType, "Vertex")) return nullptr;
  if (!register_type(&PyPathType, "Path")) return nullptr;
  if (!register_type(&PyCypherTypeType, "Type")) return nullptr;
  Py_INCREF(Py_None);
  if (PyModule_AddObject(mgp, "_MODULE", Py_None) < 0) {
    Py_DECREF(Py_None);
    Py_DECREF(mgp);
    return nullptr;
  }
  return mgp;
}

namespace {

template <class TFun>
auto WithMgpModule(mgp_module *module_def, const TFun &fun) {
  py::Object py_mgp(PyImport_ImportModule("_mgp"));
  MG_ASSERT(py_mgp, "Expected builtin '_mgp' to be available for import");
  py::Object py_mgp_module(py_mgp.GetAttr("_MODULE"));
  MG_ASSERT(py_mgp_module, "Expected '_mgp' to have attribute '_MODULE'");
  // NOTE: This check is not thread safe, but this should only go through
  // ModuleRegistry::LoadModuleLibrary which ought to serialize loading.
  MG_ASSERT(py_mgp_module.Ptr() == Py_None,
            "Expected '_mgp._MODULE' to be None as we are just starting to "
            "import a new module. Is some other thread also importing Python "
            "modules?");
  auto *py_query_module = MakePyQueryModule(module_def);
  MG_ASSERT(py_query_module);
  MG_ASSERT(py_mgp.SetAttr("_MODULE", py_query_module));
  auto ret = fun();
  auto maybe_exc = py::FetchError();
  MG_ASSERT(py_mgp.SetAttr("_MODULE", Py_None));
  if (maybe_exc) {
    py::RestoreError(*maybe_exc);
  }
  return ret;
}

}  // namespace

py::Object ImportPyModule(const char *name, mgp_module *module_def) {
  return WithMgpModule(module_def, [name]() { return py::Object(PyImport_ImportModule(name)); });
}

py::Object ReloadPyModule(PyObject *py_module, mgp_module *module_def) {
  return WithMgpModule(module_def, [py_module]() { return py::Object(PyImport_ReloadModule(py_module)); });
}

py::Object MgpValueToPyObject(const mgp_value &value, PyObject *py_graph) {
  if (Py_TYPE(py_graph) != &PyGraphType) {
    PyErr_SetString(PyExc_TypeError, "Expected a _mgp.Graph.");
    return nullptr;
  }
  return MgpValueToPyObject(value, reinterpret_cast<PyGraph *>(py_graph));
}

py::Object MgpValueToPyObject(const mgp_value &value, PyGraph *py_graph) {
  switch (mgp_value_get_type(&value)) {
    case MGP_VALUE_TYPE_NULL:
      Py_INCREF(Py_None);
      return py::Object(Py_None);
    case MGP_VALUE_TYPE_BOOL:
      return py::Object(PyBool_FromLong(mgp_value_get_bool(&value)));
    case MGP_VALUE_TYPE_INT:
      return py::Object(PyLong_FromLongLong(mgp_value_get_int(&value)));
    case MGP_VALUE_TYPE_DOUBLE:
      return py::Object(PyFloat_FromDouble(mgp_value_get_double(&value)));
    case MGP_VALUE_TYPE_STRING:
      return py::Object(PyUnicode_FromString(mgp_value_get_string(&value)));
    case MGP_VALUE_TYPE_LIST:
      return MgpListToPyTuple(mgp_value_get_list(&value), py_graph);
    case MGP_VALUE_TYPE_MAP: {
      const auto *map = mgp_value_get_map(&value);
      py::Object py_dict(PyDict_New());
      if (!py_dict) return nullptr;
      for (const auto &[key, val] : map->items) {
        auto py_val = MgpValueToPyObject(val, py_graph);
        if (!py_val) return nullptr;
        // Unlike PyList_SET_ITEM, PyDict_SetItem does not steal the value.
        if (PyDict_SetItemString(py_dict.Ptr(), key.c_str(), py_val.Ptr()) != 0) return nullptr;
      }
      return py_dict;
    }
    case MGP_VALUE_TYPE_VERTEX: {
      py::Object py_mgp(PyImport_ImportModule("mgp"));
      if (!py_mgp) return nullptr;
      const auto *v = mgp_value_get_vertex(&value);
      py::Object py_vertex(reinterpret_cast<PyObject *>(MakePyVertex(*v, py_graph)));
      return py_mgp.CallMethod("Vertex", py_vertex);
    }
    case MGP_VALUE_TYPE_EDGE: {
      py::Object py_mgp(PyImport_ImportModule("mgp"));
      if (!py_mgp) return nullptr;
      const auto *e = mgp_value_get_edge(&value);
      py::Object py_edge(reinterpret_cast<PyObject *>(MakePyEdge(*e, py_graph)));
      return py_mgp.CallMethod("Edge", py_edge);
    }
    case MGP_VALUE_TYPE_PATH: {
      py::Object py_mgp(PyImport_ImportModule("mgp"));
      if (!py_mgp) return nullptr;
      const auto *p = mgp_value_get_path(&value);
      py::Object py_path(reinterpret_cast<PyObject *>(MakePyPath(*p, py_graph)));
      return py_mgp.CallMethod("Path", py_path);
    }
  }
}

mgp_value *PyObjectToMgpValue(PyObject *o, mgp_memory *memory) {
  auto py_seq_to_list = [memory](PyObject *seq, Py_ssize_t len, const auto &py_seq_get_item) {
    static_assert(std::numeric_limits<Py_ssize_t>::max() <= std::numeric_limits<size_t>::max());
    mgp_list *list = mgp_list_make_empty(len, memory);
    if (!list) throw std::bad_alloc();
    for (Py_ssize_t i = 0; i < len; ++i) {
      PyObject *e = py_seq_get_item(seq, i);
      mgp_value *v{nullptr};
      try {
        v = PyObjectToMgpValue(e, memory);
      } catch (...) {
        mgp_list_destroy(list);
        throw;
      }
      if (!mgp_list_append(list, v)) {
        mgp_value_destroy(v);
        mgp_list_destroy(list);
        throw std::bad_alloc();
      }
      mgp_value_destroy(v);
    }
    auto *v = mgp_value_make_list(list);
    if (!v) {
      mgp_list_destroy(list);
      throw std::bad_alloc();
    }
    return v;
  };

  auto is_mgp_instance = [](PyObject *obj, const char *mgp_type_name) {
    py::Object py_mgp(PyImport_ImportModule("mgp"));
    if (!py_mgp) {
      PyErr_Clear();
      // This way we skip conversions of types from user-facing 'mgp' module.
      return false;
    }
    auto mgp_type = py_mgp.GetAttr(mgp_type_name);
    if (!mgp_type) {
      PyErr_Clear();
      std::stringstream ss;
      ss << "'mgp' module is missing '" << mgp_type_name << "' type";
      throw std::invalid_argument(ss.str());
    }
    int res = PyObject_IsInstance(obj, mgp_type.Ptr());
    if (res == -1) {
      PyErr_Clear();
      std::stringstream ss;
      ss << "Error when checking object is instance of 'mgp." << mgp_type_name << "' type";
      throw std::invalid_argument(ss.str());
    }
    return static_cast<bool>(res);
  };

  mgp_value *mgp_v{nullptr};

  if (o == Py_None) {
    mgp_v = mgp_value_make_null(memory);
  } else if (PyBool_Check(o)) {
    mgp_v = mgp_value_make_bool(static_cast<int>(o == Py_True), memory);
  } else if (PyLong_Check(o)) {
    int64_t value = PyLong_AsLong(o);
    if (PyErr_Occurred()) {
      PyErr_Clear();
      throw std::overflow_error("Python integer is out of range");
    }
    mgp_v = mgp_value_make_int(value, memory);
  } else if (PyFloat_Check(o)) {
    mgp_v = mgp_value_make_double(PyFloat_AsDouble(o), memory);
  } else if (PyUnicode_Check(o)) {
    mgp_v = mgp_value_make_string(PyUnicode_AsUTF8(o), memory);
  } else if (PyList_Check(o)) {
    mgp_v = py_seq_to_list(o, PyList_Size(o), [](auto *list, const auto i) { return PyList_GET_ITEM(list, i); });
  } else if (PyTuple_Check(o)) {
    mgp_v = py_seq_to_list(o, PyTuple_Size(o), [](auto *tuple, const auto i) { return PyTuple_GET_ITEM(tuple, i); });
  } else if (PyDict_Check(o)) {
    mgp_map *map = mgp_map_make_empty(memory);

    if (!map) {
      throw std::bad_alloc();
    }

    PyObject *key{nullptr};
    PyObject *value{nullptr};
    Py_ssize_t pos{0};
    while (PyDict_Next(o, &pos, &key, &value)) {
      if (!PyUnicode_Check(key)) {
        mgp_map_destroy(map);
        throw std::invalid_argument("Dictionary keys must be strings");
      }

      const char *k = PyUnicode_AsUTF8(key);
      mgp_value *v{nullptr};

      if (!k) {
        PyErr_Clear();
        mgp_map_destroy(map);
        throw std::bad_alloc();
      }

      try {
        v = PyObjectToMgpValue(value, memory);
      } catch (...) {
        mgp_map_destroy(map);
        throw;
      }

      if (!mgp_map_insert(map, k, v)) {
        mgp_value_destroy(v);
        mgp_map_destroy(map);
        throw std::bad_alloc();
      }

      mgp_value_destroy(v);
    }

    mgp_v = mgp_value_make_map(map);
    if (!mgp_v) {
      mgp_map_destroy(map);
      throw std::bad_alloc();
    }
  } else if (Py_TYPE(o) == &PyEdgeType) {
    // Copy the edge and pass the ownership to the created mgp_value.
    auto *e = mgp_edge_copy(reinterpret_cast<PyEdge *>(o)->edge, memory);
    if (!e) {
      throw std::bad_alloc();
    }
    mgp_v = mgp_value_make_edge(e);
    if (!mgp_v) {
      mgp_edge_destroy(e);
      throw std::bad_alloc();
    }
  } else if (Py_TYPE(o) == &PyPathType) {
    // Copy the path and pass the ownership to the created mgp_value.
    auto *p = mgp_path_copy(reinterpret_cast<PyPath *>(o)->path, memory);
    if (!p) {
      throw std::bad_alloc();
    }
    mgp_v = mgp_value_make_path(p);
    if (!mgp_v) {
      mgp_path_destroy(p);
      throw std::bad_alloc();
    }
  } else if (Py_TYPE(o) == &PyVertexType) {
    // Copy the vertex and pass the ownership to the created mgp_value.
    auto *v = mgp_vertex_copy(reinterpret_cast<PyVertex *>(o)->vertex, memory);
    if (!v) {
      throw std::bad_alloc();
    }
    mgp_v = mgp_value_make_vertex(v);
    if (!mgp_v) {
      mgp_vertex_destroy(v);
      throw std::bad_alloc();
    }
  } else if (is_mgp_instance(o, "Edge")) {
    py::Object edge(PyObject_GetAttrString(o, "_edge"));
    if (!edge) {
      PyErr_Clear();
      throw std::invalid_argument("'mgp.Edge' is missing '_edge' attribute");
    }
    return PyObjectToMgpValue(edge.Ptr(), memory);
  } else if (is_mgp_instance(o, "Vertex")) {
    py::Object vertex(PyObject_GetAttrString(o, "_vertex"));
    if (!vertex) {
      PyErr_Clear();
      throw std::invalid_argument("'mgp.Vertex' is missing '_vertex' attribute");
    }
    return PyObjectToMgpValue(vertex.Ptr(), memory);
  } else if (is_mgp_instance(o, "Path")) {
    py::Object path(PyObject_GetAttrString(o, "_path"));
    if (!path) {
      PyErr_Clear();
      throw std::invalid_argument("'mgp.Path' is missing '_path' attribute");
    }
    return PyObjectToMgpValue(path.Ptr(), memory);
  } else {
    throw std::invalid_argument("Unsupported PyObject conversion");
  }

  if (!mgp_v) {
    throw std::bad_alloc();
  }

  return mgp_v;
}

}  // namespace query::procedure
