#include "query/procedure/py_module.hpp"

#include <stdexcept>

#include "query/procedure/mg_procedure_impl.hpp"

namespace query::procedure {

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
struct PyGraph {
  PyObject_HEAD
  const mgp_graph *graph;
  mgp_memory *memory;
};

struct PyVerticesIterator {
  PyObject_HEAD
  mgp_vertices_iterator *it;
  PyGraph *py_graph;
};

PyObject *MakePyVertex(mgp_vertex *vertex, PyGraph *py_graph);

void PyVerticesIteratorDealloc(PyVerticesIterator *self) {
  CHECK(self->it);
  CHECK(self->py_graph);
  // Avoid invoking `mgp_vertices_iterator_destroy` if we are not in valid
  // execution context. The query execution should free all memory used during
  // execution, so we may cause a double free issue.
  if (self->py_graph->graph) mgp_vertices_iterator_destroy(self->it);
  Py_DECREF(self->py_graph);
  Py_TYPE(self)->tp_free(self);
}

PyObject *PyVerticesIteratorGet(PyVerticesIterator *self,
                                PyObject *Py_UNUSED(ignored)) {
  CHECK(self->it);
  CHECK(self->py_graph);
  CHECK(self->py_graph->graph);
  const auto *vertex = mgp_vertices_iterator_get(self->it);
  if (!vertex) Py_RETURN_NONE;
  return MakePyVertex(mgp_vertex_copy(vertex, self->py_graph->memory),
                      self->py_graph);
}

PyObject *PyVerticesIteratorNext(PyVerticesIterator *self,
                                 PyObject *Py_UNUSED(ignored)) {
  CHECK(self->it);
  CHECK(self->py_graph);
  CHECK(self->py_graph->graph);
  const auto *vertex = mgp_vertices_iterator_next(self->it);
  if (!vertex) Py_RETURN_NONE;
  return MakePyVertex(mgp_vertex_copy(vertex, self->py_graph->memory),
                      self->py_graph);
}

static PyMethodDef PyVerticesIteratorMethods[] = {
    {"get", reinterpret_cast<PyCFunction>(PyVerticesIteratorGet), METH_NOARGS,
     "Get the current vertex pointed to by the iterator or return None."},
    {"next", reinterpret_cast<PyCFunction>(PyVerticesIteratorNext), METH_NOARGS,
     "Advance the iterator to the next vertex and return it."},
    {nullptr},
};

static PyTypeObject PyVerticesIteratorType = {
    PyVarObject_HEAD_INIT(nullptr, 0)
    .tp_name = "_mgp.VerticesIterator",
    .tp_doc = "Wraps struct mgp_vertices_iterator.",
    .tp_basicsize = sizeof(PyVerticesIterator),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = PyType_GenericNew,
    .tp_methods = PyVerticesIteratorMethods,
    .tp_dealloc = reinterpret_cast<destructor>(PyVerticesIteratorDealloc),
};

struct PyEdgesIterator {
  PyObject_HEAD
  mgp_edges_iterator *it;
  PyGraph *py_graph;
};

PyObject *MakePyEdge(mgp_edge *edge, PyGraph *py_graph);

void PyEdgesIteratorDealloc(PyEdgesIterator *self) {
  CHECK(self->it);
  CHECK(self->py_graph);
  // Avoid invoking `mgp_edges_iterator_destroy` if we are not in valid
  // execution context. The query execution should free all memory used during
  // execution, so we may cause a double free issue.
  if (self->py_graph->graph) mgp_edges_iterator_destroy(self->it);
  Py_DECREF(self->py_graph);
  Py_TYPE(self)->tp_free(self);
}

PyObject *PyEdgesIteratorGet(PyEdgesIterator *self,
                             PyObject *Py_UNUSED(ignored)) {
  CHECK(self->it);
  CHECK(self->py_graph);
  CHECK(self->py_graph->graph);
  const auto *edge = mgp_edges_iterator_get(self->it);
  if (!edge) Py_RETURN_NONE;
  return MakePyEdge(mgp_edge_copy(edge, self->py_graph->memory),
                    self->py_graph);
}

PyObject *PyEdgesIteratorNext(PyEdgesIterator *self,
                              PyObject *Py_UNUSED(ignored)) {
  CHECK(self->it);
  CHECK(self->py_graph);
  CHECK(self->py_graph->graph);
  const auto *edge = mgp_edges_iterator_next(self->it);
  if (!edge) Py_RETURN_NONE;
  return MakePyEdge(mgp_edge_copy(edge, self->py_graph->memory),
                    self->py_graph);
}

static PyMethodDef PyEdgesIteratorMethods[] = {
    {"get", reinterpret_cast<PyCFunction>(PyEdgesIteratorGet), METH_NOARGS,
     "Get the current edge pointed to by the iterator or return None."},
    {"next", reinterpret_cast<PyCFunction>(PyEdgesIteratorNext), METH_NOARGS,
     "Advance the iterator to the next edge and return it."},
    {nullptr},
};

static PyTypeObject PyEdgesIteratorType = {
    PyVarObject_HEAD_INIT(nullptr, 0)
    .tp_name = "_mgp.EdgesIterator",
    .tp_doc = "Wraps struct mgp_edges_iterator.",
    .tp_basicsize = sizeof(PyEdgesIterator),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = PyType_GenericNew,
    .tp_methods = PyEdgesIteratorMethods,
    .tp_dealloc = reinterpret_cast<destructor>(PyEdgesIteratorDealloc),
};

PyObject *PyGraphIsValid(PyGraph *self, PyObject *Py_UNUSED(ignored)) {
  return PyBool_FromLong(!!self->graph);
}

PyObject *PyGraphGetVertexById(PyGraph *self, PyObject *args) {
  CHECK(self->graph);
  CHECK(self->memory);
  static_assert(std::is_same_v<int64_t, long>);
  int64_t id;
  if (!PyArg_ParseTuple(args, "l", &id)) return nullptr;
  auto *vertex =
      mgp_graph_get_vertex_by_id(self->graph, mgp_vertex_id{id}, self->memory);
  if (!vertex) {
    PyErr_SetString(PyExc_IndexError,
                    "Unable to find the vertex with given ID.");
    return nullptr;
  }
  return MakePyVertex(mgp_vertex_copy(vertex, self->memory), self);
}

PyObject *PyGraphIterVertices(PyGraph *self, PyObject *Py_UNUSED(ignored)) {
  CHECK(self->graph);
  CHECK(self->memory);
  auto *vertices_it = mgp_graph_iter_vertices(self->graph, self->memory);
  if (!vertices_it) {
    PyErr_SetString(PyExc_MemoryError,
                    "Unable to allocate mgp_vertices_iterator.");
    return nullptr;
  }
  auto *py_vertices_it =
      PyObject_New(PyVerticesIterator, &PyVerticesIteratorType);
  if (!py_vertices_it) {
    PyErr_SetString(PyExc_MemoryError,
                    "Unable to allocate _mgp.VerticesIterator.");
    return nullptr;
  }
  py_vertices_it->it = vertices_it;
  Py_INCREF(self);
  py_vertices_it->py_graph = self;
  return PyObject_Init(reinterpret_cast<PyObject *>(py_vertices_it),
                       &PyVerticesIteratorType);
}

static PyMethodDef PyGraphMethods[] = {
    {"is_valid", reinterpret_cast<PyCFunction>(PyGraphIsValid), METH_NOARGS,
     "Return True if Graph is in valid context and may be used."},
    {"get_vertex_by_id", reinterpret_cast<PyCFunction>(PyGraphGetVertexById),
     METH_VARARGS, "Get the vertex or raise IndexError."},
    {"iter_vertices", reinterpret_cast<PyCFunction>(PyGraphIterVertices),
     METH_NOARGS, "Return _mgp.VerticesIterator."},
    {nullptr},
};

static PyTypeObject PyGraphType = {
    PyVarObject_HEAD_INIT(nullptr, 0)
    .tp_name = "_mgp.Graph",
    .tp_doc = "Wraps struct mgp_graph.",
    .tp_basicsize = sizeof(PyGraph),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = PyType_GenericNew,
    .tp_methods = PyGraphMethods,
};

PyObject *MakePyGraph(const mgp_graph *graph, mgp_memory *memory) {
  auto *py_graph = PyObject_New(PyGraph, &PyGraphType);
  if (!py_graph) return nullptr;
  py_graph->graph = graph;
  py_graph->memory = memory;
  return PyObject_Init(reinterpret_cast<PyObject *>(py_graph), &PyGraphType);
}

struct PyCypherType {
  PyObject_HEAD
  const mgp_type *type;
};

static PyTypeObject PyCypherTypeType = {
    PyVarObject_HEAD_INIT(nullptr, 0)
    .tp_name = "_mgp.Type",
    .tp_doc = "Wraps struct mgp_type.",
    .tp_basicsize = sizeof(PyCypherType),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = PyType_GenericNew,
};

PyObject *MakePyCypherType(const mgp_type *type) {
  auto *py_type = PyObject_New(PyCypherType, &PyCypherTypeType);
  if (!py_type) return nullptr;
  py_type->type = type;
  return PyObject_Init(reinterpret_cast<PyObject *>(py_type),
                       &PyCypherTypeType);
}

struct PyQueryProc {
  PyObject_HEAD
  mgp_proc *proc;
};

PyObject *PyQueryProcAddArg(PyQueryProc *self, PyObject *args) {
  CHECK(self->proc);
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
  CHECK(self->proc);
  const char *name = nullptr;
  PyObject *py_type = nullptr;
  PyObject *py_value = nullptr;
  if (!PyArg_ParseTuple(args, "sOO", &name, &py_type, &py_value))
    return nullptr;
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
  CHECK(value);
  if (!mgp_proc_add_opt_arg(self->proc, name, type, value)) {
    mgp_value_destroy(value);
    PyErr_SetString(PyExc_ValueError, "Invalid call to mgp_proc_add_opt_arg.");
    return nullptr;
  }
  mgp_value_destroy(value);
  Py_RETURN_NONE;
}

PyObject *PyQueryProcAddResult(PyQueryProc *self, PyObject *args) {
  CHECK(self->proc);
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
  CHECK(self->proc);
  const char *name = nullptr;
  PyObject *py_type = nullptr;
  if (!PyArg_ParseTuple(args, "sO", &name, &py_type)) return nullptr;
  if (Py_TYPE(py_type) != &PyCypherTypeType) {
    PyErr_SetString(PyExc_TypeError, "Expected a _mgp.Type.");
    return nullptr;
  }
  const auto *type = reinterpret_cast<PyCypherType *>(py_type)->type;
  if (!mgp_proc_add_deprecated_result(self->proc, name, type)) {
    PyErr_SetString(PyExc_ValueError,
                    "Invalid call to mgp_proc_add_deprecated_result.");
    return nullptr;
  }
  Py_RETURN_NONE;
}

static PyMethodDef PyQueryProcMethods[] = {
    {"add_arg", reinterpret_cast<PyCFunction>(PyQueryProcAddArg), METH_VARARGS,
     "Add a required argument to a procedure."},
    {"add_opt_arg", reinterpret_cast<PyCFunction>(PyQueryProcAddOptArg),
     METH_VARARGS,
     "Add an optional argument with a default value to a procedure."},
    {"add_result", reinterpret_cast<PyCFunction>(PyQueryProcAddResult),
     METH_VARARGS, "Add a result field to a procedure."},
    {"add_deprecated_result",
     reinterpret_cast<PyCFunction>(PyQueryProcAddDeprecatedResult),
     METH_VARARGS,
     "Add a result field to a procedure and mark it as deprecated."},
    {nullptr},
};

static PyTypeObject PyQueryProcType = {
    PyVarObject_HEAD_INIT(nullptr, 0)
    .tp_name = "_mgp.Proc",
    .tp_doc = "Wraps struct mgp_proc.",
    .tp_basicsize = sizeof(PyQueryProc),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = PyType_GenericNew,
    .tp_methods = PyQueryProcMethods,
};

struct PyQueryModule {
  PyObject_HEAD
  mgp_module *module;
};

PyObject *PyQueryModuleAddReadProcedure(PyQueryModule *self, PyObject *cb) {
  CHECK(self->module);
  if (!PyCallable_Check(cb)) {
    PyErr_SetString(PyExc_TypeError, "Expected a callable object.");
    return nullptr;
  }
  Py_INCREF(cb);
  py::Object py_cb(cb);
  py::Object py_name(PyObject_GetAttrString(py_cb, "__name__"));
  const auto *name = PyUnicode_AsUTF8(py_name);
  // TODO: Validate name
  auto *memory = self->module->procedures.get_allocator().GetMemoryResource();
  mgp_proc proc(
      name,
      [py_cb](const mgp_list *, const mgp_graph *, mgp_result *, mgp_memory *) {
        auto gil = py::EnsureGIL();
        throw utils::NotYetImplemented("Invoking Python procedures");
      },
      memory);
  const auto &[proc_it, did_insert] =
      self->module->procedures.emplace(name, std::move(proc));
  if (!did_insert) {
    PyErr_SetString(PyExc_ValueError,
                    "Already registered a procedure with the same name.");
    return nullptr;
  }
  auto *py_proc = PyObject_New(PyQueryProc, &PyQueryProcType);
  if (!py_proc) return nullptr;
  py_proc->proc = &proc_it->second;
  return PyObject_Init(reinterpret_cast<PyObject *>(py_proc), &PyQueryProcType);
}

static PyMethodDef PyQueryModuleMethods[] = {
    {"add_read_procedure",
     reinterpret_cast<PyCFunction>(PyQueryModuleAddReadProcedure), METH_O,
     "Register a read-only procedure with this module."},
    {nullptr},
};

static PyTypeObject PyQueryModuleType = {
    PyVarObject_HEAD_INIT(nullptr, 0)
    .tp_name = "_mgp.Module",
    .tp_doc = "Wraps struct mgp_module.",
    .tp_basicsize = sizeof(PyQueryModule),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = PyType_GenericNew,
    .tp_methods = PyQueryModuleMethods,
};

PyObject *MakePyQueryModule(mgp_module *module) {
  auto *py_query_module = PyObject_New(PyQueryModule, &PyQueryModuleType);
  if (!py_query_module) return nullptr;
  py_query_module->module = module;
  return PyObject_Init(reinterpret_cast<PyObject *>(py_query_module),
                       &PyQueryModuleType);
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

PyObject *PyMgpModuleTypeAny(PyObject *mod, PyObject *Py_UNUSED(ignored)) {
  return MakePyCypherType(mgp_type_any());
}

PyObject *PyMgpModuleTypeBool(PyObject *mod, PyObject *Py_UNUSED(ignored)) {
  return MakePyCypherType(mgp_type_bool());
}

PyObject *PyMgpModuleTypeString(PyObject *mod, PyObject *Py_UNUSED(ignored)) {
  return MakePyCypherType(mgp_type_string());
}

PyObject *PyMgpModuleTypeInt(PyObject *mod, PyObject *Py_UNUSED(ignored)) {
  return MakePyCypherType(mgp_type_int());
}

PyObject *PyMgpModuleTypeFloat(PyObject *mod, PyObject *Py_UNUSED(ignored)) {
  return MakePyCypherType(mgp_type_float());
}

PyObject *PyMgpModuleTypeNumber(PyObject *mod, PyObject *Py_UNUSED(ignored)) {
  return MakePyCypherType(mgp_type_number());
}

PyObject *PyMgpModuleTypeMap(PyObject *mod, PyObject *Py_UNUSED(ignored)) {
  return MakePyCypherType(mgp_type_map());
}

PyObject *PyMgpModuleTypeNode(PyObject *mod, PyObject *Py_UNUSED(ignored)) {
  return MakePyCypherType(mgp_type_node());
}

PyObject *PyMgpModuleTypeRelationship(PyObject *mod,
                                      PyObject *Py_UNUSED(ignored)) {
  return MakePyCypherType(mgp_type_relationship());
}

PyObject *PyMgpModuleTypePath(PyObject *mod, PyObject *Py_UNUSED(ignored)) {
  return MakePyCypherType(mgp_type_path());
}

static PyMethodDef PyMgpModuleMethods[] = {
    {"type_nullable", PyMgpModuleTypeNullable, METH_O,
     "Build a type representing either a `null` value or a value of given "
     "type."},
    {"type_list", PyMgpModuleTypeList, METH_O,
     "Build a type representing a list of values of given type."},
    {"type_any", PyMgpModuleTypeAny, METH_NOARGS,
     "Get the type representing any value that isn't `null`."},
    {"type_bool", PyMgpModuleTypeBool, METH_NOARGS,
     "Get the type representing boolean values."},
    {"type_string", PyMgpModuleTypeString, METH_NOARGS,
     "Get the type representing string values."},
    {"type_int", PyMgpModuleTypeInt, METH_NOARGS,
     "Get the type representing integer values."},
    {"type_float", PyMgpModuleTypeFloat, METH_NOARGS,
     "Get the type representing floating-point values."},
    {"type_number", PyMgpModuleTypeNumber, METH_NOARGS,
     "Get the type representing any number value."},
    {"type_map", PyMgpModuleTypeMap, METH_NOARGS,
     "Get the type representing map values."},
    {"type_node", PyMgpModuleTypeNode, METH_NOARGS,
     "Get the type representing graph node values."},
    {"type_relationship", PyMgpModuleTypeRelationship, METH_NOARGS,
     "Get the type representing graph relationship values."},
    {"type_path", PyMgpModuleTypePath, METH_NOARGS,
     "Get the type representing a graph path (walk) from one node to another."},
    {nullptr},
};

static PyModuleDef PyMgpModule = {
    PyModuleDef_HEAD_INIT,
    .m_name = "_mgp",
    .m_doc = "Contains raw bindings to mg_procedure.h C API.",
    .m_size = -1,
    .m_methods = PyMgpModuleMethods,
};

struct PyEdge {
  PyObject_HEAD
  mgp_edge *edge;
  PyGraph *py_graph;
};

PyObject *PyEdgeGetTypeName(PyEdge *self, PyObject *Py_UNUSED(ignored)) {
  CHECK(self);
  CHECK(self->edge);
  CHECK(self->py_graph);
  CHECK(self->py_graph->graph);
  return PyUnicode_FromString(mgp_edge_get_type(self->edge).name);
}

PyObject *PyEdgeFromVertex(PyEdge *self, PyObject *Py_UNUSED(ignored)) {
  CHECK(self);
  CHECK(self->edge);
  CHECK(self->py_graph);
  CHECK(self->py_graph->graph);
  const auto *vertex = mgp_edge_get_from(self->edge);
  CHECK(vertex);
  return MakePyVertex(mgp_vertex_copy(vertex, self->py_graph->memory),
                      self->py_graph);
}

PyObject *PyEdgeToVertex(PyEdge *self, PyObject *Py_UNUSED(ignored)) {
  CHECK(self);
  CHECK(self->edge);
  CHECK(self->py_graph);
  CHECK(self->py_graph->graph);
  const auto *vertex = mgp_edge_get_to(self->edge);
  CHECK(vertex);
  return MakePyVertex(mgp_vertex_copy(vertex, self->py_graph->memory),
                      self->py_graph);
}

void PyEdgeDealloc(PyEdge *self) {
  CHECK(self->edge);
  CHECK(self->py_graph);
  // Avoid invoking `mgp_edge_destroy` if we are not in valid execution context.
  // The query execution should free all memory used during execution, so we may
  // cause a double free issue.
  if (self->py_graph->graph) mgp_edge_destroy(self->edge);
  Py_DECREF(self->py_graph);
  Py_TYPE(self)->tp_free(self);
}

PyObject *PyEdgeIsValid(PyEdge *self, PyObject *Py_UNUSED(ignored)) {
  return PyBool_FromLong(!!self->py_graph->graph);
}

static PyMethodDef PyEdgeMethods[] = {
    {"is_valid", reinterpret_cast<PyCFunction>(PyEdgeIsValid), METH_NOARGS,
     "Return True if Edge is in valid context and may be used."},
    {"get_type_name", reinterpret_cast<PyCFunction>(PyEdgeGetTypeName),
     METH_NOARGS, "Return the edge's type name."},
    {"from_vertex", reinterpret_cast<PyCFunction>(PyEdgeFromVertex),
     METH_NOARGS, "Return the edge's source vertex."},
    {"to_vertex", reinterpret_cast<PyCFunction>(PyEdgeToVertex), METH_NOARGS,
     "Return the edge's destination vertex."},
    {nullptr}};

PyObject *PyEdgeRichCompare(PyObject *self, PyObject *other, int op);

static PyTypeObject PyEdgeType = {
    PyVarObject_HEAD_INIT(nullptr, 0)
    .tp_name = "_mgp.Edge",
    .tp_doc = "Wraps struct mgp_edge.",
    .tp_basicsize = sizeof(PyEdge),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = PyType_GenericNew,
    .tp_methods = PyEdgeMethods,
    .tp_dealloc = reinterpret_cast<destructor>(PyEdgeDealloc),
    .tp_richcompare = PyEdgeRichCompare,
};

/// Create an instance of `_mgp.Edge` class.
///
/// The ownership of the edge is given to the created instance and will be
/// destroyed once the instance itself is destroyed, taking care that the
/// execution context is still valid.
///
/// The created instance references an existing `_mgp.Graph` instance, which
/// marks the execution context.
PyObject *MakePyEdge(mgp_edge *edge, PyGraph *py_graph) {
  CHECK(edge->GetMemoryResource() == py_graph->memory->impl);
  auto *py_edge = PyObject_New(PyEdge, &PyEdgeType);
  if (!py_edge) return nullptr;
  py_edge->edge = edge;
  py_edge->py_graph = py_graph;
  Py_INCREF(py_graph);
  return PyObject_Init(reinterpret_cast<PyObject *>(py_edge), &PyEdgeType);
}

PyObject *PyEdgeRichCompare(PyObject *self, PyObject *other, int op) {
  CHECK(self);
  CHECK(other);

  if (Py_TYPE(self) != &PyEdgeType || Py_TYPE(other) != &PyEdgeType ||
      op != Py_EQ) {
    Py_RETURN_NOTIMPLEMENTED;
  }

  auto *e1 = reinterpret_cast<PyEdge *>(self);
  auto *e2 = reinterpret_cast<PyEdge *>(other);
  CHECK(e1->edge);
  CHECK(e2->edge);

  return PyBool_FromLong(mgp_edge_equal(e1->edge, e2->edge));
}

struct PyVertex {
  PyObject_HEAD
  mgp_vertex *vertex;
  PyGraph *py_graph;
};

void PyVertexDealloc(PyVertex *self) {
  CHECK(self->vertex);
  CHECK(self->py_graph);
  // Avoid invoking `mgp_vertex_destroy` if we are not in valid execution
  // context. The query execution should free all memory used during
  // execution, so  we may cause a double free issue.
  if (self->py_graph->graph) mgp_vertex_destroy(self->vertex);
  Py_DECREF(self->py_graph);
  Py_TYPE(self)->tp_free(self);
}

PyObject *PyVertexIsValid(PyVertex *self, PyObject *Py_UNUSED(ignored)) {
  return PyBool_FromLong(!!self->py_graph->graph);
}

PyObject *PyVertexGetId(PyVertex *self, PyObject *Py_UNUSED(ignored)) {
  CHECK(self);
  CHECK(self->vertex);
  CHECK(self->py_graph);
  CHECK(self->py_graph->graph);
  return PyLong_FromLongLong(mgp_vertex_get_id(self->vertex).as_int);
}

PyObject *PyVertexLabelsCount(PyVertex *self, PyObject *Py_UNUSED(ignored)) {
  CHECK(self);
  CHECK(self->vertex);
  CHECK(self->py_graph);
  CHECK(self->py_graph->graph);
  return PyLong_FromSize_t(mgp_vertex_labels_count(self->vertex));
}

PyObject *PyVertexLabelAt(PyVertex *self, PyObject *args) {
  CHECK(self);
  CHECK(self->vertex);
  CHECK(self->py_graph);
  CHECK(self->py_graph->graph);
  static_assert(std::numeric_limits<Py_ssize_t>::max() <=
                std::numeric_limits<size_t>::max());
  Py_ssize_t id;
  if (!PyArg_ParseTuple(args, "n", &id)) return nullptr;
  auto label = mgp_vertex_label_at(self->vertex, id);
  if (label.name == nullptr || id < 0) {
    PyErr_SetString(PyExc_IndexError,
                    "Unable to find the label with given ID.");
    return nullptr;
  }
  return PyUnicode_FromString(label.name);
}

PyObject *PyVertexIterInEdges(PyVertex *self, PyObject *Py_UNUSED(ignored)) {
  CHECK(self);
  CHECK(self->vertex);
  CHECK(self->py_graph);
  CHECK(self->py_graph->graph);
  auto *edges_it =
      mgp_vertex_iter_in_edges(self->vertex, self->py_graph->memory);
  if (!edges_it) {
    PyErr_SetString(PyExc_MemoryError,
                    "Unable to allocate mgp_edges_iterator for in edges.");
    return nullptr;
  }
  auto *py_edges_it = PyObject_New(PyEdgesIterator, &PyEdgesIteratorType);
  if (!py_edges_it) {
    PyErr_SetString(PyExc_MemoryError,
                    "Unable to allocate _mgp.EdgesIterator for in edges.");
    return nullptr;
  }
  py_edges_it->it = edges_it;
  Py_INCREF(self->py_graph);
  py_edges_it->py_graph = self->py_graph;
  return PyObject_Init(reinterpret_cast<PyObject *>(py_edges_it),
                       &PyEdgesIteratorType);
}

PyObject *PyVertexIterOutEdges(PyVertex *self, PyObject *Py_UNUSED(ignored)) {
  CHECK(self);
  CHECK(self->vertex);
  CHECK(self->py_graph);
  CHECK(self->py_graph->graph);
  auto *edges_it =
      mgp_vertex_iter_out_edges(self->vertex, self->py_graph->memory);
  if (!edges_it) {
    PyErr_SetString(PyExc_MemoryError,
                    "Unable to allocate mgp_edges_iterator for out edges.");
    return nullptr;
  }
  auto *py_edges_it = PyObject_New(PyEdgesIterator, &PyEdgesIteratorType);
  if (!py_edges_it) {
    PyErr_SetString(PyExc_MemoryError,
                    "Unable to allocate _mgp.EdgesIterator for out edges.");
    return nullptr;
  }
  py_edges_it->it = edges_it;
  Py_INCREF(self->py_graph);
  py_edges_it->py_graph = self->py_graph;
  return PyObject_Init(reinterpret_cast<PyObject *>(py_edges_it),
                       &PyEdgesIteratorType);
}

static PyMethodDef PyVertexMethods[] = {
    {"is_valid", reinterpret_cast<PyCFunction>(PyVertexIsValid), METH_NOARGS,
     "Return True if Vertex is in valid context and may be used."},
    {"get_id", reinterpret_cast<PyCFunction>(PyVertexGetId), METH_NOARGS,
     "Return vertex id."},
    {"labels_count", reinterpret_cast<PyCFunction>(PyVertexLabelsCount),
     METH_NOARGS, "Return number of lables of a vertex."},
    {"label_at", reinterpret_cast<PyCFunction>(PyVertexLabelAt), METH_VARARGS,
     "Return label of a vertex on a given index."},
    {"iter_in_edges", reinterpret_cast<PyCFunction>(PyVertexIterInEdges),
     METH_NOARGS, "Return _mgp.EdgesIterator for in edges"},
    {"iter_out_edges", reinterpret_cast<PyCFunction>(PyVertexIterOutEdges),
     METH_NOARGS, "Return _mgp.EdgesIterator for out edges"},
    {nullptr}};

PyObject *PyVertexRichCompare(PyObject *self, PyObject *other, int op);

static PyTypeObject PyVertexType = {
    PyVarObject_HEAD_INIT(nullptr, 0).tp_name = "_mgp.Vertex",
    .tp_doc = "Wraps struct mgp_vertex.",
    .tp_basicsize = sizeof(PyVertex),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = PyType_GenericNew,
    .tp_methods = PyVertexMethods,
    .tp_dealloc = reinterpret_cast<destructor>(PyVertexDealloc),
    .tp_richcompare = PyVertexRichCompare,
};

PyObject *MakePyVertex(mgp_vertex *vertex, PyGraph *py_graph) {
  CHECK(vertex->GetMemoryResource() == py_graph->memory->impl);
  auto *py_vertex = PyObject_New(PyVertex, &PyVertexType);
  if (!py_vertex) return nullptr;
  py_vertex->vertex = vertex;
  py_vertex->py_graph = py_graph;
  Py_INCREF(py_graph);
  return PyObject_Init(reinterpret_cast<PyObject *>(py_vertex), &PyVertexType);
}

PyObject *PyVertexRichCompare(PyObject *self, PyObject *other, int op) {
  CHECK(self);
  CHECK(other);

  if (Py_TYPE(self) != &PyVertexType || Py_TYPE(other) != &PyVertexType ||
      op != Py_EQ) {
    Py_RETURN_NOTIMPLEMENTED;
  }

  auto *v1 = reinterpret_cast<PyVertex *>(self);
  auto *v2 = reinterpret_cast<PyVertex *>(other);
  CHECK(v1->vertex);
  CHECK(v2->vertex);

  return PyBool_FromLong(mgp_vertex_equal(v1->vertex, v2->vertex));
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
  if (!register_type(&PyVerticesIteratorType, "VerticesIterator"))
    return nullptr;
  if (!register_type(&PyEdgesIteratorType, "EdgesIterator")) return nullptr;
  if (!register_type(&PyGraphType, "Graph")) return nullptr;
  if (!register_type(&PyEdgeType, "Edge")) return nullptr;
  if (!register_type(&PyQueryProcType, "Proc")) return nullptr;
  if (!register_type(&PyQueryModuleType, "Module")) return nullptr;
  if (!register_type(&PyVertexType, "Vertex")) return nullptr;
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
  CHECK(py_mgp) << "Expected builtin '_mgp' to be available for import";
  py::Object py_mgp_module(PyObject_GetAttrString(py_mgp, "_MODULE"));
  CHECK(py_mgp_module) << "Expected '_mgp' to have attribute '_MODULE'";
  // NOTE: This check is not thread safe, but this should only go through
  // ModuleRegistry::LoadModuleLibrary which ought to serialize loading.
  CHECK(py_mgp_module == Py_None)
      << "Expected '_mgp._MODULE' to be None as we are just starting to "
         "import a new module. Is some other thread also importing Python "
         "modules?";
  auto *py_query_module = MakePyQueryModule(module_def);
  CHECK(py_query_module);
  CHECK(0 <= PyObject_SetAttrString(py_mgp, "_MODULE", py_query_module));
  auto ret = fun();
  CHECK(0 <= PyObject_SetAttrString(py_mgp, "_MODULE", Py_None));
  return ret;
}

}  // namespace

py::Object ImportPyModule(const char *name, mgp_module *module_def) {
  return WithMgpModule(
      module_def, [name]() { return py::Object(PyImport_ImportModule(name)); });
}

py::Object ReloadPyModule(PyObject *py_module, mgp_module *module_def) {
  return WithMgpModule(module_def, [py_module]() {
    return py::Object(PyImport_ReloadModule(py_module));
  });
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
    case MGP_VALUE_TYPE_LIST: {
      const auto *list = mgp_value_get_list(&value);
      const size_t len = mgp_list_size(list);
      py::Object py_list(PyList_New(len));
      CHECK(py_list);
      for (size_t i = 0; i < len; ++i) {
        auto elem = MgpValueToPyObject(*mgp_list_at(list, i), py_graph);
        CHECK(elem);
        // Explicitly convert `py_list`, which is `py::Object`, via static_cast.
        // Then the macro will cast it to `PyList *`.
        PyList_SET_ITEM(static_cast<PyObject *>(py_list), i, elem.Steal());
      }
      return py_list;
    }
    case MGP_VALUE_TYPE_MAP: {
      const auto *map = mgp_value_get_map(&value);
      py::Object py_dict(PyDict_New());
      CHECK(py_dict);
      for (const auto &[key, val] : map->items) {
        auto py_val = MgpValueToPyObject(val, py_graph);
        CHECK(py_val);
        // Unlike PyList_SET_ITEM, PyDict_SetItem does not steal the value.
        CHECK(PyDict_SetItemString(py_dict, key.c_str(), py_val) == 0);
      }
      return py_dict;
    }
    case MGP_VALUE_TYPE_VERTEX:
      throw utils::NotYetImplemented("MgpValueToPyObject");
    case MGP_VALUE_TYPE_EDGE: {
      // Copy the edge and pass the ownership to the created _mgp.Edge
      // instance.
      auto *e = mgp_edge_copy(mgp_value_get_edge(&value), py_graph->memory);
      if (!e) {
        PyErr_NoMemory();
        return py::Object();
      }
      return py::Object(reinterpret_cast<PyObject *>(MakePyEdge(e, py_graph)));
    }
    case MGP_VALUE_TYPE_PATH:
      throw utils::NotYetImplemented("MgpValueToPyObject");
  }
}

mgp_value *PyObjectToMgpValue(PyObject *o, mgp_memory *memory) {
  auto py_seq_to_list = [memory](PyObject *seq, Py_ssize_t len,
                                 const auto &py_seq_get_item) {
    static_assert(std::numeric_limits<Py_ssize_t>::max() <=
                  std::numeric_limits<size_t>::max());
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
    mgp_v = py_seq_to_list(o, PyList_Size(o), [](auto *list, const auto i) {
      return PyList_GET_ITEM(list, i);
    });
  } else if (PyTuple_Check(o)) {
    mgp_v = py_seq_to_list(o, PyTuple_Size(o), [](auto *tuple, const auto i) {
      return PyTuple_GET_ITEM(tuple, i);
    });
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
  } else {
    // TODO: Check for Vertex and Path. Throw std::invalid_argument for
    // everything else.
    throw utils::NotYetImplemented("PyObjectToMgpValue");
  }

  if (!mgp_v) {
    throw std::bad_alloc();
  }

  return mgp_v;
}

}  // namespace query::procedure
