#include "query/procedure/py_module.hpp"

#include "query/procedure/mg_procedure_impl.hpp"

namespace query::procedure {

py::Object MgpValueToPyObject(const mgp_value &value) {
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
        auto elem = MgpValueToPyObject(*mgp_list_at(list, i));
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
        auto py_val = MgpValueToPyObject(val);
        CHECK(py_val);
        // Unlike PyList_SET_ITEM, PyDict_SetItem does not steal the value.
        CHECK(PyDict_SetItemString(py_dict, key.c_str(), py_val) == 0);
      }
      return py_dict;
    }
    case MGP_VALUE_TYPE_VERTEX:
    case MGP_VALUE_TYPE_EDGE:
    case MGP_VALUE_TYPE_PATH:
      throw utils::NotYetImplemented("MgpValueToPyObject");
  }
}

}  // namespace query::procedure
