// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/// @file
/// Provides a C++ API for working with Python's original C API.
#pragma once

#include <optional>
#include <ostream>
#include <string_view>

// Define to use Py_ssize_t for API returning length of something. Some future
// Python version will only support Py_ssize_t, so it's best to always define
// this macro before including Python.h.
#define PY_SSIZE_T_CLEAN
#include <Python.h>

#if PY_MAJOR_VERSION != 3 || PY_MINOR_VERSION < 5
#error "Minimum supported Python API is 3.5"
#endif

#include "py/fmt.hpp"
#include "utils/logging.hpp"

namespace memgraph::py {

/// Ensure the current thread is ready to call Python C API.
///
/// You must *not* try to ensure the GIL when the runtime is finalizing, as
/// that will terminate the thread. You may use `_Py_IsFinalizing` or
/// `sys.is_finalizing()` to check for such a case.
class EnsureGIL final {
  PyGILState_STATE gil_state_;

 public:
  EnsureGIL() noexcept : gil_state_(PyGILState_Ensure()) {}
  ~EnsureGIL() noexcept { PyGILState_Release(gil_state_); }
  EnsureGIL(const EnsureGIL &) = delete;
  EnsureGIL(EnsureGIL &&) = delete;
  EnsureGIL &operator=(const EnsureGIL &) = delete;
  EnsureGIL &operator=(EnsureGIL &&) = delete;
};

/// Owns a `PyObject *` and supports a more C++ idiomatic API to objects.
class [[nodiscard]] Object final {
  PyObject *ptr_{nullptr};

 public:
  Object() = default;
  Object(std::nullptr_t) {}
  /// Construct by taking the ownership of `PyObject *`.
  explicit Object(PyObject *ptr) noexcept : ptr_(ptr) {}

  /// Construct from a borrowed `PyObject *`, i.e. non-owned pointer.
  static Object FromBorrow(PyObject *ptr) noexcept {
    Py_XINCREF(ptr);
    return Object(ptr);
  }

  ~Object() noexcept { Py_XDECREF(ptr_); }

  Object(const Object &other) noexcept : ptr_(other.ptr_) { Py_XINCREF(ptr_); }

  Object(Object &&other) noexcept : ptr_(other.ptr_) { other.ptr_ = nullptr; }

  Object &operator=(const Object &other) noexcept {
    if (this == &other) return *this;
    Py_XDECREF(ptr_);
    ptr_ = other.ptr_;
    Py_XINCREF(ptr_);
    return *this;
  }

  Object &operator=(Object &&other) noexcept {
    if (this == &other) return *this;
    Py_XDECREF(ptr_);
    ptr_ = other.ptr_;
    other.ptr_ = nullptr;
    return *this;
  }

  operator bool() const { return ptr_; }

  /// Borrow the original `PyObject *`, the ownership is not transferred.
  /// @sa Steal
  explicit operator PyObject *() const { return ptr_; }

  /// Borrow the original `PyObject *`, the ownership is not transferred.
  /// @sa Steal
  PyObject *Ptr() const { return ptr_; }

  /// Release the ownership on this PyObject, i.e. we steal the reference.
  /// @sa Ptr
  PyObject *Steal() {
    auto *p = ptr_;
    ptr_ = nullptr;
    return p;
  }

  /// Equivalent to `str(o)` in Python.
  ///
  /// Returned Object is nullptr if an error occurred.
  /// @sa FetchError
  Object Str() const { return Object(PyObject_Str(ptr_)); }

  /// Equivalent to `hasattr(this, attr_name)` in Python.
  ///
  /// This function always succeeds, meaning that exceptions that occur while
  /// calling __getattr__ and __getattribute__ will get suppressed. To get error
  /// reporting, use GetAttr instead.
  bool HasAttr(const char *attr_name) const { return PyObject_HasAttrString(ptr_, attr_name); }

  /// Equivalent to `hasattr(this, attr_name)` in Python.
  ///
  /// This function always succeeds, meaning that exceptions that occur while
  /// calling __getattr__ and __getattribute__ will get suppressed. To get error
  /// reporting, use GetAttr instead.
  bool HasAttr(PyObject *attr_name) const { return PyObject_HasAttr(ptr_, attr_name); }

  /// Equivalent to `this.attr_name` in Python.
  ///
  /// Returned Object is nullptr if an error occurred.
  /// @sa FetchError
  Object GetAttr(const char *attr_name) const { return Object(PyObject_GetAttrString(ptr_, attr_name)); }

  /// Equivalent to `this.attr_name` in Python.
  ///
  /// Returned Object is nullptr if an error occurred.
  /// @sa FetchError
  Object GetAttr(PyObject *attr_name) const { return Object(PyObject_GetAttr(ptr_, attr_name)); }

  /// Equivalent to `this.attr_name = v` in Python.
  ///
  /// False is returned if an error occurred.
  /// @sa FetchError
  [[nodiscard]] bool SetAttr(const char *attr_name, PyObject *v) {
    return PyObject_SetAttrString(ptr_, attr_name, v) == 0;
  }

  /// Equivalent to `this.attr_name = v` in Python.
  ///
  /// False is returned if an error occurred.
  /// @sa FetchError
  [[nodiscard]] bool SetAttr(PyObject *attr_name, PyObject *v) { return PyObject_SetAttr(ptr_, attr_name, v) == 0; }

  /// Equivalent to `callable()` in Python.
  ///
  /// Returned Object is nullptr if an error occurred.
  /// @sa FetchError
  Object Call() const { return Object(PyObject_CallObject(ptr_, nullptr)); }

  /// Equivalent to `callable(*args)` in Python.
  ///
  /// Returned Object is nullptr if an error occurred.
  /// @sa FetchError
  template <class... TArgs>
  Object Call(const TArgs &...args) const {
    return Object(PyObject_CallFunctionObjArgs(ptr_, static_cast<PyObject *>(args)..., nullptr));
  }

  /// Equivalent to `obj.meth_name()` in Python.
  ///
  /// Returned Object is nullptr if an error occurred.
  /// @sa FetchError
  Object CallMethod(std::string_view meth_name) const {
    Object name(PyUnicode_FromStringAndSize(meth_name.data(), meth_name.size()));
    return Object(PyObject_CallMethodObjArgs(ptr_, name.Ptr(), nullptr));
  }

  /// Equivalent to `obj.meth_name(*args)` in Python.
  ///
  /// Returned Object is nullptr if an error occurred.
  /// @sa FetchError
  template <class... TArgs>
  Object CallMethod(std::string_view meth_name, const TArgs &...args) const {
    Object name(PyUnicode_FromStringAndSize(meth_name.data(), meth_name.size()));
    return Object(PyObject_CallMethodObjArgs(ptr_, name.Ptr(), static_cast<PyObject *>(args)..., nullptr));
  }
};

/// Write Object to stream as if `str(o)` was called in Python.
inline std::ostream &operator<<(std::ostream &os, const Object &py_object) {
  auto py_str = py_object.Str();
  os << PyUnicode_AsUTF8(py_str.Ptr());
  return os;
}

/// Stores information on a raised Python exception.
/// @sa FetchError
struct [[nodiscard]] ExceptionInfo final {
  /// Type of the exception, if nullptr there is no exception.
  Object type;
  /// Optional value of the exception.
  Object value;
  /// Optional traceback of the exception.
  Object traceback;
};

/// Format ExceptionInfo as a string just like the Python interpreter would. The
/// argument `skip_first_line` allows the user to skip the first line of the
/// traceback. It is useful if the first line in the traceback always prints
/// some internal wrapper function.
[[nodiscard]] inline std::string FormatException(const ExceptionInfo &exc_info, bool skip_first_line = false) {
  if (!exc_info.type) return "";
  Object traceback_mod(PyImport_ImportModule("traceback"));
  MG_ASSERT(traceback_mod);
  Object format_exception_fn(traceback_mod.GetAttr("format_exception"));
  MG_ASSERT(format_exception_fn);
  Object traceback_root(exc_info.traceback);
  if (skip_first_line && traceback_root) {
    traceback_root = traceback_root.GetAttr("tb_next");
  }
  auto list = format_exception_fn.Call(exc_info.type, exc_info.value ? exc_info.value.Ptr() : Py_None,
                                       traceback_root ? traceback_root.Ptr() : Py_None);
  MG_ASSERT(list);
  std::stringstream ss;
  auto len = PyList_GET_SIZE(list.Ptr());
  for (Py_ssize_t i = 0; i < len; ++i) {
    auto *py_str = PyList_GET_ITEM(list.Ptr(), i);
    ss << PyUnicode_AsUTF8(py_str);
  }
  return ss.str();
}

/// Write ExceptionInfo to stream just like the Python interpreter would.
inline std::ostream &operator<<(std::ostream &os, const ExceptionInfo &exc_info) {
  os << FormatException(exc_info);
  return os;
}

/// Get the current exception info and clear the current exception indicator.
///
/// NOTE: This function must be used with caution because it returns the
/// exception information as real Python objects. The returned objects will have
/// references to the current objects on the Python frame in them. That could
/// cause unintentional lifetime extension of objects that you potentially want
/// to destroy or, even worse, that you have already destroyed.
///
/// This is normally used to catch and handle exceptions via C API.
[[nodiscard]] inline std::optional<ExceptionInfo> FetchError() {
  PyObject *exc_type, *exc_value, *traceback;
  PyErr_Fetch(&exc_type, &exc_value, &traceback);
  if (!exc_type) return std::nullopt;
  PyErr_NormalizeException(&exc_type, &exc_value, &traceback);
  return ExceptionInfo{Object(exc_type), Object(exc_value), Object(traceback)};
}

inline void RestoreError(ExceptionInfo exc_info) {
  PyErr_Restore(exc_info.type.Steal(), exc_info.value.Steal(), exc_info.traceback.Steal());
}

/// Append `dir` to Python's `sys.path`.
///
/// The function does not check whether the directory exists, or is readable.
/// ExceptionInfo is returned if an error occurred.
[[nodiscard]] inline std::optional<ExceptionInfo> AppendToSysPath(const char *dir) {
  MG_ASSERT(dir);
  auto *py_path = PySys_GetObject("path");
  MG_ASSERT(py_path);
  py::Object import_dir(PyUnicode_FromString(dir));
  if (!import_dir) return py::FetchError();
  int import_dir_in_path = PySequence_Contains(py_path, import_dir.Ptr());
  if (import_dir_in_path == -1) return py::FetchError();
  if (import_dir_in_path == 1) return std::nullopt;
  if (PyList_Append(py_path, import_dir.Ptr()) == -1) return py::FetchError();
  return std::nullopt;
}

}  // namespace memgraph::py
