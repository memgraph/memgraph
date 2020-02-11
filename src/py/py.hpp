/// @file
/// Provides a C++ API for working with Python's original C API.
#pragma once

#include <ostream>
#include <string_view>
#include <optional>

// Define to use Py_ssize_t for API returning length of something. Some future
// Python version will only support Py_ssize_t, so it's best to always define
// this macro before including Python.h.
#define PY_SSIZE_T_CLEAN
#include <Python.h>

#if PY_MAJOR_VERSION != 3 || PY_MINOR_VERSION < 5
#error "Minimum supported Python API is 3.5"
#endif

#include <glog/logging.h>

namespace py {

/// Ensure the current thread is ready to call Python C API.
///
/// You must *not* try to ensure the GIL when the runtime is finalizing, as
/// that will terminate the thread. You may use `_Py_IsFinalizing` or
/// `sys.is_finalizing()` to check for such a case.
class EnsureGIL final {
  PyGILState_STATE gil_state_;

 public:
  EnsureGIL() : gil_state_(PyGILState_Ensure()) {}
  ~EnsureGIL() { PyGILState_Release(gil_state_); }
  EnsureGIL(const EnsureGIL &) = delete;
  EnsureGIL(EnsureGIL &&) = delete;
  EnsureGIL &operator=(const EnsureGIL &) = delete;
  EnsureGIL &operator=(EnsureGIL &&) = delete;
};

/// Owns a `PyObject *` and supports a more C++ idiomatic API to objects.
class Object final {
  PyObject *ptr_{nullptr};

 public:
  Object() = default;
  explicit Object(PyObject *ptr) noexcept : ptr_(ptr) {}

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

  operator PyObject *() const { return ptr_; }

  operator bool() const { return ptr_; }

  /// Release the ownership on this PyObject, i.e. we steal the reference.
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
  Object Call(const TArgs &... args) const {
    return Object(PyObject_CallFunctionObjArgs(
        ptr_, static_cast<PyObject *>(args)..., nullptr));
  }

  /// Equivalent to `obj.meth_name()` in Python.
  ///
  /// Returned Object is nullptr if an error occurred.
  /// @sa FetchError
  Object CallMethod(std::string_view meth_name) const {
    Object name(
        PyUnicode_FromStringAndSize(meth_name.data(), meth_name.size()));
    return Object(PyObject_CallMethodObjArgs(ptr_, name, nullptr));
  }

  /// Equivalent to `obj.meth_name(*args)` in Python.
  ///
  /// Returned Object is nullptr if an error occurred.
  /// @sa FetchError
  template <class... TArgs>
  Object CallMethod(std::string_view meth_name, const TArgs &... args) const {
    Object name(
        PyUnicode_FromStringAndSize(meth_name.data(), meth_name.size()));
    return Object(PyObject_CallMethodObjArgs(
        ptr_, name, static_cast<PyObject *>(args)..., nullptr));
  }
};

/// Write Object to stream as if `str(o)` was called in Python.
inline std::ostream &operator<<(std::ostream &os, const Object &py_object) {
  auto py_str = py_object.Str();
  os << PyUnicode_AsUTF8(py_str);
  return os;
}

/// Stores information on a raised Python exception.
/// @sa FetchError
struct ExceptionInfo final {
  /// Type of the exception, if nullptr there is no exception.
  Object type;
  /// Optional value of the exception.
  Object value;
  /// Optional traceback of the exception.
  Object traceback;
};

/// Write ExceptionInfo to stream just like the Python interpreter would.
inline std::ostream &operator<<(std::ostream &os,
                                const ExceptionInfo &exc_info) {
  if (!exc_info.type) return os;
  Object traceback_mod(PyImport_ImportModule("traceback"));
  CHECK(traceback_mod);
  Object format_exception_fn(
      PyObject_GetAttrString(traceback_mod, "format_exception"));
  CHECK(format_exception_fn);
  auto list = format_exception_fn.Call(exc_info.type, exc_info.value,
                                       exc_info.traceback);
  CHECK(list);
  auto len = PyList_GET_SIZE(static_cast<PyObject *>(list));
  for (Py_ssize_t i = 0; i < len; ++i) {
    auto *py_str = PyList_GET_ITEM(static_cast<PyObject *>(list), i);
    os << PyUnicode_AsUTF8(py_str);
  }
  return os;
}

/// Get the current exception info and clear the current exception indicator.
///
/// This is normally used to catch and handle exceptions via C API.
inline std::optional<ExceptionInfo> FetchError() {
  PyObject *exc_type, *exc_value, *traceback;
  PyErr_Fetch(&exc_type, &exc_value, &traceback);
  if (!exc_type) return std::nullopt;
  return ExceptionInfo{Object(exc_type), Object(exc_value), Object(traceback)};
}

}  // namespace py
