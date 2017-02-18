#pragma once

#include <memory>

// ReferenceWrapper was created because std::reference_wrapper
// wasn't copyable
// Implementation has been taken from:
// http://en.cppreference.com/w/cpp/utility/functional/reference_wrapper
// TODO: once the c++ implementation will have proper implementation
// this class should be deleted and replaced with std::reference_wrapper

template <class T>
class ReferenceWrapper {
 public:
  // types
  typedef T type;

  // construct/copy/destroy
  ReferenceWrapper(T &ref) noexcept : _ptr(std::addressof(ref)) {}
  ReferenceWrapper(T &&) = delete;
  ReferenceWrapper(const ReferenceWrapper &) noexcept = default;

  // assignment
  ReferenceWrapper &operator=(const ReferenceWrapper &x) noexcept = default;

  // access
  operator T &() const noexcept { return *_ptr; }
  T &get() const noexcept { return *_ptr; }

  // template <class... ArgTypes>
  // typename std::result_of<T &(ArgTypes &&...)>::type
  // operator()(ArgTypes &&... args) const
  // {
  //     return std::invoke(get(), std::forward<ArgTypes>(args)...);
  // }

 private:
  T *_ptr;
};
