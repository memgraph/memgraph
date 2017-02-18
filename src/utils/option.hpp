#pragma once

#include <ext/aligned_buffer.h>
#include <cassert>
#include <cstring>
#include <utility>

// Optional object storage. It maybe has and maybe
// dosent have objet of type T.
template <class T>
class Option {
 public:
  Option() { std::memset(data._M_addr(), 0, sizeof(T)); }

  Option(T const &item) {
    new (data._M_addr()) T(item);
    initialized = true;
  }

  Option(T &&item) {
    new (data._M_addr()) T(std::move(item));
    initialized = true;
  }

  Option(const Option &other) {
    if (other.initialized) {
      new (data._M_addr()) T(other.get());
      initialized = true;
    } else {
      std::memset(data._M_addr(), 0, sizeof(T));
    }
  }

  // Containers from std which have strong exception
  // guarantees wont use move constructors and operators
  // wihtout noexcept. "Optimized C++,2016 , Kurt
  // Guntheroth, page: 142, title: Moving instances into
  // std::vector"
  Option(Option &&other) noexcept {
    if (other.initialized) {
      new (data._M_addr()) T(std::move(other.get()));
      other.initialized = false;
      initialized = true;
    } else {
      std::memset(data._M_addr(), 0, sizeof(T));
    }
  }

  ~Option() {
    if (initialized) {
      get().~T();
      initialized = false;
    }
  }

  Option &operator=(const Option &other) {
    if (initialized) {
      get().~T();
      initialized = false;
    }
    if (other.initialized) {
      new (data._M_addr()) T(other.get());
      initialized = true;
    }

    return *this;
  }

  Option &operator=(Option &&other) {
    if (initialized) {
      get().~T();
      initialized = false;
    }

    if (other.initialized) {
      new (data._M_addr()) T(std::move(other.get()));
      other.initialized = false;
      initialized = true;
    }

    return *this;
  }

  // True if object i present.
  bool is_present() const { return initialized; }

  T &get() noexcept {
    assert(initialized);
    return *data._M_ptr();
  }

  T &get_or(T &other) {
    if (is_present()) {
      return get();
    } else {
      return other;
    }
  }

  // Returns ref to object if present else other.
  T const &get_or(T const &other) const {
    if (is_present()) {
      return get();
    } else {
      return other;
    }
  }

  const T &get() const noexcept {
    assert(initialized);
    return *data._M_ptr();
  }

  template <class U>
  Option<U> map() {
    if (is_present()) {
      return Option<U>(U(take()));
    } else {
      return Option<U>();
    }
  }

  template <class U, class F>
  Option<U> map(F f) {
    if (is_present()) {
      return Option<U>(f(take()));
    } else {
      return Option<U>();
    }
  }

  template <class U, class F>
  U map_or(F f, U &&def) {
    if (is_present()) {
      return f(take());
    } else {
      return std::move(def);
    }
  }

  template <class U, class F>
  U call_or(F f, U &&def) {
    if (is_present()) {
      return f(get());
    } else {
      return std::move(def);
    }
  }

  T take() {
    assert(initialized);
    initialized = false;
    return std::move(*data._M_ptr());
  }

  // Takes if it exists otherwise returns given value.
  T take_or(T &&value) {
    if (initialized) {
      initialized = false;
      return std::move(*data._M_ptr());
    } else {
      return std::move(value);
    }
  }

  explicit operator bool() const { return initialized; }

 private:
  // Aligned buffer is here to ensure aligment for
  // data of type T. It isn't applicable to just put T
  // field because the field has to be able to be
  // uninitialized to fulfill the semantics of Option class.
  __gnu_cxx::__aligned_buffer<T> data;
  bool initialized = false;
};

template <class T>
auto make_option() {
  return Option<T>();
}

template <class T>
auto make_option(T &&data) {
  return Option<T>(std::move(data));
}

template <class T>
auto make_option_const(const T &&data) {
  return Option<const T>(std::move(data));
}

// HELPER FUNCTIONS
template <class R>
bool option_fill(Option<R> &o) {
  return o.is_present() && o.get().fill();
}
