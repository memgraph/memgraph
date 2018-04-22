#pragma once

#include <utility>

#include "glog/logging.h"

#include <ext/aligned_buffer.h>

namespace utils {

/**
 * @class Placeholder
 *
 * @brief
 * Placeholder is used to allocate memory for an object on heap providing
 * methods for setting and getting the object and making sure that the
 * object is initialized.
 *
 * @tparam T type of object to be wrapped in the placeholder
 */

template <class T>
class Placeholder {
 public:
  Placeholder() = default;

  Placeholder(Placeholder &) = delete;
  Placeholder(Placeholder &&) = delete;

  /**
   * The destructor automatically calls the wrapped objects destructor.
   */
  ~Placeholder() {
    if (initialized) get().~T();
  };

  /**
   * @return returns true if object is set in memory otherwise false.
   */
  bool is_initialized() { return initialized; }

  T &get() noexcept {
    DCHECK(initialized) << "Placeholder object not initialized";
    return *data._M_ptr();
  }

  /**
   * @return const reference to object.
   */
  const T &get() const noexcept {
    DCHECK(initialized) << "Placeholder object not initialized";
    return *data._M_ptr();
  }

  /**
   * Sets item in allocated memory and sets the initialized flag.
   *
   * @param T& item reference to the item initialized in allocated memory
   */
  void set(const T &item) {
    DCHECK(!initialized) << "Placeholder object already initialized";
    new (data._M_addr()) T(item);
    initialized = true;
  }

  /**
   * Moves item to allocated memory and sets initialized flag.1
   *
   * @param T&& rvalue reference to the item which is moved to allocated memory
   */
  void set(T &&item) {
    DCHECK(!initialized) << "Placeholder object already initialized";
    new (data._M_addr()) T(std::move(item));
    initialized = true;
  }

  /**
   * Emplaces item to allocated memory and calls the Constructor with specified
   * arguments.
   *
   * @tparam Args type of arguments to be passed to the objects constructor.
   * @param Parameters passed to the objects constructor.
   */
  template <class... Args>
  void emplace(Args &&... args) {
    DCHECK(!initialized) << "Placeholder object already initialized";
    new (data._M_addr()) T(args...);
    initialized = true;
  }

 private:
  // libstd aligned buffer struct
  __gnu_cxx::__aligned_buffer<T> data;
  bool initialized = false;
};

}  // namespace utils
