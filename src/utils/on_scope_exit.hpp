#pragma once

#include <functional>

namespace utils {

/**
 * Calls a function in it's destructor (on scope exit).
 *
 * Example usage:
 *
 * void long_function() {
 *     resource.enable();
 *     // long block of code, might throw an exception
 *     resource.disable(); // we want this to happen for sure, and function end
 * }
 *
 * Can be nicer and safer:
 *
 * void long_function() {
 *     resource.enable();
 *     OnScopeExit on_exit([&resource] { resource.disable(); });
 *     // long block of code, might trow an exception
 * }
 */
class OnScopeExit {
 public:
  explicit OnScopeExit(const std::function<void()> &function)
      : function_(function) {}
  ~OnScopeExit() { function_(); }

 private:
  std::function<void()> function_;
};

}  // namespace utils
