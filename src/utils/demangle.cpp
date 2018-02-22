#include "utils/demangle.hpp"

#include <cxxabi.h>

namespace utils {

std::experimental::optional<std::string> Demangle(const char *mangled_name) {
  int s;
  char *type_name = abi::__cxa_demangle(mangled_name, nullptr, nullptr, &s);
  std::experimental::optional<std::string> ret = std::experimental::nullopt;
  if (s == 0) {
    ret = type_name;
    free(type_name);
  }
  return ret;
}

}  // namespace utils
