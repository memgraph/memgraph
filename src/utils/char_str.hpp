#pragma once

#include <cstring>
#include "utils/total_ordering.hpp"

class CharStr : public TotalOrdering<CharStr> {
 public:
  CharStr(const char *str) : str(str) {}

  std::string to_string() const { return std::string(str); }

  friend bool operator==(const CharStr &lhs, const CharStr &rhs) {
    return strcmp(lhs.str, rhs.str) == 0;
  }

  friend bool operator<(const CharStr &lhs, const CharStr &rhs) {
    return strcmp(lhs.str, rhs.str) < 0;
  }

 private:
  const char *str;
};
