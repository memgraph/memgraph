#include "utils/string/transform.hpp"

namespace utils {

// TODO CPPCheck -> function never used
void str_tolower(std::string& s) {
  //  en_US.utf8 localization
  std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) {
    return std::tolower(c, std::locale("en_US.utf8"));
  });
}
}
