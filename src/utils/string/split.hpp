#pragma once

#include <regex>
#include <vector>

namespace utils {

std::vector<std::string> split(const std::string& src,
                               const std::string& delimiter) {
  size_t index = 0;
  std::vector<std::string> res;
  size_t n = src.find(delimiter, index);

  while (n != std::string::npos) {
    n = src.find(delimiter, index);
    res.push_back(src.substr(index, n - index));
    index = n + delimiter.size();
  }

  return res;
}

// doesn't work with gcc even though it's only c++11...
// and it's slow as hell compared to the split implementation above
// (more powerful though)

std::vector<std::string> regex_split(const std::string& input,
                                     const std::string& regex) {
  auto rgx = std::regex(regex);

  std::sregex_token_iterator last, first{input.begin(), input.end(), rgx, -1};

  return {first, last};
}
}
