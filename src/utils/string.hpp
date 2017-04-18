#pragma once

#include <algorithm>
#include <cctype>
#include <string>

#include <iterator>
#include <sstream>
#include <string>
#include <vector>

namespace utils {

/**
 * Removes whitespace characters from the start and from the end of a string.
 *
 * @param str string that is going to be trimmed
 *
 * @return trimmed string
 */
std::string Trim(const std::string& s) {
  auto begin = s.begin();
  auto end = s.end();
  if (begin == end) {
    // Need to check this to be sure that prev(end) exists.
    return s;
  }
  while (begin != end && isspace(*begin)) {
    ++begin;
  }
  while (prev(end) != begin && isspace(*prev(end))) {
    --end;
  }
  return std::string(begin, end);
}

/**
 * Return string with all lowercased characters (locale independent).
 */
std::string ToLowerCase(std::string s) {
  std::transform(s.begin(), s.end(), s.begin(),
                 [](char c) { return tolower(c); });
  return s;
}

/**
 * Return string with all uppercased characters (locale independent).
 */
std::string ToUpperCase(std::string s) {
  std::string s2(s.size(), ' ');
  std::transform(s.begin(), s.end(), s.begin(),
                 [](char c) { return toupper(c); });
  return s;
}

/**
 * Join strings in vector separated by a given separator.
 */
std::string Join(const std::vector<std::string>& strings,
                 const char* separator) {
  std::ostringstream oss;
  std::copy(strings.begin(), strings.end(),
            std::ostream_iterator<std::string>(oss, separator));
  return oss.str();
}

/**
 * Replaces all occurences of <match> in <src> with <replacement>.
 */
// TODO: This could be implemented much more efficient.
std::string Replace(std::string src, const std::string& match,
                    const std::string& replacement) {
  for (size_t pos = src.find(match); pos != std::string::npos;
       pos = src.find(match, pos + replacement.size())) {
    src.erase(pos, match.length()).insert(pos, replacement);
  }
  return src;
}

/**
 * Split string by delimeter and return vector of results.
 */
std::vector<std::string> Split(const std::string& src,
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
}
