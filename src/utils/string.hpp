#pragma once

#include <algorithm>
#include <cctype>
#include <string>

#include <iterator>
#include <sstream>
#include <string>
#include <vector>

#include "utils/exceptions.hpp"

namespace utils {

/**
 * Removes whitespace characters from the start and from the end of a string.
 *
 * @param str string that is going to be trimmed
 *
 * @return trimmed string
 */
inline std::string Trim(const std::string& s) {
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
inline std::string ToLowerCase(std::string s) {
  std::transform(s.begin(), s.end(), s.begin(),
                 [](char c) { return tolower(c); });
  return s;
}

/**
 * Return string with all uppercased characters (locale independent).
 */
inline std::string ToUpperCase(std::string s) {
  std::string s2(s.size(), ' ');
  std::transform(s.begin(), s.end(), s.begin(),
                 [](char c) { return toupper(c); });
  return s;
}

/**
 * Join strings in vector separated by a given separator.
 */
inline std::string Join(const std::vector<std::string>& strings,
                        const std::string& separator) {
  if (strings.size() == 0U) return "";
  int64_t total_size = 0;
  for (const auto& x : strings) {
    total_size += x.size();
  }
  total_size += separator.size() * (static_cast<int64_t>(strings.size()) - 1);
  std::string s;
  s.reserve(total_size);
  s += strings[0];
  for (auto it = strings.begin() + 1; it != strings.end(); ++it) {
    s += separator;
    s += *it;
  }
  return s;
}

/**
 * Replaces all occurences of <match> in <src> with <replacement>.
 */
// TODO: This could be implemented much more efficient.
inline std::string Replace(std::string src, const std::string& match,
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
inline std::vector<std::string> Split(const std::string &src,
                                      const std::string &delimiter = " ") {
  if (src.empty()) {
    return {};
  }
  size_t index = 0;
  size_t n = std::string::npos;
  std::vector<std::string> res;
  do {
    n = src.find(delimiter, index);
    auto word = src.substr(index, n - index);
    if (!word.empty()) res.push_back(word);
    index = n + delimiter.size();
  } while (n != std::string::npos);
  return res;
}

/**
 * Parse double using classic locale, throws BasicException if it wasn't able to
 * parse whole string.
 */
inline double ParseDouble(const std::string& s) {
  // stod would be nicer but it uses current locale so we shouldn't use it.
  double t = 0.0;
  std::istringstream iss(s);
  iss.imbue(std::locale::classic());
  iss >> t;
  if (iss.fail() || !iss.eof()) {
    throw BasicException("Couldn't parse string");
  }
  return t;
}
}
