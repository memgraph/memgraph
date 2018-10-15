#pragma once

#include <algorithm>
#include <cctype>
#include <cstring>
#include <iostream>
#include <iterator>
#include <random>
#include <regex>
#include <sstream>
#include <string>
#include <vector>

#include "utils/exceptions.hpp"

namespace utils {

/**
 * Removes whitespace characters from the start of a string.
 *
 * @param str string that is going to be trimmed
 *
 * @return trimmed string
 */
inline std::string LTrim(const std::string &s) {
  auto begin = s.begin();
  auto end = s.end();
  if (begin == end) {
    // Need to check this to be sure that std::prev(end) exists.
    return s;
  }
  while (begin < end && isspace(*begin)) {
    ++begin;
  }
  return std::string(begin, end);
}

/**
 * Removes characters contained in chars from the start of a string.
 *
 * @param s string that is going to be trimmed
 * @param chars string that contains chars that are to be removed
 *
 * @return trimmed string
 */
inline std::string LTrim(const std::string &s, const std::string &chars) {
  auto begin = s.begin();
  auto end = s.end();
  if (begin == end) {
    // Need to check this to be sure that std::prev(end) exists.
    return s;
  }
  while (begin < end && chars.find(*begin) != std::string::npos) {
    ++begin;
  }
  return std::string(begin, end);
}

/**
 * Removes whitespace characters from the end of a string.
 *
 * @param str string that is going to be trimmed
 *
 * @return trimmed string
 */
inline std::string RTrim(const std::string &s) {
  auto begin = s.begin();
  auto end = s.end();
  if (begin == end) {
    // Need to check this to be sure that std::prev(end) exists.
    return s;
  }
  while (end > begin && isspace(*std::prev(end))) {
    --end;
  }
  return std::string(begin, end);
}

/**
 * Removes characters contained in chars from the end of a string.
 *
 * @param s string that is going to be trimmed
 * @param chars string that contains chars that are to be removed
 *
 * @return trimmed string
 */
inline std::string RTrim(const std::string &s, const std::string &chars) {
  auto begin = s.begin();
  auto end = s.end();
  if (begin == end) {
    // Need to check this to be sure that std::prev(end) exists.
    return s;
  }
  while (end > begin && chars.find(*std::prev(end)) != std::string::npos) {
    --end;
  }
  return std::string(begin, end);
}

/**
 * Removes whitespace characters from the start and from the end of a string.
 *
 * @param str string that is going to be trimmed
 *
 * @return trimmed string
 */
inline std::string Trim(const std::string &s) {
  auto begin = s.begin();
  auto end = s.end();
  if (begin == end) {
    // Need to check this to be sure that std::prev(end) exists.
    return s;
  }
  while (begin < end && isspace(*begin)) {
    ++begin;
  }
  while (end > begin && isspace(*std::prev(end))) {
    --end;
  }
  return std::string(begin, end);
}

/**
 * Removes characters contained in chars from the start and the end of a string.
 *
 * @param s string that is going to be trimmed
 * @param chars string that contains chars that are to be removed
 *
 * @return trimmed string
 */
inline std::string Trim(const std::string &s, const std::string &chars) {
  auto begin = s.begin();
  auto end = s.end();
  if (begin == end) {
    // Need to check this to be sure that std::prev(end) exists.
    return s;
  }
  while (begin < end && chars.find(*begin) != std::string::npos) {
    ++begin;
  }
  while (end > begin && chars.find(*std::prev(end)) != std::string::npos) {
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
  std::transform(s.begin(), s.end(), s.begin(),
                 [](char c) { return toupper(c); });
  return s;
}

/**
 * Join strings in vector separated by a given separator.
 */
inline std::string Join(const std::vector<std::string> &strings,
                        const std::string &separator) {
  if (strings.size() == 0U) return "";
  int64_t total_size = 0;
  for (const auto &x : strings) {
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
// TODO: This could be implemented much more efficiently.
inline std::string Replace(std::string src, const std::string &match,
                           const std::string &replacement) {
  for (size_t pos = src.find(match); pos != std::string::npos;
       pos = src.find(match, pos + replacement.size())) {
    src.erase(pos, match.length()).insert(pos, replacement);
  }
  return src;
}

/**
 * Split string by delimeter and return vector of results.
 *
 * @param src - The string to split.
 * @param delimitier - The delimiter to split on.
 * @param splits - The maximum number of splits. For the given value N the
 * returned vector will contain at most (N + 1) elements. If given a negative
 * value, all possible splits are performed.
 * @return - a vector of splits.
 */
inline std::vector<std::string> Split(const std::string &src,
                                      const std::string &delimiter,
                                      int splits = -1) {
  std::vector<std::string> res;
  if (src.empty()) {
    return res;
  }
  size_t index = 0;
  while (splits < 0 || splits-- != 0) {
    auto n = src.find(delimiter, index);
    if (n == std::string::npos) break;
    res.emplace_back(src.substr(index, n - index));
    index = n + delimiter.size();
  }

  res.emplace_back(src.substr(index));
  return res;
}

/**
 * Split string by delimiter, from right to left, and return vector of results.
 * For example, RSplit("a.b.c.", ".", 1) results in {"a.b", "c"}.
 *
 * @param src - The string to split.
 * @param delimitier - The delimiter to split on.
 * @param splits - The maximum number of splits. For the given value N the
 * returned vector will contain at most (N + 1) elements. If given a negative
 * value, all possible splits are performed.
 */
inline std::vector<std::string> RSplit(const std::string &src,
                                       const std::string &delimiter,
                                       int splits = -1) {
  std::vector<std::string> res;
  if (src.empty()) {
    return res;
  }
  size_t index = src.size();
  while (splits < 0 || splits-- != 0) {
    auto n = src.rfind(delimiter, index - 1);
    if (n == std::string::npos) break;
    res.emplace_back(
        src.substr(n + delimiter.size(), index - n - delimiter.size()));
    index = n;
    if (n == 0) break;
  }

  res.emplace_back(src.substr(0, index));
  std::reverse(res.begin(), res.end());
  return res;
}

/**
 * Split string by whitespace and return vector of results.
 * Runs of consecutive whitespace are regarded as a single delimiter.
 * Additionally, the result will not contain empty strings at the start or end
 * as if the string was trimmed before splitting.
 */
inline std::vector<std::string> Split(const std::string &src) {
  if (src.empty()) {
    return {};
  }
  std::regex not_whitespace("[^\\s]+");
  auto matches_begin =
      std::sregex_iterator(src.begin(), src.end(), not_whitespace);
  auto matches_end = std::sregex_iterator();
  std::vector<std::string> res;
  res.reserve(std::distance(matches_begin, matches_end));
  for (auto match = matches_begin; match != matches_end; ++match) {
    res.emplace_back(match->str());
  }
  return res;
}

/**
 * Parse double using classic locale, throws BasicException if it wasn't able to
 * parse whole string.
 */
inline double ParseDouble(const std::string &s) {
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

/**
 * Checks if the given string `s` ends with the given `suffix`.
 */
inline bool EndsWith(const std::string &s, const std::string &suffix) {
  return s.size() >= suffix.size() &&
         s.compare(s.size() - suffix.size(), std::string::npos, suffix) == 0;
}

/**
 * Checks if the given string `s` starts with the given `prefix`.
 */
inline bool StartsWith(const std::string &s, const std::string &prefix) {
  return s.size() >= prefix.size() && s.compare(0, prefix.size(), prefix) == 0;
}

/**
 * Case-insensitive string comparison.
 */
inline bool IEquals(const std::string &lhs, const std::string &rhs) {
  if (lhs.size() != rhs.size()) return false;
  for (size_t i = 0; i < lhs.size(); ++i) {
    if (tolower(lhs[i]) != tolower(rhs[i])) return false;
  }
  return true;
}

/** Creates a random alphanumeric string of the given length. */
inline std::string RandomString(size_t length) {
  static const char charset[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";
  static thread_local std::mt19937 pseudo_rand_gen{std::random_device{}()};
  static thread_local std::uniform_int_distribution<size_t> rand_dist{
      0, strlen(charset) - 1};
  std::string str(length, 0);
  for (size_t i = 0; i < length; ++i)
    str[i] = charset[rand_dist(pseudo_rand_gen)];
  return str;
}

/// Escapes all whitespace and quotation characters to produce a string
/// which can be used as a string literal.
inline std::string Escape(const std::string &src) {
  std::string ret;
  ret.reserve(src.size() + 2);
  ret.append(1, '"');
  for (auto c : src) {
    if (c == '\\' || c == '\'' || c == '"') {
      ret.append(1, '\\');
      ret.append(1, c);
    } else if (c == '\b') {
      ret.append("\\b");
    } else if (c == '\f') {
      ret.append("\\f");
    } else if (c == '\n') {
      ret.append("\\n");
    } else if (c == '\r') {
      ret.append("\\r");
    } else if (c == '\t') {
      ret.append("\\t");
    } else {
      ret.append(1, c);
    }
  }
  ret.append(1, '"');
  return ret;
}
}  // namespace utils
