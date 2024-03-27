// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/** @file */
#pragma once

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <random>
#include <regex>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "utils/exceptions.hpp"

namespace memgraph::utils {

/** Remove whitespace characters from the start of a string. */
inline std::string_view LTrim(const std::string_view s) {
  size_t start = 0;
  while (start < s.size() && isspace(s[start])) {
    ++start;
  }
  return std::string_view(s.data() + start, s.size() - start);
}

/** Remove characters found in `chars` from the start of a string. */
inline std::string_view LTrim(const std::string_view s, const std::string_view chars) {
  size_t start = 0;
  while (start < s.size() && chars.find(s[start]) != std::string::npos) {
    ++start;
  }
  return std::string_view(s.data() + start, s.size() - start);
}

/** Remove whitespace characters from the end of a string. */
inline std::string_view RTrim(const std::string_view s) {
  size_t count = s.size();
  while (count > static_cast<size_t>(0) && isspace(s[count - 1])) {
    --count;
  }
  return std::string_view(s.data(), count);
}

/** Remove characters found in `chars` from the end of a string. */
inline std::string_view RTrim(const std::string_view s, const std::string_view chars) {
  size_t count = s.size();
  while (count > static_cast<size_t>(0) && chars.find(s[count - 1]) != std::string::npos) {
    --count;
  }
  return std::string_view(s.data(), count);
}

/** Remove whitespace characters from the start and from the end of a string. */
inline std::string_view Trim(const std::string_view s) {
  size_t start = 0;
  size_t count = s.size();
  while (start < s.size() && isspace(s[start])) {
    ++start;
  }
  while (count > start && isspace(s[count - 1])) {
    --count;
  }
  return std::string_view(s.data() + start, count - start);
}

/** Remove characters found in `chars` from the start and the end of `s`. */
inline std::string_view Trim(const std::string_view s, const std::string_view chars) {
  size_t start = 0;
  size_t count = s.size();
  while (start < s.size() && chars.find(s[start]) != std::string::npos) {
    ++start;
  }
  while (count > start && chars.find(s[count - 1]) != std::string::npos) {
    --count;
  }
  return std::string_view(s.data() + start, count - start);
}

/**
 * Lowercase all characters of a string and store the result in `out`.
 * Transformation is locale independent.
 * @return pointer to `out`.
 */
template <class TAllocator>
std::basic_string<char, std::char_traits<char>, TAllocator> *ToLowerCase(
    std::basic_string<char, std::char_traits<char>, TAllocator> *out, const std::string_view s) {
  out->resize(s.size());
  std::transform(s.begin(), s.end(), out->begin(), [](char c) { return tolower(c); });
  return out;
}

/**
 * Lowercase all characters of a string.
 * Transformation is locale independent.
 */
inline std::string ToLowerCase(const std::string_view s) {
  std::string res;
  ToLowerCase(&res, s);
  return res;
}

/**
 * Uppercase all characters of a string and store the result in `out`.
 * Transformation is locale independent.
 * @return pointer to `out`.
 */
template <class TAllocator>
std::basic_string<char, std::char_traits<char>, TAllocator> *ToUpperCase(
    std::basic_string<char, std::char_traits<char>, TAllocator> *out, const std::string_view s) {
  out->resize(s.size());
  std::transform(s.begin(), s.end(), out->begin(), [](char c) { return toupper(c); });
  return out;
}

/**
 * Uppercase all characters of a string and store the result in `out`.
 * Transformation is locale independent.
 */
inline std::string ToUpperCase(const std::string_view s) {
  std::string res;
  ToUpperCase(&res, s);
  return res;
}

/**
 * Join the `strings` collection separated by a given separator into `out`.
 * @return pointer to `out`.
 */
template <class TCollection, class TAllocator>
std::basic_string<char, std::char_traits<char>, TAllocator> *Join(
    std::basic_string<char, std::char_traits<char>, TAllocator> *out, const TCollection &strings,
    const std::string_view separator) {
  out->clear();
  if (strings.empty()) return out;
  int64_t total_size = 0;
  for (const auto &x : strings) {
    total_size += x.size();
  }
  total_size += separator.size() * (static_cast<int64_t>(strings.size()) - 1);
  out->reserve(total_size);
  *out += strings[0];
  for (auto it = strings.begin() + 1; it != strings.end(); ++it) {
    *out += separator;
    *out += *it;
  }
  return out;
}

/**
 * Join the `strings` collection separated by a given separator.
 */
inline std::string Join(const std::vector<std::string> &strings, const std::string_view separator) {
  std::string res;
  Join(&res, strings, separator);
  return res;
}

/**
 * Replace all occurrences of `match` in `src` with `replacement`.
 * @return pointer to `out`.
 */
template <class TAllocator>
std::basic_string<char, std::char_traits<char>, TAllocator> *Replace(
    std::basic_string<char, std::char_traits<char>, TAllocator> *out, const std::string_view src,
    const std::string_view match, const std::string_view replacement) {
  // TODO: This could be implemented much more efficiently.
  *out = src;
  for (size_t pos = out->find(match); pos != std::string::npos; pos = out->find(match, pos + replacement.size())) {
    out->erase(pos, match.length()).insert(pos, replacement);
  }
  return out;
}

/** Replace all occurrences of `match` in `src` with `replacement`. */
inline std::string Replace(const std::string_view src, const std::string_view match,
                           const std::string_view replacement) {
  std::string res;
  Replace(&res, src, match, replacement);
  return res;
}

/**
 * Split a string by `delimiter` with a maximum of `splits` into a vector.
 * The vector will have at most `splits` + 1 elements. Negative value of
 * `splits` indicates to perform all possible splits.
 * @return pointer to `out`.
 */
template <class TString, class TAllocator>
std::vector<TString, TAllocator> *Split(std::vector<TString, TAllocator> *out, const std::string_view src,
                                        const std::string_view delimiter, int splits = -1) {
  out->clear();
  if (src.empty()) return out;
  size_t index = 0;
  while (splits < 0 || splits-- != 0) {
    auto n = src.find(delimiter, index);
    if (n == std::string::npos) break;
    out->emplace_back(src.substr(index, n - index));
    index = n + delimiter.size();
  }
  out->emplace_back(src.substr(index));
  return out;
}

/**
 * Split a string by `delimiter` with a maximum of `splits` into a vector.
 * The vector will have at most `splits` + 1 elements. Negative value of
 * `splits` indicates to perform all possible splits.
 */
inline std::vector<std::string> Split(const std::string_view src, const std::string_view delimiter, int splits = -1) {
  std::vector<std::string> res;
  Split(&res, src, delimiter, splits);
  return res;
}

inline std::vector<std::string_view> SplitView(const std::string_view src, const std::string_view delimiter,
                                               int splits = -1) {
  std::vector<std::string_view> res;
  Split(&res, src, delimiter, splits);
  return res;
}

/**
 * Split a string by whitespace into a vector.
 * Runs of consecutive whitespace are regarded as a single delimiter.
 * Additionally, the result will not contain empty strings at the start or end
 * as if the string was trimmed before splitting.
 * @return pointer to `out`.
 */
template <class TString, class TAllocator>
std::vector<TString, TAllocator> *Split(std::vector<TString, TAllocator> *out, const std::string_view src) {
  out->clear();
  if (src.empty()) return out;
  // TODO: Investigate how much regex allocate and perhaps replace with custom
  // solution doing no allocations.
  static std::regex not_whitespace("[^\\s]+");
  auto matches_begin = std::cregex_iterator(src.data(), src.data() + src.size(), not_whitespace);
  auto matches_end = std::cregex_iterator();
  out->reserve(std::distance(matches_begin, matches_end));
  for (auto match = matches_begin; match != matches_end; ++match) {
    std::string_view match_view(&src[match->position()], match->length());
    out->emplace_back(match_view);
  }
  return out;
}

/**
 * Split a string by whitespace into a vector.
 * Runs of consecutive whitespace are regarded as a single delimiter.
 * Additionally, the result will not contain empty strings at the start or end
 * as if the string was trimmed before splitting.
 */
inline std::vector<std::string> Split(const std::string_view src) {
  std::vector<std::string> res;
  Split(&res, src);
  return res;
}

/**
 * Like `Split` but string is processed from right to left.
 * For example, RSplit("a.b.c.", ".", 1) results in {"a.b", "c"}.
 * The returned vector and its elements use `std::allocator<>`. The vector will
 * have at most `splits` + 1 elements. Negative value of `splits` indicates to
 * perform all possible splits.
 * @return pointer to `out`.
 */
template <class TString, class TAllocator>
std::vector<TString, TAllocator> *RSplit(std::vector<TString, TAllocator> *out, const std::string_view src,
                                         const std::string_view delimiter, int splits = -1) {
  out->clear();
  if (src.empty()) return out;
  size_t index = src.size();
  while (splits < 0 || splits-- != 0) {
    auto n = src.rfind(delimiter, index - 1);
    if (n == std::string::npos) break;
    out->emplace_back(src.substr(n + delimiter.size(), index - n - delimiter.size()));
    index = n;
    if (n == 0) break;
  }
  out->emplace_back(src.substr(0, index));
  std::reverse(out->begin(), out->end());
  return out;
}

/**
 * Like `Split` but string is processed from right to left.
 * For example, RSplit("a.b.c.", ".", 1) results in {"a.b", "c"}.
 * The returned vector and its elements use `std::allocator<>`. The vector will
 * have at most `splits` + 1 elements. Negative value of `splits` indicates to
 * perform all possible splits.
 */
inline std::vector<std::string> RSplit(const std::string_view src, const std::string_view delimiter, int splits = -1) {
  std::vector<std::string> res;
  RSplit(&res, src, delimiter, splits);
  return res;
}

/**
 * Parse a signed integer value from a string using classic locale.
 * Note, the current implementation copies the given string which may perform a
 * heap allocation if the string is big enough.
 *
 * @throw BasicException if unable to parse the whole string.
 */
inline int64_t ParseInt(const std::string_view s) {
  // stol would be nicer but it uses current locale so we shouldn't use it.
  int64_t t = 0;
  // NOTE: Constructing std::istringstream will make a copy of the string, which
  // may make a heap allocation if string is large enough. There is no
  // std::istringstream constructor accepting a custom allocator. We could pass
  // a std::basic_string with a custom allocator, but std::istringstream will
  // probably invoke
  // std::allocator_traits<>::select_on_container_copy_construction which
  // doesn't really help as most allocators default to global new/delete
  // allocator.
  std::istringstream iss(std::string(s.data(), s.size()));
  iss.imbue(std::locale::classic());
  iss >> t;
  if (iss.fail() || !iss.eof()) {
    throw BasicException("Couldn't parse string");
  }
  return t;
}

inline uint64_t ParseStringToUint64(const std::string_view s) {
  if (uint64_t value = 0; std::from_chars(s.data(), s.data() + s.size(), value).ec == std::errc{}) {
    return value;
  }
  throw utils::ParseException(s);
}

inline uint32_t ParseStringToUint32(const std::string_view s) {
  if (uint32_t value = 0; std::from_chars(s.data(), s.data() + s.size(), value).ec == std::errc{}) {
    return value;
  }
  throw utils::ParseException(s);
}

/**
 * Parse a double floating point value from a string using classic locale.
 * Note, the current implementation copies the given string which may perform a
 * heap allocation if the string is big enough.
 *
 * @throw BasicException if unable to parse the whole string.
 */
inline double ParseDouble(const std::string_view s) {
  // stod would be nicer but it uses current locale so we shouldn't use it.
  double t = 0.0;
  // NOTE: Constructing std::istringstream will make a copy of the string, which
  // may make a heap allocation if string is large enough. There is no
  // std::istringstream constructor accepting a custom allocator. We could pass
  // a std::basic_string with a custom allocator, but std::istringstream will
  // probably invoke
  // std::allocator_traits<>::select_on_container_copy_construction which
  // doesn't really help as most allocators default to global new/delete
  // allocator.
  std::istringstream iss(std::string(s.data(), s.size()));
  iss.imbue(std::locale::classic());
  iss >> t;
  if (iss.fail() || !iss.eof()) {
    throw BasicException("Couldn't parse string");
  }
  return t;
}

/** Check if the given string `s` ends with the given `suffix`. */
inline bool EndsWith(const std::string_view s, const std::string_view suffix) {
  return s.size() >= suffix.size() && s.compare(s.size() - suffix.size(), std::string::npos, suffix) == 0;
}

/** Check if the given string `s` starts with the given `prefix`. */
inline bool StartsWith(const std::string_view s, const std::string_view prefix) {
  return s.size() >= prefix.size() && s.compare(0, prefix.size(), prefix) == 0;
}

/** Perform case-insensitive string equality test. */
inline bool IEquals(const std::string_view lhs, const std::string_view rhs) {
  if (lhs.size() != rhs.size()) return false;
  for (size_t i = 0; i < lhs.size(); ++i) {
    if (tolower(lhs[i]) != tolower(rhs[i])) return false;
  }
  return true;
}

/**
 * Create a random alphanumeric string of the given length.
 * @return pointer to `out`.
 */
template <class TAllocator>
std::basic_string<char, std::char_traits<char>, TAllocator> *RandomString(
    std::basic_string<char, std::char_traits<char>, TAllocator> *out, size_t length) {
  static const char charset[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";
  static thread_local std::mt19937 pseudo_rand_gen{std::random_device{}()};
  static thread_local std::uniform_int_distribution<size_t> rand_dist{0, strlen(charset) - 1};
  out->resize(length);
  for (size_t i = 0; i < length; ++i) (*out)[i] = charset[rand_dist(pseudo_rand_gen)];
  return out;
}

/** Create a random alphanumeric string of the given length. */
inline std::string RandomString(size_t length) {
  std::string res;
  RandomString(&res, length);
  return res;
}

/**
 * Escape all whitespace and quotation characters in the given string.
 * @return pointer to `out`.
 */
template <class TAllocator>
std::basic_string<char, std::char_traits<char>, TAllocator> *Escape(
    std::basic_string<char, std::char_traits<char>, TAllocator> *out, const std::string_view src) {
  out->clear();
  out->reserve(src.size() + 2);
  out->append(1, '"');
  for (auto c : src) {
    if (c == '\\' || c == '\'' || c == '"') {
      out->append(1, '\\');
      out->append(1, c);
    } else if (c == '\b') {
      out->append("\\b");
    } else if (c == '\f') {
      out->append("\\f");
    } else if (c == '\n') {
      out->append("\\n");
    } else if (c == '\r') {
      out->append("\\r");
    } else if (c == '\t') {
      out->append("\\t");
    } else {
      out->append(1, c);
    }
  }
  out->append(1, '"');
  return out;
}

/** Escape all whitespace and quotation characters in the given string. */
inline std::string Escape(const std::string_view src) {
  std::string res;
  Escape(&res, src);
  return res;
}

/**
 * Return a view into substring [pos, pos+count).
 * If the bounds extend past the length of the string then both sides are
 * clamped to a valid interval. Therefore, this function never throws
 * std::out_of_range, unlike std::basic_string::substr.
 */
inline std::string_view Substr(const std::string_view string, size_t pos = 0, size_t count = std::string::npos) {
  if (pos >= string.size()) return std::string_view(string.data(), 0);
  auto len = std::min(string.size() - pos, count);
  return string.substr(pos, len);
}

/**
 * Convert a double value to a string representation.
 * Precision of converted value is 16.
 * Function also removes trailing zeros after the dot.
 *
 * @param value The double value to be converted.
 *
 * @return The string representation of the double value.
 *
 * @throws None
 */
inline std::string DoubleToString(const double value) {
  static const int PRECISION = 15;

  std::stringstream ss;
  ss << std::setprecision(PRECISION) << std::fixed << value;
  auto sv = ss.view();

  // Because of setprecision and fixed manipulator we are guaranteed to have the dot
  sv = sv.substr(0, sv.find_last_not_of('0') + 1);
  if (sv.ends_with('.')) {
    sv = sv.substr(0, sv.size() - 1);
  }
  return std::string(sv);
}

}  // namespace memgraph::utils
