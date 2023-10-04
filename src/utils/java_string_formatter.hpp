// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <string>

#include "utils/exceptions.hpp"
#include "utils/pmr/string.hpp"
#include "utils/pmr/vector.hpp"

namespace memgraph::utils {

class JStringFormatException final : public BasicException {
 public:
  explicit JStringFormatException(const std::string &what) noexcept : BasicException(what) {}

  template <class... Args>
  explicit JStringFormatException(fmt::format_string<Args...> fmt, Args &&...args) noexcept
      : JStringFormatException(fmt::format(fmt, std::forward<Args>(args)...)) {}

  std::string name() const override { return "JStringFormatException"; }
};

template <typename T>
concept TTypedValueLike = requires(T t) {
  { t.ValueInt() } -> std::convertible_to<int>;
  { t.ValueDouble() } -> std::convertible_to<double>;
  { t.ValueString() } -> std::convertible_to<pmr::string>;
};

template <typename TString, typename TTypedValueLike>
class JStringFormatter final {
 public:
  [[nodiscard]] TString FormatString(TString str, const pmr::vector<TTypedValueLike> &format_args) const {
    std::size_t found{0U};
    std::size_t arg_index{0U};

    while (true) {
      found = str.find('%', found);
      if (found == std::string::npos) {
        break;
      }

      const bool ends_with_percentile = (found == str.size() - 1U);
      if (ends_with_percentile) {
        break;
      }

      const auto format_specifier = str.at(found + 1U);
      if (!std::isalpha(format_specifier)) {
        ++found;
        continue;
      }
      const bool does_argument_list_overflow = (format_args.size() < arg_index + 1U) && (arg_index > 0U);
      if (does_argument_list_overflow) {
        throw JStringFormatException(
            "There are more format specifiers in the CALL procedure error message, then arguments provided.");
      }
      const bool arg_count_exceeds_format_spec_count = (arg_index > format_args.size() - 1U);
      if (arg_count_exceeds_format_spec_count) {
        break;
      }

      ReplaceFormatSpecifier(str, found, format_specifier, format_args.at(arg_index));
      ++arg_index;
      ++found;
    }

    str.shrink_to_fit();
    return str;
  }

 private:
  void ReplaceFormatSpecifier(TString &str, std::size_t pos, char format_specifier, TTypedValueLike current_arg) const {
    std::string replacement_str;
    switch (format_specifier) {
      case 'd':
        replacement_str = std::to_string(current_arg.ValueInt());
        break;
      case 'f':
        replacement_str = std::to_string(current_arg.ValueDouble());
        break;
      case 's':
        replacement_str = current_arg.ValueString();
        break;
      default:
        throw JStringFormatException("Format specifier %'{}', in CALL procedure is not supported.", format_specifier);
    }

    str.replace(pos, 2U, replacement_str);
  }
};

}  // namespace memgraph::utils
