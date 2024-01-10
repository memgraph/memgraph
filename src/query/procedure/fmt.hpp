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

#pragma once

#include <fmt/ostream.h>

#include "mg_procedure.h"
#include "utils/logging.hpp"

template <>
class fmt::formatter<mgp_log_level> {
 public:
  constexpr auto parse(format_parse_context &ctx) { return ctx.begin(); }
  template <typename Context>
  constexpr auto format(mgp_log_level const &log_level, Context &ctx) const {
    switch (log_level) {
      case mgp_log_level::MGP_LOG_LEVEL_CRITICAL:
        return fmt::format_to(ctx.out(), "{}", "CRITICAL");
      case mgp_log_level::MGP_LOG_LEVEL_ERROR:
        return fmt::format_to(ctx.out(), "{}", "ERROR");
      case mgp_log_level::MGP_LOG_LEVEL_WARN:
        return fmt::format_to(ctx.out(), "{}", "WARN");
      case mgp_log_level::MGP_LOG_LEVEL_INFO:
        return fmt::format_to(ctx.out(), "{}", "INFO");
      case mgp_log_level::MGP_LOG_LEVEL_DEBUG:
        return fmt::format_to(ctx.out(), "{}", "DEBUG");
      case mgp_log_level::MGP_LOG_LEVEL_TRACE:
        return fmt::format_to(ctx.out(), "{}", "TRACE");
    }
    LOG_FATAL("Format of a wrong mgp_log_level -> check missing switch case");
  }
};

template <>
class fmt::formatter<mgp_error> {
 public:
  constexpr auto parse(format_parse_context &ctx) { return ctx.begin(); }
  template <typename Context>
  constexpr auto format(mgp_error const &error, Context &ctx) const {
    switch (error) {
      case mgp_error::MGP_ERROR_NO_ERROR:
        return fmt::format_to(ctx.out(), "{}", "NO ERROR");
      case mgp_error::MGP_ERROR_UNKNOWN_ERROR:
        return fmt::format_to(ctx.out(), "{}", "UNKNOWN ERROR");
      case mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE:
        return fmt::format_to(ctx.out(), "{}", "UNABLE TO ALLOCATE ERROR");
      case mgp_error::MGP_ERROR_INSUFFICIENT_BUFFER:
        return fmt::format_to(ctx.out(), "{}", "INSUFFICIENT BUFFER ERROR");
      case mgp_error::MGP_ERROR_OUT_OF_RANGE:
        return fmt::format_to(ctx.out(), "{}", "OUT OF RANGE ERROR");
      case mgp_error::MGP_ERROR_LOGIC_ERROR:
        return fmt::format_to(ctx.out(), "{}", "LOGIC ERROR");
      case mgp_error::MGP_ERROR_DELETED_OBJECT:
        return fmt::format_to(ctx.out(), "{}", "DELETED OBJECT ERROR");
      case mgp_error::MGP_ERROR_INVALID_ARGUMENT:
        return fmt::format_to(ctx.out(), "{}", "INVALID ARGUMENT ERROR");
      case mgp_error::MGP_ERROR_KEY_ALREADY_EXISTS:
        return fmt::format_to(ctx.out(), "{}", "KEY ALREADY EXISTS ERROR");
      case mgp_error::MGP_ERROR_IMMUTABLE_OBJECT:
        return fmt::format_to(ctx.out(), "{}", "IMMUTABLE OBJECT ERROR");
      case mgp_error::MGP_ERROR_VALUE_CONVERSION:
        return fmt::format_to(ctx.out(), "{}", "VALUE CONVERSION ERROR");
      case mgp_error::MGP_ERROR_SERIALIZATION_ERROR:
        return fmt::format_to(ctx.out(), "{}", "SERIALIZATION ERROR");
      case mgp_error::MGP_ERROR_AUTHORIZATION_ERROR:
        return fmt::format_to(ctx.out(), "{}", "AUTHORIZATION ERROR");
    }
    LOG_FATAL("Format of a wrong mgp_error -> check missing switch case");
  }
};
