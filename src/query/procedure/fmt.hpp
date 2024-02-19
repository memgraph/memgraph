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

#if FMT_VERSION > 90000
#include <fmt/ostream.h>
#include <string>

#include "mg_procedure.h"
#include "utils/logging.hpp"

inline std::string ToString(const mgp_log_level &log_level) {
  switch (log_level) {
    case mgp_log_level::MGP_LOG_LEVEL_CRITICAL:
      return "CRITICAL";
    case mgp_log_level::MGP_LOG_LEVEL_ERROR:
      return "ERROR";
    case mgp_log_level::MGP_LOG_LEVEL_WARN:
      return "WARN";
    case mgp_log_level::MGP_LOG_LEVEL_INFO:
      return "INFO";
    case mgp_log_level::MGP_LOG_LEVEL_DEBUG:
      return "DEBUG";
    case mgp_log_level::MGP_LOG_LEVEL_TRACE:
      return "TRACE";
  }
  LOG_FATAL("ToString of a wrong mgp_log_level -> check missing switch case");
}
inline std::ostream &operator<<(std::ostream &os, const mgp_log_level &log_level) {
  os << ToString(log_level);
  return os;
}
template <>
class fmt::formatter<mgp_log_level> : public fmt::ostream_formatter {};

inline std::string ToString(const mgp_error &error) {
  switch (error) {
    case mgp_error::MGP_ERROR_NO_ERROR:
      return "NO ERROR";
    case mgp_error::MGP_ERROR_UNKNOWN_ERROR:
      return "UNKNOWN ERROR";
    case mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE:
      return "UNABLE TO ALLOCATE ERROR";
    case mgp_error::MGP_ERROR_INSUFFICIENT_BUFFER:
      return "INSUFFICIENT BUFFER ERROR";
    case mgp_error::MGP_ERROR_OUT_OF_RANGE:
      return "OUT OF RANGE ERROR";
    case mgp_error::MGP_ERROR_LOGIC_ERROR:
      return "LOGIC ERROR";
    case mgp_error::MGP_ERROR_DELETED_OBJECT:
      return "DELETED OBJECT ERROR";
    case mgp_error::MGP_ERROR_INVALID_ARGUMENT:
      return "INVALID ARGUMENT ERROR";
    case mgp_error::MGP_ERROR_KEY_ALREADY_EXISTS:
      return "KEY ALREADY EXISTS ERROR";
    case mgp_error::MGP_ERROR_IMMUTABLE_OBJECT:
      return "IMMUTABLE OBJECT ERROR";
    case mgp_error::MGP_ERROR_VALUE_CONVERSION:
      return "VALUE CONVERSION ERROR";
    case mgp_error::MGP_ERROR_SERIALIZATION_ERROR:
      return "SERIALIZATION ERROR";
    case mgp_error::MGP_ERROR_AUTHORIZATION_ERROR:
      return "AUTHORIZATION ERROR";
  }
  LOG_FATAL("ToString of a wrong mgp_error -> check missing switch case");
}
inline std::ostream &operator<<(std::ostream &os, const mgp_error &error) {
  os << ToString(error);
  return os;
}
template <>
class fmt::formatter<mgp_error> : public fmt::ostream_formatter {};
#endif
