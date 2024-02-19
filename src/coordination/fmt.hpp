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

#include <libnuraft/nuraft.hxx>
#include "utils/logging.hpp"

inline std::string ToString(const nuraft::cmd_result_code &code) {
  switch (code) {
    case nuraft::cmd_result_code::OK:
      return "OK";
    case nuraft::cmd_result_code::FAILED:
      return "FAILED";
    case nuraft::cmd_result_code::RESULT_NOT_EXIST_YET:
      return "RESULT_NOT_EXIST_YET";
    case nuraft::cmd_result_code::TERM_MISMATCH:
      return "TERM_MISMATCH";
    case nuraft::cmd_result_code::SERVER_IS_LEAVING:
      return "SERVER_IS_LEAVING";
    case nuraft::cmd_result_code::CANNOT_REMOVE_LEADER:
      return "CANNOT_REMOVE_LEADER";
    case nuraft::cmd_result_code::SERVER_NOT_FOUND:
      return "SERVER_NOT_FOUND";
    case nuraft::cmd_result_code::SERVER_IS_JOINING:
      return "SERVER_IS_JOINING";
    case nuraft::cmd_result_code::CONFIG_CHANGING:
      return "CONFIG_CHANGING";
    case nuraft::cmd_result_code::SERVER_ALREADY_EXISTS:
      return "SERVER_ALREADY_EXISTS";
    case nuraft::cmd_result_code::BAD_REQUEST:
      return "BAD_REQUEST";
    case nuraft::cmd_result_code::NOT_LEADER:
      return "NOT_LEADER";
    case nuraft::cmd_result_code::TIMEOUT:
      return "TIMEOUT";
    case nuraft::cmd_result_code::CANCELLED:
      return "CANCELLED";
  }
  LOG_FATAL("ToString of a nuraft::cmd_result_code -> check missing switch case");
}
inline std::ostream &operator<<(std::ostream &os, const nuraft::cmd_result_code &code) {
  os << ToString(code);
  return os;
}
template <>
class fmt::formatter<nuraft::cmd_result_code> : public fmt::ostream_formatter {};
#endif
