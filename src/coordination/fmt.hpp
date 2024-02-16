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

#include <libnuraft/nuraft.hxx>
#include "utils/logging.hpp"

template <>
class fmt::formatter<nuraft::cmd_result_code> {
 public:
  constexpr auto parse(format_parse_context &ctx) { return ctx.begin(); }
  template <typename Context>
  constexpr auto format(nuraft::cmd_result_code const &result_code, Context &ctx) const {
    switch (result_code) {
      case nuraft::cmd_result_code::OK:
        return fmt::format_to(ctx.out(), "{}", "OK");
      case nuraft::cmd_result_code::FAILED:
        return fmt::format_to(ctx.out(), "{}", "FAILED");
      case nuraft::cmd_result_code::RESULT_NOT_EXIST_YET:
        return fmt::format_to(ctx.out(), "{}", "RESULT_NOT_EXIST_YET");
      case nuraft::cmd_result_code::TERM_MISMATCH:
        return fmt::format_to(ctx.out(), "{}", "TERM_MISMATCH");
      case nuraft::cmd_result_code::SERVER_IS_LEAVING:
        return fmt::format_to(ctx.out(), "{}", "SERVER_IS_LEAVING");
      case nuraft::cmd_result_code::CANNOT_REMOVE_LEADER:
        return fmt::format_to(ctx.out(), "{}", "CANNOT_REMOVE_LEADER");
      case nuraft::cmd_result_code::SERVER_NOT_FOUND:
        return fmt::format_to(ctx.out(), "{}", "SERVER_NOT_FOUND");
      case nuraft::cmd_result_code::SERVER_IS_JOINING:
        return fmt::format_to(ctx.out(), "{}", "SERVER_IS_JOINING");
      case nuraft::cmd_result_code::CONFIG_CHANGING:
        return fmt::format_to(ctx.out(), "{}", "CONFIG_CHANGING");
      case nuraft::cmd_result_code::SERVER_ALREADY_EXISTS:
        return fmt::format_to(ctx.out(), "{}", "SERVER_ALREADY_EXISTS");
      case nuraft::cmd_result_code::BAD_REQUEST:
        return fmt::format_to(ctx.out(), "{}", "BAD_REQUEST");
      case nuraft::cmd_result_code::NOT_LEADER:
        return fmt::format_to(ctx.out(), "{}", "NOT_LEADER");
      case nuraft::cmd_result_code::TIMEOUT:
        return fmt::format_to(ctx.out(), "{}", "TIMEOUT");
      case nuraft::cmd_result_code::CANCELLED:
        return fmt::format_to(ctx.out(), "{}", "CANCELLED");
    }
    LOG_FATAL("Format of a wrong nuraft::cmd_result_code -> check missing switch case");
  }
};
#endif
