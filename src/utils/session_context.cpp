// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "utils/session_context.hpp"

#include <iterator>

namespace memgraph::logging {

namespace {
// Append "<value>" as a self-delimiting field: `"` and `\` are escaped, and
// newlines/tabs become escape sequences so the field never spans log lines.
void AppendQuoted(fmt::memory_buffer &out, std::string_view value) {
  out.push_back('"');
  for (const char c : value) {
    switch (c) {
      case '"':
        out.append(std::string_view{"\\\""});
        break;
      case '\\':
        out.append(std::string_view{"\\\\"});
        break;
      case '\n':
        out.append(std::string_view{"\\n"});
        break;
      case '\r':
        out.append(std::string_view{"\\r"});
        break;
      case '\t':
        out.append(std::string_view{"\\t"});
        break;
      default:
        out.push_back(c);
    }
  }
  out.push_back('"');
}
}  // namespace

void EmitSlowQueryLog(std::string_view user, std::string_view db, std::string_view query, int64_t duration_ms,
                      std::optional<std::string_view> plan) {
  fmt::memory_buffer buf;
  fmt::format_to(std::back_inserter(buf), "[slow-query] duration_ms={} user={} db={} query=", duration_ms, user, db);
  AppendQuoted(buf, query);
  if (plan.has_value()) {
    fmt::format_to(std::back_inserter(buf), "\nPLAN:\n");
    // Indent each non-empty plan line by two spaces.
    std::string_view rest = *plan;
    while (!rest.empty()) {
      auto nl = rest.find('\n');
      auto line = nl == std::string_view::npos ? rest : rest.substr(0, nl);
      if (!line.empty()) {
        buf.push_back(' ');
        buf.push_back(' ');
        buf.append(line.data(), line.data() + line.size());
      }
      if (nl == std::string_view::npos) break;
      buf.push_back('\n');
      rest.remove_prefix(nl + 1);
    }
  }
  spdlog::warn(std::string_view{buf.data(), buf.size()});
}

void EmitFailedQueryLog(std::string_view user, std::string_view db, std::string_view query, std::string_view error) {
  fmt::memory_buffer buf;
  fmt::format_to(std::back_inserter(buf), "[failed-query] user={} db={} error=", user, db);
  AppendQuoted(buf, error);
  fmt::format_to(std::back_inserter(buf), " query=");
  AppendQuoted(buf, query);
  spdlog::error(std::string_view{buf.data(), buf.size()});
}

}  // namespace memgraph::logging
