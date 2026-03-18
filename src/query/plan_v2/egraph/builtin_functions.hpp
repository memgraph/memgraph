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

#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace memgraph::query::plan::v2 {

/// Hot-path classification for built-in functions.
///
/// Cached on the e-graph's per-function-id record so the cost-model and
/// estimator dispatch is an integer compare instead of a string compare on
/// every Function alt.  `Unknown` covers UDFs and any name that doesn't
/// resolve to a builtin we model.
enum class BuiltinKind : std::uint8_t {
  Unknown,  ///< UDFs / unrecognised builtins; estimator falls back to default.
  Range,    ///< Cypher range(start, end[, step]); estimator deduces from
            ///< constant int args.
};

/// Classify a function name into a BuiltinKind.  Case-insensitive on the
/// builtin set; everything else is Unknown.
inline auto BuiltinKindFor(std::string_view name) -> BuiltinKind {
  // Case-insensitive compare against the small static set of recognised
  // builtins.  Adding a new builtin is one entry here plus an estimator case.
  auto eq_ci = [name](std::string_view candidate) {
    if (name.size() != candidate.size()) return false;
    for (std::size_t i = 0; i < name.size(); ++i) {
      auto a = name[i];
      auto b = candidate[i];
      if (a >= 'A' && a <= 'Z') a = static_cast<char>(a - 'A' + 'a');
      if (b >= 'A' && b <= 'Z') b = static_cast<char>(b - 'A' + 'a');
      if (a != b) return false;
    }
    return true;
  };
  // TODO: we know candidate is lower case eg, "range" no need to do per charater lowercase
  if (eq_ci("range")) return BuiltinKind::Range;
  return BuiltinKind::Unknown;
}

/// What we cache per interned function name.  Storing the exact source name
/// (not the lowercased form) so the Builder can recreate the AST `Function`
/// with the user's spelling.
struct FunctionInfo {
  std::string name;
  BuiltinKind kind;
};

}  // namespace memgraph::query::plan::v2
