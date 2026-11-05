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

/// Classification for built-in functions whose result the planner reasons
/// about.
///
/// Cached on the e-graph's per-function-id record so deriving a builtin's facts
/// is an integer compare rather than a string compare on every refresh.
/// `Unknown` covers UDFs and any name that does not resolve to a builtin we
/// model.
enum class BuiltinKind : std::uint8_t {
  Unknown,  ///< UDFs / unrecognised builtins; no facts derived.
  Range,    ///< Cypher range. Only the two-bound form has a derivable length;
            ///< the optional step form is left unknown.
  Size,     ///< Cypher size(list); folds to the list's known length, no eval.
};

/// Classify a function name into a BuiltinKind.  Case-insensitive on the
/// builtin set; everything else is Unknown.
inline auto BuiltinKindFor(std::string_view name) -> BuiltinKind {
  // Case-insensitive compare against the small static set of recognised
  // builtins.  Adding a new builtin is one entry here plus a case in the
  // Function trait's update.
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
  if (eq_ci("size")) return BuiltinKind::Size;
  return BuiltinKind::Unknown;
}

/// What we cache per interned function name.  Storing the exact source name
/// (not the lowercased form) so the Builder can recreate the AST `Function`
/// with the user's spelling.  `is_pure` is sourced from the executor's
/// authoritative function table at the converter boundary (the planner does not
/// re-derive it); it gates treating the call's output as a statically-known
/// fact - only a pure function may be folded or carry a known value.
struct FunctionInfo {
  std::string name;
  BuiltinKind kind;
  bool is_pure;
};

}  // namespace memgraph::query::plan::v2
