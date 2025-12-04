// Copyright 2025 Memgraph Ltd.
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

#include "frontend/ast/ast.hpp"
#include "spdlog/spdlog.h"

namespace memgraph::utils {

struct FrameChangeId {
  explicit FrameChangeId(query::Expression *expr) : kind_(Kind::Expr), hash_{std::hash<void *>{}(expr)}, expr_{expr} {}

  explicit FrameChangeId(query::Symbol::Position_t const &symbol_pos)
      : kind_(Kind::Sym), hash_{std::hash<int32_t>{}(symbol_pos)}, symbol_pos_{symbol_pos} {}

  friend bool operator==(const FrameChangeId &lhs, const FrameChangeId &rhs) {
    if (lhs.kind_ != rhs.kind_) return false;
    switch (lhs.kind_) {
      case Kind::Expr:
        return lhs.expr_ == rhs.expr_;
      case Kind::Sym:
        return lhs.symbol_pos_ == rhs.symbol_pos_;
    }
  }

  size_t hash() const { return hash_; }

 private:
  enum struct Kind : uint8_t { Expr, Sym };
  Kind kind_;
  std::size_t hash_;
  union {
    query::Symbol::Position_t symbol_pos_;
    query::Expression *expr_;
  };
};

inline FrameChangeId GetFrameChangeId(const memgraph::query::RegexMatch &regex_match) {
  if (regex_match.regex_->GetTypeInfo() == memgraph::query::Identifier::kType) {
    const auto *identifier = utils::Downcast<memgraph::query::Identifier>(regex_match.regex_);
    MG_ASSERT(identifier->symbol_pos_ != -1);
    return FrameChangeId{identifier->symbol_pos_};
  }
  return FrameChangeId{regex_match.regex_};
}

// Get ID by which FrameChangeCollector struct can cache in_list.expression2_
inline FrameChangeId GetFrameChangeId(const memgraph::query::InListOperator &in_list) {
  // Identifiers refer to a list already stored on the frame
  // We only want to build one set for that identifier, hence key on that symbol
  // e.g. `x IN lst AND y IN lst` that is 2 InListOperator that both use the same `lst`
  if (in_list.expression2_->GetTypeInfo() == memgraph::query::Identifier::kType) {
    const auto *identifier = utils::Downcast<memgraph::query::Identifier>(in_list.expression2_);
    MG_ASSERT(identifier->symbol_pos_ != -1);
    return FrameChangeId{identifier->symbol_pos_};
  }
  // Otherwise we uniquely cache this expression
  return FrameChangeId{in_list.expression2_};
}

}  // namespace memgraph::utils

namespace std {
template <>
struct hash<memgraph::utils::FrameChangeId> {
  size_t operator()(memgraph::utils::FrameChangeId const &frame_change_id) const noexcept {
    return frame_change_id.hash();
  }
};
}  // namespace std
