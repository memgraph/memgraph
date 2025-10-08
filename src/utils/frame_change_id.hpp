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

#include "spdlog/spdlog.h"

namespace memgraph::utils {

struct FrameChangeId {
  explicit FrameChangeId(query::ListLiteral *list_literal)
      : kind_(Kind::ListExpr), hash_{std::hash<void *>{}(list_literal)}, list_expr_{list_literal} {}

  FrameChangeId(query::Identifier const &identifier)
      : kind_(Kind::Sym), hash_{std::hash<int32_t>{}(identifier.symbol_pos_)}, symbol_pos_{identifier.symbol_pos_} {}

  FrameChangeId(query::NamedExpression const &named_expression)
      : kind_(Kind::Sym),
        hash_{std::hash<int32_t>{}(named_expression.symbol_pos_)},
        symbol_pos_{named_expression.symbol_pos_} {}

  FrameChangeId(query::Symbol const &symbol)
      : kind_(Kind::Sym),
        hash_{std::hash<int32_t>{}(static_cast<int32_t>(symbol.position_))},
        symbol_pos_{static_cast<int32_t>(symbol.position_)} {}

  friend bool operator==(const FrameChangeId &lhs, const FrameChangeId &rhs) {
    if (lhs.kind_ != rhs.kind_) return false;
    switch (lhs.kind_) {
      case Kind::ListExpr:
        return lhs.list_expr_ == rhs.list_expr_;
      case Kind::Sym:
        return lhs.symbol_pos_ == rhs.symbol_pos_;
    }
  }

  size_t hash() const { return hash_; }

 private:
  enum struct Kind : uint8_t { ListExpr, Sym };
  Kind kind_;
  std::size_t hash_;
  union {
    int32_t symbol_pos_;
    query::ListLiteral *list_expr_;
  };
};

// Get ID by which FrameChangeCollector struct can cache in_list.expression2_
inline std::optional<FrameChangeId> GetFrameChangeId(memgraph::query::InListOperator &in_list) {
  if (in_list.expression2_->GetTypeInfo() == memgraph::query::ListLiteral::kType) {
    auto *list_literal = utils::Downcast<memgraph::query::ListLiteral>(in_list.expression2_);
    return FrameChangeId{list_literal};
  }
  if (in_list.expression2_->GetTypeInfo() == memgraph::query::Identifier::kType) {
    auto *identifier = utils::Downcast<memgraph::query::Identifier>(in_list.expression2_);
    MG_ASSERT(identifier->symbol_pos_ != -1);
    return FrameChangeId{*identifier};
  }
  return {};
};

}  // namespace memgraph::utils

namespace std {
template <>
struct hash<memgraph::utils::FrameChangeId> {
  size_t operator()(memgraph::utils::FrameChangeId const &frame_change_id) const noexcept {
    return frame_change_id.hash();
  }
};
}  // namespace std
