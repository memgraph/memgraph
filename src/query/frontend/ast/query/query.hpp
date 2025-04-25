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

#include "query/frontend/ast/ast_visitor.hpp"

#include "query/frontend/ast/ast_storage.hpp"

namespace memgraph::query {
class Query : public memgraph::query::Tree, public utils::Visitable<QueryVisitor<void>> {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  using utils::Visitable<QueryVisitor<void>>::Accept;

  Query() = default;

  Query *Clone(AstStorage *storage) const override = 0;

 private:
  friend class AstStorage;
};
}  // namespace memgraph::query
