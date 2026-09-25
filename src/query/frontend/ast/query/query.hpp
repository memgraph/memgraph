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

#include "query/frontend/ast/ast_visitor.hpp"

#include "query/frontend/ast/ast_storage.hpp"
#include "query/frontend/ast/query/storage_access_policy.hpp"

namespace memgraph::query {
class Query : public memgraph::query::Tree, public utils::Visitable<QueryVisitor<void>> {
 public:
  static const utils::TypeInfo kType;

  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  using utils::Visitable<QueryVisitor<void>>::Accept;

  Query() = default;

  Query *Clone(AstStorage *storage) const override = 0;

  /// What this kind of query needs held on the graph while it is prepared. Pure so that a new kind
  /// of query cannot inherit an answer that happens to compile.
  virtual StorageAccessPolicy AccessPolicy() const = 0;

  /// Whether this query works on the current database's graph data, including the metadata
  /// describing it, rather than on instance, session or system state. A database that failed
  /// recovery serves none of these until it has been recovered, so the default answer is the one
  /// that refuses: a query that has to stay available while a database is broken says so.
  virtual bool OperatesOnGraphData() const { return true; }

 private:
  friend class AstStorage;
};
}  // namespace memgraph::query
