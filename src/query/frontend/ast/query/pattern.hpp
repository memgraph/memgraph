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

#include "query/frontend/ast/ast_storage.hpp"
#include "query/frontend/ast/ast_visitor.hpp"
#include "query/frontend/ast/query/identifier.hpp"

namespace memgraph::query {

class PatternAtom : public memgraph::query::Tree, public utils::Visitable<HierarchicalTreeVisitor> {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  using utils::Visitable<HierarchicalTreeVisitor>::Accept;

  PatternAtom() = default;

  memgraph::query::Identifier *identifier_{nullptr};

  PatternAtom *Clone(AstStorage *storage) const override = 0;

 protected:
  explicit PatternAtom(Identifier *identifier) : identifier_(identifier) {}

 private:
  friend class AstStorage;
};

class Pattern : public memgraph::query::Tree, public utils::Visitable<HierarchicalTreeVisitor> {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  using utils::Visitable<HierarchicalTreeVisitor>::Accept;

  Pattern() = default;

  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      bool cont = identifier_->Accept(visitor);
      for (auto &part : atoms_) {
        if (cont) {
          cont = part->Accept(visitor);
        }
      }
    }
    return visitor.PostVisit(*this);
  }

  memgraph::query::Identifier *identifier_{nullptr};
  std::vector<memgraph::query::PatternAtom *> atoms_;

  Pattern *Clone(AstStorage *storage) const override {
    Pattern *object = storage->Create<Pattern>();
    object->identifier_ = identifier_ ? identifier_->Clone(storage) : nullptr;
    object->atoms_.resize(atoms_.size());
    for (auto i3 = 0; i3 < atoms_.size(); ++i3) {
      object->atoms_[i3] = atoms_[i3] ? atoms_[i3]->Clone(storage) : nullptr;
    }
    return object;
  }

 private:
  friend class AstStorage;
};
}  // namespace memgraph::query
