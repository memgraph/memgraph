// Copyright 2023 Memgraph Ltd.
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

#include "query/plan/operator.hpp"

namespace memgraph::query::plan {

struct ReadWriteTypeChecker : public virtual HierarchicalLogicalOperatorVisitor {
 public:
  ReadWriteTypeChecker() = default;

  ReadWriteTypeChecker(const ReadWriteTypeChecker &) = delete;
  ReadWriteTypeChecker(ReadWriteTypeChecker &&) = delete;

  ReadWriteTypeChecker &operator=(const ReadWriteTypeChecker &) = delete;
  ReadWriteTypeChecker &operator=(ReadWriteTypeChecker &&) = delete;

  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  // NONE type describes an operator whose action neither reads nor writes from
  // the database (e.g. Produce or Once).
  // R type describes an operator whose action involves reading from the
  // database.
  // W type describes an operator whose action involves writing to the
  // database.
  // RW type describes an operator whose action involves both reading and
  // writing to the database.
  enum class RWType : uint8_t { NONE, R, W, RW };

  RWType type{RWType::NONE};
  void InferRWType(LogicalOperator &root);
  static std::string TypeToString(const RWType type);

  bool PreVisit(CreateNode &) override;
  bool PreVisit(CreateExpand &) override;
  bool PreVisit(Delete &) override;

  bool PreVisit(SetProperty &) override;
  bool PreVisit(SetProperties &) override;
  bool PreVisit(SetLabels &) override;

  bool PreVisit(RemoveProperty &) override;
  bool PreVisit(RemoveLabels &) override;

  bool PreVisit(ScanAll &) override;
  bool PreVisit(ScanAllByLabel &) override;
  bool PreVisit(ScanAllByLabelPropertyValue &) override;
  bool PreVisit(ScanAllByLabelPropertyRange &) override;
  bool PreVisit(ScanAllByLabelProperty &) override;
  bool PreVisit(ScanAllById &) override;

  bool PreVisit(Expand &) override;
  bool PreVisit(ExpandVariable &) override;

  bool PreVisit(ConstructNamedPath &) override;

  bool PreVisit(Filter &) override;
  bool PreVisit(EdgeUniquenessFilter &) override;

  bool PreVisit(Merge &) override;
  bool PreVisit(Optional &) override;
  bool PreVisit(Cartesian &) override;

  bool PreVisit(EmptyResult &) override;
  bool PreVisit(Produce &) override;
  bool PreVisit(Accumulate &) override;
  bool PreVisit(Aggregate &) override;
  bool PreVisit(Skip &) override;
  bool PreVisit(Limit &) override;
  bool PreVisit(OrderBy &) override;
  bool PreVisit(Distinct &) override;
  bool PreVisit(Union &) override;

  bool PreVisit(Unwind &) override;
  bool PreVisit(CallProcedure &) override;
  bool PreVisit(Foreach &) override;

  bool Visit(Once &) override;

  void UpdateType(RWType op_type);
};

}  // namespace memgraph::query::plan
