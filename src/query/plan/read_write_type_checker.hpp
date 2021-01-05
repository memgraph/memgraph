#pragma once

#include "query/plan/operator.hpp"

namespace query::plan {

class ReadWriteTypeChecker : public virtual HierarchicalLogicalOperatorVisitor {
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
  // the database
  enum class RWType : uint8_t { NONE, R, W, RW };

  RWType type{RWType::NONE};
  void InferRWType(LogicalOperator &root);
  std::string TypeToString() const;

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

  bool Visit(Once &) override;

 private:
  void UpdateType(RWType op_type);
};

}  // namespace query::plan
