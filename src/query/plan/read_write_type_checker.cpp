#include "query/plan/read_write_type_checker.hpp"

#define PRE_VISIT(TOp, RWType, continue_visiting) \
  bool ReadWriteTypeChecker::PreVisit(TOp &op) {  \
    UpdateType(RWType);                           \
    return continue_visiting;                     \
  }

namespace query::plan {

PRE_VISIT(CreateNode, RWType::W, true)
PRE_VISIT(CreateExpand, RWType::R, true)
PRE_VISIT(Delete, RWType::W, true)

PRE_VISIT(SetProperty, RWType::W, true)
PRE_VISIT(SetProperties, RWType::W, true)
PRE_VISIT(SetLabels, RWType::W, true)

PRE_VISIT(RemoveProperty, RWType::W, true)
PRE_VISIT(RemoveLabels, RWType::W, true)

PRE_VISIT(ScanAll, RWType::R, true)
PRE_VISIT(ScanAllByLabel, RWType::W, true)
PRE_VISIT(ScanAllByLabelPropertyRange, RWType::R, true)
PRE_VISIT(ScanAllByLabelPropertyValue, RWType::R, true)
PRE_VISIT(ScanAllByLabelProperty, RWType::R, true)
PRE_VISIT(ScanAllById, RWType::R, true)

PRE_VISIT(Expand, RWType::R, true)
PRE_VISIT(ExpandVariable, RWType::R, true)

PRE_VISIT(ConstructNamedPath, RWType::R, true)

PRE_VISIT(Filter, RWType::NONE, true)
PRE_VISIT(EdgeUniquenessFilter, RWType::NONE, true)

PRE_VISIT(Merge, RWType::RW, true)
PRE_VISIT(Optional, RWType::NONE, true)

bool ReadWriteTypeChecker::PreVisit(Cartesian &op) {
  op.left_op_->Accept(*this);
  op.right_op_->Accept(*this);
  return false;
}

PRE_VISIT(Produce, RWType::NONE, true)
PRE_VISIT(Accumulate, RWType::NONE, true)
PRE_VISIT(Aggregate, RWType::NONE, true)
PRE_VISIT(Skip, RWType::NONE, true)
PRE_VISIT(Limit, RWType::NONE, true)
PRE_VISIT(OrderBy, RWType::NONE, true)
PRE_VISIT(Distinct, RWType::NONE, true)

bool ReadWriteTypeChecker::PreVisit(Union &op) {
  op.left_op_->Accept(*this);
  op.right_op_->Accept(*this);
  return false;
}

PRE_VISIT(Unwind, RWType::NONE, true)
PRE_VISIT(CallProcedure, RWType::NONE, true)

bool ReadWriteTypeChecker::Visit(Once &op) { return false; }

void ReadWriteTypeChecker::UpdateType(RWType op_type) {
  // update type only if it's not the NONE type and the current operator's type
  // is different than the one that's currently inferred
  if (type != RWType::NONE && type != op_type) {
    type = RWType::RW;
  }
  // stop inference because RW is the most "dominant" type, i.e. it isn't
  // affected by the type of nodes in the plan appearing after the node for
  // which the type is set to RW
  if (type == RWType::RW) {
    return;
  }
  if (type == RWType::NONE && op_type != RWType::NONE) {
    type = op_type;
  }
}

void ReadWriteTypeChecker::InferRWType(LogicalOperator &root) {
  root.Accept(*this);
}

std::string ReadWriteTypeChecker::TypeToString() const {
  switch (type) {
    case RWType::NONE:
      return "none";
    case RWType::R:
      return "r";
    case RWType::W:
      return "w";
    case RWType::RW:
      return "rw";
  }
}

}  // namespace query::plan
