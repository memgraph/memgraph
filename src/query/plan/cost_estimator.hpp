#include "query/frontend/ast/ast.hpp"
#include "query/plan/operator.hpp"
#include "query/typed_value.hpp"

namespace query::plan {

/**
 * Query plan execution time cost estimator, for comparing and choosing optimal
 * execution plans.
 *
 * In Cypher the write part of the query always executes in the same
 * cardinality. It is not allowed to execute a write operation before all the
 * expansion for that query part (WITH splits a query into parts) have executed.
 * For that reason cost estimation comes down to cardinality estimation for the
 * read parts of the query, and their expansion. We want to compare different
 * plans and try to figure out which has the optimal organization of scans,
 * expansions and filters.
 *
 * Note that expansions and filtering can also happen during Merge, which is a
 * write operation. We let that get evaluated like all other cardinality
 * influencing ops. Also, Merge cardinality modification should be contained (it
 * can never reduce it's input cardinality), but since Merge always happens
 * after the read part, and can't be reoredered, we can ignore that.
 *
 * Limiting and accumulating (Aggregate, OrderBy, Accumulate) operations are
 * cardinality modifiers that always execute at the end of the query part. Their
 * cardinality influence is irrelevant because they execute the same
 * for all plans for a single query part, and query part reordering is not
 * allowed.
 *
 * This kind of cost estimation can only be used for comparing logical plans.
 * It's aim is to estimate cost(A) to be less then cost(B) in every case where
 * actual query execution for plan A is less then that of plan B. It can NOT be
 * used to estimate how MUCH execution between A and B will differ.
 */
class CostEstimator : public HierarchicalLogicalOperatorVisitor {
 public:
  struct CostParam {
    static constexpr double kScanAll{1.0};
    static constexpr double kScanAllByLabel{1.1};
    static constexpr double MakeScanAllByLabelPropertyValue{1.1};
    static constexpr double MakeScanAllByLabelPropertyRange{1.1};
    static constexpr double kExpand{2.0};
    static constexpr double kExpandVariable{3.0};
    static constexpr double kExpandBreadthFirst{5.0};
    static constexpr double kFilter{1.5};
    static constexpr double kExpandUniquenessFilter{1.5};
    static constexpr double kUnwind{1.3};
  };

  struct CardParam {
    static constexpr double kExpand{3.0};
    static constexpr double kExpandVariable{9.0};
    static constexpr double kExpandBreadthFirst{8.0};
    static constexpr double kFilter{0.25};
    static constexpr double kExpandUniquenessFilter{0.95};
  };

  struct MiscParam {
    static constexpr double kUnwindNoLiteral{10.0};
  };

  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::PostVisit;

  CostEstimator(const GraphDbAccessor &db_accessor)
      : db_accessor_(db_accessor) {}

  bool PostVisit(ScanAll &) override;
  bool PostVisit(ScanAllByLabel &scan_all_by_label) override;
  bool PostVisit(ScanAllByLabelPropertyValue &logical_op) override;
  bool PostVisit(ScanAllByLabelPropertyRange &logical_op) override;
  bool PostVisit(Expand &) override;
  bool PostVisit(ExpandVariable &) override;
  bool PostVisit(ExpandBreadthFirst &) override;
  bool PostVisit(Filter &) override;
  bool PostVisit(ExpandUniquenessFilter<VertexAccessor> &) override;
  bool PostVisit(ExpandUniquenessFilter<EdgeAccessor> &) override;
  bool PostVisit(Unwind &unwind) override;
  bool Visit(Once &) override;
  bool Visit(CreateIndex &) override;

  auto cost() const { return cost_; }
  auto cardinality() const { return cardinality_; }

 private:
  // cost estimation that gets accumulated as the visitor
  // tours the logical plan
  double cost_{0};

  // cardinality estimation (how many times an operator gets executed)
  // cardinality is a double to make it easier to work with
  double cardinality_{1};
  //
  // accessor used for cardinality estimates in ScanAll and ScanAllByLabel
  const GraphDbAccessor &db_accessor_;

  void IncrementCost(double param) { cost_ += param * cardinality_; }
};

}  // namespace query::plan
