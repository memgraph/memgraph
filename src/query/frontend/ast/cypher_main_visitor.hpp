#pragma once

#include <string>
#include <unordered_set>
#include <utility>

#include <antlr4-runtime.h>

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/opencypher/generated/MemgraphCypherBaseVisitor.h"
#include "utils/exceptions.hpp"
#include "utils/logging.hpp"

namespace query {
namespace frontend {

using antlropencypher::MemgraphCypher;

struct ParsingContext {
  bool is_query_cached = false;
};

class CypherMainVisitor : public antlropencypher::MemgraphCypherBaseVisitor {
 public:
  explicit CypherMainVisitor(ParsingContext context, AstStorage *storage) : context_(context), storage_(storage) {}

 private:
  Expression *CreateBinaryOperatorByToken(size_t token, Expression *e1, Expression *e2) {
    switch (token) {
      case MemgraphCypher::OR:
        return storage_->Create<OrOperator>(e1, e2);
      case MemgraphCypher::XOR:
        return storage_->Create<XorOperator>(e1, e2);
      case MemgraphCypher::AND:
        return storage_->Create<AndOperator>(e1, e2);
      case MemgraphCypher::PLUS:
        return storage_->Create<AdditionOperator>(e1, e2);
      case MemgraphCypher::MINUS:
        return storage_->Create<SubtractionOperator>(e1, e2);
      case MemgraphCypher::ASTERISK:
        return storage_->Create<MultiplicationOperator>(e1, e2);
      case MemgraphCypher::SLASH:
        return storage_->Create<DivisionOperator>(e1, e2);
      case MemgraphCypher::PERCENT:
        return storage_->Create<ModOperator>(e1, e2);
      case MemgraphCypher::EQ:
        return storage_->Create<EqualOperator>(e1, e2);
      case MemgraphCypher::NEQ1:
      case MemgraphCypher::NEQ2:
        return storage_->Create<NotEqualOperator>(e1, e2);
      case MemgraphCypher::LT:
        return storage_->Create<LessOperator>(e1, e2);
      case MemgraphCypher::GT:
        return storage_->Create<GreaterOperator>(e1, e2);
      case MemgraphCypher::LTE:
        return storage_->Create<LessEqualOperator>(e1, e2);
      case MemgraphCypher::GTE:
        return storage_->Create<GreaterEqualOperator>(e1, e2);
      default:
        throw utils::NotYetImplemented("binary operator");
    }
  }

  Expression *CreateUnaryOperatorByToken(size_t token, Expression *e) {
    switch (token) {
      case MemgraphCypher::NOT:
        return storage_->Create<NotOperator>(e);
      case MemgraphCypher::PLUS:
        return storage_->Create<UnaryPlusOperator>(e);
      case MemgraphCypher::MINUS:
        return storage_->Create<UnaryMinusOperator>(e);
      default:
        throw utils::NotYetImplemented("unary operator");
    }
  }

  auto ExtractOperators(std::vector<antlr4::tree::ParseTree *> &all_children,
                        const std::vector<size_t> &allowed_operators) {
    std::vector<size_t> operators;
    for (auto *child : all_children) {
      antlr4::tree::TerminalNode *operator_node = nullptr;
      if ((operator_node = dynamic_cast<antlr4::tree::TerminalNode *>(child))) {
        if (std::find(allowed_operators.begin(), allowed_operators.end(), operator_node->getSymbol()->getType()) !=
            allowed_operators.end()) {
          operators.push_back(operator_node->getSymbol()->getType());
        }
      }
    }
    return operators;
  }

  /**
   * Convert opencypher's n-ary production to ast binary operators.
   *
   * @param _expressions Subexpressions of child for which we construct ast
   * operators, for example expression6 if we want to create ast nodes for
   * expression7.
   */
  template <typename TExpression>
  Expression *LeftAssociativeOperatorExpression(std::vector<TExpression *> _expressions,
                                                std::vector<antlr4::tree::ParseTree *> all_children,
                                                const std::vector<size_t> &allowed_operators) {
    DMG_ASSERT(_expressions.size(), "can't happen");
    std::vector<Expression *> expressions;
    auto operators = ExtractOperators(all_children, allowed_operators);

    for (auto *expression : _expressions) {
      expressions.push_back(expression->accept(this));
    }

    Expression *first_operand = expressions[0];
    for (int i = 1; i < (int)expressions.size(); ++i) {
      first_operand = CreateBinaryOperatorByToken(operators[i - 1], first_operand, expressions[i]);
    }
    return first_operand;
  }

  template <typename TExpression>
  Expression *PrefixUnaryOperator(TExpression *_expression, std::vector<antlr4::tree::ParseTree *> all_children,
                                  const std::vector<size_t> &allowed_operators) {
    DMG_ASSERT(_expression, "can't happen");
    auto operators = ExtractOperators(all_children, allowed_operators);

    Expression *expression = _expression->accept(this);
    for (int i = (int)operators.size() - 1; i >= 0; --i) {
      expression = CreateUnaryOperatorByToken(operators[i], expression);
    }
    return expression;
  }

  /**
   * @return CypherQuery*
   */
  antlrcpp::Any visitCypherQuery(MemgraphCypher::CypherQueryContext *ctx) override;

  /**
   * @return IndexQuery*
   */
  antlrcpp::Any visitIndexQuery(MemgraphCypher::IndexQueryContext *ctx) override;

  /**
   * @return ExplainQuery*
   */
  antlrcpp::Any visitExplainQuery(MemgraphCypher::ExplainQueryContext *ctx) override;

  /**
   * @return ProfileQuery*
   */
  antlrcpp::Any visitProfileQuery(MemgraphCypher::ProfileQueryContext *ctx) override;

  /**
   * @return InfoQuery*
   */
  antlrcpp::Any visitInfoQuery(MemgraphCypher::InfoQueryContext *ctx) override;

  /**
   * @return Constraint
   */
  antlrcpp::Any visitConstraint(MemgraphCypher::ConstraintContext *ctx) override;

  /**
   * @return ConstraintQuery*
   */
  antlrcpp::Any visitConstraintQuery(MemgraphCypher::ConstraintQueryContext *ctx) override;

  /**
   * @return AuthQuery*
   */
  antlrcpp::Any visitAuthQuery(MemgraphCypher::AuthQueryContext *ctx) override;

  /**
   * @return DumpQuery*
   */
  antlrcpp::Any visitDumpQuery(MemgraphCypher::DumpQueryContext *ctx) override;

  /**
   * @return ReplicationQuery*
   */
  antlrcpp::Any visitReplicationQuery(MemgraphCypher::ReplicationQueryContext *ctx) override;

  /**
   * @return ReplicationQuery*
   */
  antlrcpp::Any visitSetReplicationRole(MemgraphCypher::SetReplicationRoleContext *ctx) override;

  /**
   * @return ReplicationQuery*
   */
  antlrcpp::Any visitShowReplicationRole(MemgraphCypher::ShowReplicationRoleContext *ctx) override;

  /**
   * @return ReplicationQuery*
   */
  antlrcpp::Any visitRegisterReplica(MemgraphCypher::RegisterReplicaContext *ctx) override;

  /**
   * @return ReplicationQuery*
   */
  antlrcpp::Any visitDropReplica(MemgraphCypher::DropReplicaContext *ctx) override;

  /**
   * @return ReplicationQuery*
   */
  antlrcpp::Any visitShowReplicas(MemgraphCypher::ShowReplicasContext *ctx) override;

  /**
   * @return LockPathQuery*
   */
  antlrcpp::Any visitLockPathQuery(MemgraphCypher::LockPathQueryContext *ctx) override;

  /**
   * @return LoadCsvQuery*
   */
  antlrcpp::Any visitLoadCsv(MemgraphCypher::LoadCsvContext *ctx) override;

  /**
   * @return FreeMemoryQuery*
   */
  antlrcpp::Any visitFreeMemoryQuery(MemgraphCypher::FreeMemoryQueryContext *ctx) override;

  /**
   * @return TriggerQuery*
   */
  antlrcpp::Any visitTriggerQuery(MemgraphCypher::TriggerQueryContext *ctx) override;

  /**
   * @return CreateTrigger*
   */
  antlrcpp::Any visitCreateTrigger(MemgraphCypher::CreateTriggerContext *ctx) override;

  /**
   * @return DropTrigger*
   */
  antlrcpp::Any visitDropTrigger(MemgraphCypher::DropTriggerContext *ctx) override;

  /**
   * @return ShowTriggers*
   */
  antlrcpp::Any visitShowTriggers(MemgraphCypher::ShowTriggersContext *ctx) override;

  /**
   * @return IsolationLevelQuery*
   */
  antlrcpp::Any visitIsolationLevelQuery(MemgraphCypher::IsolationLevelQueryContext *ctx) override;

  /**
   * @return StreamQuery*
   */
  antlrcpp::Any visitStreamQuery(MemgraphCypher::StreamQueryContext *ctx) override;

  /**
   * @return StreamQuery*
   */
  antlrcpp::Any visitCreateStream(MemgraphCypher::CreateStreamContext *ctx) override;

  /**
   * @return StreamQuery*
   */
  antlrcpp::Any visitDropStream(MemgraphCypher::DropStreamContext *ctx) override;

  /**
   * @return StreamQuery*
   */
  antlrcpp::Any visitStartStream(MemgraphCypher::StartStreamContext *ctx) override;

  /**
   * @return StreamQuery*
   */
  antlrcpp::Any visitStartAllStreams(MemgraphCypher::StartAllStreamsContext *ctx) override;

  /**
   * @return StreamQuery*
   */
  antlrcpp::Any visitStopStream(MemgraphCypher::StopStreamContext *ctx) override;

  /**
   * @return StreamQuery*
   */
  antlrcpp::Any visitStopAllStreams(MemgraphCypher::StopAllStreamsContext *ctx) override;

  /**
   * @return StreamQuery*
   */
  antlrcpp::Any visitShowStreams(MemgraphCypher::ShowStreamsContext *ctx) override;

  /**
   * @return StreamQuery*
   */
  antlrcpp::Any visitCheckStream(MemgraphCypher::CheckStreamContext *ctx) override;

  /**
   * @return CypherUnion*
   */
  antlrcpp::Any visitCypherUnion(MemgraphCypher::CypherUnionContext *ctx) override;

  /**
   * @return SingleQuery*
   */
  antlrcpp::Any visitSingleQuery(MemgraphCypher::SingleQueryContext *ctx) override;

  /**
   * @return Clause* or vector<Clause*>!!!
   */
  antlrcpp::Any visitClause(MemgraphCypher::ClauseContext *ctx) override;

  /**
   * @return Match*
   */
  antlrcpp::Any visitCypherMatch(MemgraphCypher::CypherMatchContext *ctx) override;

  /**
   * @return Create*
   */
  antlrcpp::Any visitCreate(MemgraphCypher::CreateContext *ctx) override;

  /**
   * @return CallProcedure*
   */
  antlrcpp::Any visitCallProcedure(MemgraphCypher::CallProcedureContext *ctx) override;

  /**
   * @return std::string
   */
  antlrcpp::Any visitUserOrRoleName(MemgraphCypher::UserOrRoleNameContext *ctx) override;

  /**
   * @return AuthQuery*
   */
  antlrcpp::Any visitCreateRole(MemgraphCypher::CreateRoleContext *ctx) override;

  /**
   * @return AuthQuery*
   */
  antlrcpp::Any visitDropRole(MemgraphCypher::DropRoleContext *ctx) override;

  /**
   * @return AuthQuery*
   */
  antlrcpp::Any visitShowRoles(MemgraphCypher::ShowRolesContext *ctx) override;

  /**
   * @return IndexQuery*
   */
  antlrcpp::Any visitCreateIndex(MemgraphCypher::CreateIndexContext *ctx) override;

  /**
   * @return DropIndex*
   */
  antlrcpp::Any visitDropIndex(MemgraphCypher::DropIndexContext *ctx) override;

  /**
   * @return AuthQuery*
   */
  antlrcpp::Any visitCreateUser(MemgraphCypher::CreateUserContext *ctx) override;

  /**
   * @return AuthQuery*
   */
  antlrcpp::Any visitSetPassword(MemgraphCypher::SetPasswordContext *ctx) override;

  /**
   * @return AuthQuery*
   */
  antlrcpp::Any visitDropUser(MemgraphCypher::DropUserContext *ctx) override;

  /**
   * @return AuthQuery*
   */
  antlrcpp::Any visitShowUsers(MemgraphCypher::ShowUsersContext *ctx) override;

  /**
   * @return AuthQuery*
   */
  antlrcpp::Any visitSetRole(MemgraphCypher::SetRoleContext *ctx) override;

  /**
   * @return AuthQuery*
   */
  antlrcpp::Any visitClearRole(MemgraphCypher::ClearRoleContext *ctx) override;

  /**
   * @return AuthQuery*
   */
  antlrcpp::Any visitGrantPrivilege(MemgraphCypher::GrantPrivilegeContext *ctx) override;

  /**
   * @return AuthQuery*
   */
  antlrcpp::Any visitDenyPrivilege(MemgraphCypher::DenyPrivilegeContext *ctx) override;

  /**
   * @return AuthQuery*
   */
  antlrcpp::Any visitRevokePrivilege(MemgraphCypher::RevokePrivilegeContext *ctx) override;

  /**
   * @return AuthQuery::Privilege
   */
  antlrcpp::Any visitPrivilege(MemgraphCypher::PrivilegeContext *ctx) override;

  /**
   * @return AuthQuery*
   */
  antlrcpp::Any visitShowPrivileges(MemgraphCypher::ShowPrivilegesContext *ctx) override;

  /**
   * @return AuthQuery*
   */
  antlrcpp::Any visitShowRoleForUser(MemgraphCypher::ShowRoleForUserContext *ctx) override;

  /**
   * @return AuthQuery*
   */
  antlrcpp::Any visitShowUsersForRole(MemgraphCypher::ShowUsersForRoleContext *ctx) override;

  /**
   * @return Return*
   */
  antlrcpp::Any visitCypherReturn(MemgraphCypher::CypherReturnContext *ctx) override;

  /**
   * @return Return*
   */
  antlrcpp::Any visitReturnBody(MemgraphCypher::ReturnBodyContext *ctx) override;

  /**
   * @return pair<bool, vector<NamedExpression*>> first member is true if
   * asterisk was found in return
   * expressions.
   */
  antlrcpp::Any visitReturnItems(MemgraphCypher::ReturnItemsContext *ctx) override;

  /**
   * @return vector<NamedExpression*>
   */
  antlrcpp::Any visitReturnItem(MemgraphCypher::ReturnItemContext *ctx) override;

  /**
   * @return vector<SortItem>
   */
  antlrcpp::Any visitOrder(MemgraphCypher::OrderContext *ctx) override;

  /**
   * @return SortItem
   */
  antlrcpp::Any visitSortItem(MemgraphCypher::SortItemContext *ctx) override;

  /**
   * @return NodeAtom*
   */
  antlrcpp::Any visitNodePattern(MemgraphCypher::NodePatternContext *ctx) override;

  /**
   * @return vector<LabelIx>
   */
  antlrcpp::Any visitNodeLabels(MemgraphCypher::NodeLabelsContext *ctx) override;

  /**
   * @return unordered_map<PropertyIx, Expression*>
   */
  antlrcpp::Any visitProperties(MemgraphCypher::PropertiesContext *ctx) override;

  /**
   * @return map<std::string, Expression*>
   */
  antlrcpp::Any visitMapLiteral(MemgraphCypher::MapLiteralContext *ctx) override;

  /**
   * @return vector<Expression*>
   */
  antlrcpp::Any visitListLiteral(MemgraphCypher::ListLiteralContext *ctx) override;

  /**
   * @return PropertyIx
   */
  antlrcpp::Any visitPropertyKeyName(MemgraphCypher::PropertyKeyNameContext *ctx) override;

  /**
   * @return string
   */
  antlrcpp::Any visitSymbolicName(MemgraphCypher::SymbolicNameContext *ctx) override;

  /**
   * @return vector<Pattern*>
   */
  antlrcpp::Any visitPattern(MemgraphCypher::PatternContext *ctx) override;

  /**
   * @return Pattern*
   */
  antlrcpp::Any visitPatternPart(MemgraphCypher::PatternPartContext *ctx) override;

  /**
   * @return Pattern*
   */
  antlrcpp::Any visitPatternElement(MemgraphCypher::PatternElementContext *ctx) override;

  /**
   * @return vector<pair<EdgeAtom*, NodeAtom*>>
   */
  antlrcpp::Any visitPatternElementChain(MemgraphCypher::PatternElementChainContext *ctx) override;

  /**
   *@return EdgeAtom*
   */
  antlrcpp::Any visitRelationshipPattern(MemgraphCypher::RelationshipPatternContext *ctx) override;

  /**
   * This should never be called. Everything is done directly in
   * visitRelationshipPattern.
   */
  antlrcpp::Any visitRelationshipDetail(MemgraphCypher::RelationshipDetailContext *ctx) override;

  /**
   * This should never be called. Everything is done directly in
   * visitRelationshipPattern.
   */
  antlrcpp::Any visitRelationshipLambda(MemgraphCypher::RelationshipLambdaContext *ctx) override;

  /**
   * @return vector<EdgeTypeIx>
   */
  antlrcpp::Any visitRelationshipTypes(MemgraphCypher::RelationshipTypesContext *ctx) override;

  /**
   * @return std::tuple<EdgeAtom::Type, int64_t, int64_t>.
   */
  antlrcpp::Any visitVariableExpansion(MemgraphCypher::VariableExpansionContext *ctx) override;

  /**
   * Top level expression, does nothing.
   *
   * @return Expression*
   */
  antlrcpp::Any visitExpression(MemgraphCypher::ExpressionContext *ctx) override;

  /**
   * OR.
   *
   * @return Expression*
   */
  antlrcpp::Any visitExpression12(MemgraphCypher::Expression12Context *ctx) override;

  /**
   * XOR.
   *
   * @return Expression*
   */
  antlrcpp::Any visitExpression11(MemgraphCypher::Expression11Context *ctx) override;

  /**
   * AND.
   *
   * @return Expression*
   */
  antlrcpp::Any visitExpression10(MemgraphCypher::Expression10Context *ctx) override;

  /**
   * NOT.
   *
   * @return Expression*
   */
  antlrcpp::Any visitExpression9(MemgraphCypher::Expression9Context *ctx) override;

  /**
   * Comparisons.
   *
   * @return Expression*
   */
  antlrcpp::Any visitExpression8(MemgraphCypher::Expression8Context *ctx) override;

  /**
   * Never call this. Everything related to generating code for comparison
   * operators should be done in visitExpression8.
   */
  antlrcpp::Any visitPartialComparisonExpression(MemgraphCypher::PartialComparisonExpressionContext *ctx) override;

  /**
   * Addition and subtraction.
   *
   * @return Expression*
   */
  antlrcpp::Any visitExpression7(MemgraphCypher::Expression7Context *ctx) override;

  /**
   * Multiplication, division, modding.
   *
   * @return Expression*
   */
  antlrcpp::Any visitExpression6(MemgraphCypher::Expression6Context *ctx) override;

  /**
   * Power.
   *
   * @return Expression*
   */
  antlrcpp::Any visitExpression5(MemgraphCypher::Expression5Context *ctx) override;

  /**
   * Unary minus and plus.
   *
   * @return Expression*
   */
  antlrcpp::Any visitExpression4(MemgraphCypher::Expression4Context *ctx) override;

  /**
   * IS NULL, IS NOT NULL, STARTS WITH, END WITH, =~, ...
   *
   * @return Expression*
   */
  antlrcpp::Any visitExpression3a(MemgraphCypher::Expression3aContext *ctx) override;

  /**
   * Does nothing, everything is done in visitExpression3a.
   *
   * @return Expression*
   */
  antlrcpp::Any visitStringAndNullOperators(MemgraphCypher::StringAndNullOperatorsContext *ctx) override;

  /**
   * List indexing and slicing.
   *
   * @return Expression*
   */
  antlrcpp::Any visitExpression3b(MemgraphCypher::Expression3bContext *ctx) override;

  /**
   * Does nothing, everything is done in visitExpression3b.
   */
  antlrcpp::Any visitListIndexingOrSlicing(MemgraphCypher::ListIndexingOrSlicingContext *ctx) override;

  /**
   * Node labels test.
   *
   * @return Expression*
   */
  antlrcpp::Any visitExpression2a(MemgraphCypher::Expression2aContext *ctx) override;

  /**
   * Property lookup.
   *
   * @return Expression*
   */
  antlrcpp::Any visitExpression2b(MemgraphCypher::Expression2bContext *ctx) override;

  /**
   * Literals, params, list comprehension...
   *
   * @return Expression*
   */
  antlrcpp::Any visitAtom(MemgraphCypher::AtomContext *ctx) override;

  /**
   * @return ParameterLookup*
   */
  antlrcpp::Any visitParameter(MemgraphCypher::ParameterContext *ctx) override;

  /**
   * @return Expression*
   */
  antlrcpp::Any visitParenthesizedExpression(MemgraphCypher::ParenthesizedExpressionContext *ctx) override;

  /**
   * @return Expression*
   */
  antlrcpp::Any visitFunctionInvocation(MemgraphCypher::FunctionInvocationContext *ctx) override;

  /**
   * @return string - uppercased
   */
  antlrcpp::Any visitFunctionName(MemgraphCypher::FunctionNameContext *ctx) override;

  /**
   * @return Expression*
   */
  antlrcpp::Any visitLiteral(MemgraphCypher::LiteralContext *ctx) override;

  /**
   * Convert escaped string from a query to unescaped utf8 string.
   *
   * @return string
   */
  antlrcpp::Any visitStringLiteral(const std::string &escaped);

  /**
   * @return bool
   */
  antlrcpp::Any visitBooleanLiteral(MemgraphCypher::BooleanLiteralContext *ctx) override;

  /**
   * @return TypedValue with either double or int
   */
  antlrcpp::Any visitNumberLiteral(MemgraphCypher::NumberLiteralContext *ctx) override;

  /**
   * @return int64_t
   */
  antlrcpp::Any visitIntegerLiteral(MemgraphCypher::IntegerLiteralContext *ctx) override;

  /**
   * @return double
   */
  antlrcpp::Any visitDoubleLiteral(MemgraphCypher::DoubleLiteralContext *ctx) override;

  /**
   * @return Delete*
   */
  antlrcpp::Any visitCypherDelete(MemgraphCypher::CypherDeleteContext *ctx) override;

  /**
   * @return Where*
   */
  antlrcpp::Any visitWhere(MemgraphCypher::WhereContext *ctx) override;

  /**
   * return vector<Clause*>
   */
  antlrcpp::Any visitSet(MemgraphCypher::SetContext *ctx) override;

  /**
   * @return Clause*
   */
  antlrcpp::Any visitSetItem(MemgraphCypher::SetItemContext *ctx) override;

  /**
   * return vector<Clause*>
   */
  antlrcpp::Any visitRemove(MemgraphCypher::RemoveContext *ctx) override;

  /**
   * @return Clause*
   */
  antlrcpp::Any visitRemoveItem(MemgraphCypher::RemoveItemContext *ctx) override;

  /**
   * @return PropertyLookup*
   */
  antlrcpp::Any visitPropertyExpression(MemgraphCypher::PropertyExpressionContext *ctx) override;

  /**
   * @return IfOperator*
   */
  antlrcpp::Any visitCaseExpression(MemgraphCypher::CaseExpressionContext *ctx) override;

  /**
   * Never call this. Ast generation for this production is done in
   * @c visitCaseExpression.
   */
  antlrcpp::Any visitCaseAlternatives(MemgraphCypher::CaseAlternativesContext *ctx) override;

  /**
   * @return With*
   */
  antlrcpp::Any visitWith(MemgraphCypher::WithContext *ctx) override;

  /**
   * @return Merge*
   */
  antlrcpp::Any visitMerge(MemgraphCypher::MergeContext *ctx) override;

  /**
   * @return Unwind*
   */
  antlrcpp::Any visitUnwind(MemgraphCypher::UnwindContext *ctx) override;

  /**
   * Never call this. Ast generation for these expressions should be done by
   * explicitly visiting the members of @c FilterExpressionContext.
   */
  antlrcpp::Any visitFilterExpression(MemgraphCypher::FilterExpressionContext *) override;

 public:
  Query *query() { return query_; }
  const static std::string kAnonPrefix;

  bool IsCacheable() const { return is_cacheable_; }

 private:
  LabelIx AddLabel(const std::string &name);
  PropertyIx AddProperty(const std::string &name);
  EdgeTypeIx AddEdgeType(const std::string &name);

  ParsingContext context_;
  AstStorage *storage_;

  // Set of identifiers from queries.
  std::unordered_set<std::string> users_identifiers;
  // Identifiers that user didn't name.
  std::vector<Identifier **> anonymous_identifiers;
  Query *query_ = nullptr;
  // All return items which are not variables must be aliased in with.
  // We use this variable in visitReturnItem to check if we are in with or
  // return.
  bool in_with_ = false;

  bool is_cacheable_ = true;
};
}  // namespace frontend
}  // namespace query
