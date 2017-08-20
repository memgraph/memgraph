#include "query/frontend/ast/cypher_main_visitor.hpp"

#include <algorithm>
#include <climits>
#include <codecvt>
#include <cstring>
#include <limits>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "database/graph_db.hpp"
#include "query/common.hpp"
#include "query/exceptions.hpp"
#include "query/interpret/awesome_memgraph_functions.hpp"
#include "utils/assert.hpp"
#include "utils/exceptions.hpp"
#include "utils/string.hpp"

namespace query::frontend {

const std::string CypherMainVisitor::kAnonPrefix = "anon";

antlrcpp::Any CypherMainVisitor::visitSingleQuery(
    CypherParser::SingleQueryContext *ctx) {
  query_ = storage_.query();
  for (auto *child : ctx->clause()) {
    antlrcpp::Any got = child->accept(this);
    if (got.is<Clause *>()) {
      query_->clauses_.push_back(got.as<Clause *>());
    } else {
      auto child_clauses = got.as<std::vector<Clause *>>();
      query_->clauses_.insert(query_->clauses_.end(), child_clauses.begin(),
                              child_clauses.end());
    }
  }

  // Check if ordering of clauses makes sense.
  //
  // TODO: should we forbid multiple consecutive set clauses? That case is
  // little bit problematic because multiple barriers are needed. Multiple
  // consecutive SET clauses are undefined behaviour in neo4j.
  bool has_update = false;
  bool has_return = false;
  bool has_optional_match = false;
  bool has_create_index = false;
  for (Clause *clause : query_->clauses_) {
    if (dynamic_cast<Unwind *>(clause)) {
      if (has_update || has_return) {
        throw SemanticException(
            "Unwind can't be after return or update clause");
      }
    } else if (auto *match = dynamic_cast<Match *>(clause)) {
      if (has_update || has_return) {
        throw SemanticException("Match can't be after return or update clause");
      }
      if (match->optional_) {
        has_optional_match = true;
      } else if (has_optional_match) {
        throw SemanticException("Match can't be after optional match");
      }
    } else if (dynamic_cast<Create *>(clause) ||
               dynamic_cast<Delete *>(clause) ||
               dynamic_cast<SetProperty *>(clause) ||
               dynamic_cast<SetProperties *>(clause) ||
               dynamic_cast<SetLabels *>(clause) ||
               dynamic_cast<RemoveProperty *>(clause) ||
               dynamic_cast<RemoveLabels *>(clause) ||
               dynamic_cast<Merge *>(clause)) {
      if (has_return) {
        throw SemanticException("Update clauses can't be after return");
      }
      has_update = true;
    } else if (dynamic_cast<Return *>(clause)) {
      if (has_return) {
        throw SemanticException("There can be only one return in a clause");
      }
      has_return = true;
    } else if (dynamic_cast<With *>(clause)) {
      if (has_return) {
        throw SemanticException("Return can't be before with");
      }
      has_update = has_return = has_optional_match = false;
    } else if (dynamic_cast<CreateIndex *>(clause)) {
      // If there is CreateIndex clause then there shouldn't be anything else.
      if (query_->clauses_.size() != 1U) {
        throw SemanticException(
            "CreateIndex must be only clause in the query.");
      }
      has_create_index = true;
    } else {
      debug_assert(false, "Can't happen");
    }
  }
  if (!has_update && !has_return && !has_create_index) {
    throw SemanticException(
        "Query should either update something, return results or create an "
        "index");
  }

  // Construct unique names for anonymous identifiers;
  int id = 1;
  for (auto **identifier : anonymous_identifiers) {
    while (true) {
      std::string id_name = kAnonPrefix + std::to_string(id++);
      if (users_identifiers.find(id_name) == users_identifiers.end()) {
        *identifier = storage_.Create<Identifier>(id_name, false);
        break;
      }
    }
  }
  return query_;
}

antlrcpp::Any CypherMainVisitor::visitClause(CypherParser::ClauseContext *ctx) {
  if (ctx->cypherReturn()) {
    return static_cast<Clause *>(
        ctx->cypherReturn()->accept(this).as<Return *>());
  }
  if (ctx->cypherMatch()) {
    return static_cast<Clause *>(
        ctx->cypherMatch()->accept(this).as<Match *>());
  }
  if (ctx->create()) {
    return static_cast<Clause *>(ctx->create()->accept(this).as<Create *>());
  }
  if (ctx->cypherDelete()) {
    return static_cast<Clause *>(
        ctx->cypherDelete()->accept(this).as<Delete *>());
  }
  if (ctx->set()) {
    // Different return type!!!
    return ctx->set()->accept(this).as<std::vector<Clause *>>();
  }
  if (ctx->remove()) {
    // Different return type!!!
    return ctx->remove()->accept(this).as<std::vector<Clause *>>();
  }
  if (ctx->with()) {
    return static_cast<Clause *>(ctx->with()->accept(this).as<With *>());
  }
  if (ctx->merge()) {
    return static_cast<Clause *>(ctx->merge()->accept(this).as<Merge *>());
  }
  if (ctx->unwind()) {
    return static_cast<Clause *>(ctx->unwind()->accept(this).as<Unwind *>());
  }
  if (ctx->createIndex()) {
    return static_cast<Clause *>(
        ctx->createIndex()->accept(this).as<CreateIndex *>());
  }
  // TODO: implement other clauses.
  throw utils::NotYetImplemented("clause '{}'", ctx->getText());
  return 0;
}

antlrcpp::Any CypherMainVisitor::visitCypherMatch(
    CypherParser::CypherMatchContext *ctx) {
  auto *match = storage_.Create<Match>();
  match->optional_ = !!ctx->OPTIONAL();
  if (ctx->where()) {
    match->where_ = ctx->where()->accept(this);
  }
  match->patterns_ = ctx->pattern()->accept(this).as<std::vector<Pattern *>>();
  return match;
}

antlrcpp::Any CypherMainVisitor::visitCreate(CypherParser::CreateContext *ctx) {
  auto *create = storage_.Create<Create>();
  create->patterns_ = ctx->pattern()->accept(this).as<std::vector<Pattern *>>();
  return create;
}

/**
 * @return CreateIndex*
 */
antlrcpp::Any CypherMainVisitor::visitCreateIndex(
    CypherParser::CreateIndexContext *ctx) {
  std::pair<std::string, GraphDbTypes::Property> key =
      ctx->propertyKeyName()->accept(this);
  return storage_.Create<CreateIndex>(
      ctx_.db_accessor_.Label(ctx->labelName()->accept(this)), key.second);
}

antlrcpp::Any CypherMainVisitor::visitCypherReturn(
    CypherParser::CypherReturnContext *ctx) {
  auto *return_clause = storage_.Create<Return>();
  return_clause->body_ = ctx->returnBody()->accept(this);
  if (ctx->DISTINCT()) {
    return_clause->body_.distinct = true;
  }
  return return_clause;
}

antlrcpp::Any CypherMainVisitor::visitReturnBody(
    CypherParser::ReturnBodyContext *ctx) {
  ReturnBody body;
  if (ctx->order()) {
    body.order_by = ctx->order()
                        ->accept(this)
                        .as<std::vector<std::pair<Ordering, Expression *>>>();
  }
  if (ctx->skip()) {
    body.skip = static_cast<Expression *>(ctx->skip()->accept(this));
  }
  if (ctx->limit()) {
    body.limit = static_cast<Expression *>(ctx->limit()->accept(this));
  }
  std::tie(body.all_identifiers, body.named_expressions) =
      ctx->returnItems()
          ->accept(this)
          .as<std::pair<bool, std::vector<NamedExpression *>>>();
  return body;
}

antlrcpp::Any CypherMainVisitor::visitReturnItems(
    CypherParser::ReturnItemsContext *ctx) {
  std::vector<NamedExpression *> named_expressions;
  for (auto *item : ctx->returnItem()) {
    named_expressions.push_back(item->accept(this));
  }
  return std::pair<bool, std::vector<NamedExpression *>>(
      ctx->getTokens(kReturnAllTokenId).size(), named_expressions);
}

antlrcpp::Any CypherMainVisitor::visitReturnItem(
    CypherParser::ReturnItemContext *ctx) {
  auto *named_expr = storage_.Create<NamedExpression>();
  named_expr->expression_ = ctx->expression()->accept(this);
  if (ctx->variable()) {
    named_expr->name_ =
        std::string(ctx->variable()->accept(this).as<std::string>());
  } else {
    if (in_with_ && !dynamic_cast<Identifier *>(named_expr->expression_)) {
      throw SemanticException("Only variables can be non aliased in with");
    }
    named_expr->name_ = std::string(ctx->getText());
    named_expr->token_position_ =
        ctx->expression()->getStart()->getTokenIndex();
  }
  return named_expr;
}

antlrcpp::Any CypherMainVisitor::visitOrder(CypherParser::OrderContext *ctx) {
  std::vector<std::pair<Ordering, Expression *>> order_by;
  for (auto *sort_item : ctx->sortItem()) {
    order_by.push_back(sort_item->accept(this));
  }
  return order_by;
}

antlrcpp::Any CypherMainVisitor::visitSortItem(
    CypherParser::SortItemContext *ctx) {
  return std::pair<Ordering, Expression *>(
      ctx->DESC() || ctx->DESCENDING() ? Ordering::DESC : Ordering::ASC,
      ctx->expression()->accept(this));
}

antlrcpp::Any CypherMainVisitor::visitNodePattern(
    CypherParser::NodePatternContext *ctx) {
  auto *node = storage_.Create<NodeAtom>();
  if (ctx->variable()) {
    std::string variable = ctx->variable()->accept(this);
    node->identifier_ = storage_.Create<Identifier>(variable);
    users_identifiers.insert(variable);
  } else {
    anonymous_identifiers.push_back(&node->identifier_);
  }
  if (ctx->nodeLabels()) {
    node->labels_ =
        ctx->nodeLabels()->accept(this).as<std::vector<GraphDbTypes::Label>>();
  }
  if (ctx->properties()) {
    node->properties_ =
        ctx->properties()
            ->accept(this)
            .as<std::map<std::pair<std::string, GraphDbTypes::Property>,
                         Expression *>>();
  }
  return node;
}

antlrcpp::Any CypherMainVisitor::visitNodeLabels(
    CypherParser::NodeLabelsContext *ctx) {
  std::vector<GraphDbTypes::Label> labels;
  for (auto *node_label : ctx->nodeLabel()) {
    labels.push_back(ctx_.db_accessor_.Label(node_label->accept(this)));
  }
  return labels;
}

antlrcpp::Any CypherMainVisitor::visitProperties(
    CypherParser::PropertiesContext *ctx) {
  if (!ctx->mapLiteral()) {
    // If child is not mapLiteral that means child is params. At the moment
    // we don't support properties to be a param because we can generate
    // better logical plan if we have an information about properties at
    // compile time.
    // TODO: implement other clauses.
    throw utils::NotYetImplemented("property parameters");
  }
  return ctx->mapLiteral()->accept(this);
}

antlrcpp::Any CypherMainVisitor::visitMapLiteral(
    CypherParser::MapLiteralContext *ctx) {
  std::map<std::pair<std::string, GraphDbTypes::Property>, Expression *> map;
  for (int i = 0; i < static_cast<int>(ctx->propertyKeyName().size()); ++i) {
    std::pair<std::string, GraphDbTypes::Property> key =
        ctx->propertyKeyName()[i]->accept(this);
    Expression *value = ctx->expression()[i]->accept(this);
    if (!map.insert({key, value}).second) {
      throw SemanticException("Same key can't appear twice in map literal");
    }
  }
  return map;
}

antlrcpp::Any CypherMainVisitor::visitListLiteral(
    CypherParser::ListLiteralContext *ctx) {
  std::vector<Expression *> expressions;
  for (auto expr_ctx_ptr : ctx->expression())
    expressions.push_back(expr_ctx_ptr->accept(this));
  return expressions;
}

antlrcpp::Any CypherMainVisitor::visitPropertyKeyName(
    CypherParser::PropertyKeyNameContext *ctx) {
  const std::string key_name = visitChildren(ctx);
  return std::make_pair(key_name, ctx_.db_accessor_.Property(key_name));
}

antlrcpp::Any CypherMainVisitor::visitSymbolicName(
    CypherParser::SymbolicNameContext *ctx) {
  if (ctx->EscapedSymbolicName()) {
    auto quoted_name = ctx->getText();
    debug_assert(quoted_name.size() >= 2U && quoted_name[0] == '`' &&
                     quoted_name.back() == '`',
                 "Can't happen. Grammar ensures this");
    // Remove enclosing backticks.
    std::string escaped_name =
        quoted_name.substr(1, static_cast<int>(quoted_name.size()) - 2);
    // Unescape remaining backticks.
    std::string name;
    bool escaped = false;
    for (auto c : escaped_name) {
      if (escaped) {
        if (c == '`') {
          name.push_back('`');
          escaped = false;
        } else {
          debug_assert(false, "Can't happen. Grammar ensures that.");
        }
      } else if (c == '`') {
        escaped = true;
      } else {
        name.push_back(c);
      }
    }
    return name;
  }
  if (ctx->UnescapedSymbolicName() || ctx->HexLetter()) {
    return std::string(ctx->getText());
  }
  // Symbolic names are case sensitive. Since StrippedQuery lowercases all
  // keywords there is no way to differentiate between differently cased
  // symbolic names if they are equal to a keyword.
  throw SemanticException(
      fmt::format("Symbolic name can't be keyword {}", ctx->getText()));
}

antlrcpp::Any CypherMainVisitor::visitPattern(
    CypherParser::PatternContext *ctx) {
  std::vector<Pattern *> patterns;
  for (auto *pattern_part : ctx->patternPart()) {
    patterns.push_back(pattern_part->accept(this));
  }
  return patterns;
}

antlrcpp::Any CypherMainVisitor::visitPatternPart(
    CypherParser::PatternPartContext *ctx) {
  Pattern *pattern = ctx->anonymousPatternPart()->accept(this);
  if (ctx->variable()) {
    std::string variable = ctx->variable()->accept(this);
    pattern->identifier_ = storage_.Create<Identifier>(variable);
    users_identifiers.insert(variable);
  } else {
    anonymous_identifiers.push_back(&pattern->identifier_);
  }
  return pattern;
}

antlrcpp::Any CypherMainVisitor::visitPatternElement(
    CypherParser::PatternElementContext *ctx) {
  if (ctx->patternElement()) {
    return ctx->patternElement()->accept(this);
  }
  auto pattern = storage_.Create<Pattern>();
  pattern->atoms_.push_back(ctx->nodePattern()->accept(this).as<NodeAtom *>());
  for (auto *pattern_element_chain : ctx->patternElementChain()) {
    std::pair<PatternAtom *, PatternAtom *> element =
        pattern_element_chain->accept(this);
    pattern->atoms_.push_back(element.first);
    pattern->atoms_.push_back(element.second);
  }
  return pattern;
}

antlrcpp::Any CypherMainVisitor::visitPatternElementChain(
    CypherParser::PatternElementChainContext *ctx) {
  return std::pair<PatternAtom *, PatternAtom *>(
      ctx->relationshipPattern()->accept(this).as<EdgeAtom *>(),
      ctx->nodePattern()->accept(this).as<NodeAtom *>());
}

antlrcpp::Any CypherMainVisitor::visitRelationshipPattern(
    CypherParser::RelationshipPatternContext *ctx) {
  auto *edge = ctx->bfsDetail() ? storage_.Create<BreadthFirstAtom>()
                                : storage_.Create<EdgeAtom>();
  if (ctx->bfsDetail()) {
    if (ctx->bfsDetail()->bfs_variable) {
      std::string variable = ctx->bfsDetail()->bfs_variable->accept(this);
      edge->identifier_ = storage_.Create<Identifier>(variable);
      users_identifiers.insert(variable);
    }
    auto *bf_atom = dynamic_cast<BreadthFirstAtom *>(edge);
    std::string traversed_edge_variable =
        ctx->bfsDetail()->traversed_edge->accept(this);
    bf_atom->traversed_edge_identifier_ =
        storage_.Create<Identifier>(traversed_edge_variable);
    std::string next_node_variable = ctx->bfsDetail()->next_node->accept(this);
    bf_atom->next_node_identifier_ =
        storage_.Create<Identifier>(next_node_variable);
    bf_atom->filter_expression_ =
        ctx->bfsDetail()->expression()[0]->accept(this);
    bf_atom->max_depth_ = ctx->bfsDetail()->expression()[1]->accept(this);
  } else if (ctx->relationshipDetail()) {
    if (ctx->relationshipDetail()->variable()) {
      std::string variable =
          ctx->relationshipDetail()->variable()->accept(this);
      edge->identifier_ = storage_.Create<Identifier>(variable);
      users_identifiers.insert(variable);
    }
    if (ctx->relationshipDetail()->relationshipTypes()) {
      edge->edge_types_ = ctx->relationshipDetail()
                              ->relationshipTypes()
                              ->accept(this)
                              .as<std::vector<GraphDbTypes::EdgeType>>();
    }
    if (ctx->relationshipDetail()->properties()) {
      edge->properties_ =
          ctx->relationshipDetail()
              ->properties()
              ->accept(this)
              .as<std::map<std::pair<std::string, GraphDbTypes::Property>,
                           Expression *>>();
    }
    if (ctx->relationshipDetail()->rangeLiteral()) {
      edge->has_range_ = true;
      auto range = ctx->relationshipDetail()
                       ->rangeLiteral()
                       ->accept(this)
                       .as<std::pair<Expression *, Expression *>>();
      edge->lower_bound_ = range.first;
      edge->upper_bound_ = range.second;
    }
  }
  if (!edge->identifier_) {
    anonymous_identifiers.push_back(&edge->identifier_);
  }

  if (ctx->leftArrowHead() && !ctx->rightArrowHead()) {
    edge->direction_ = EdgeAtom::Direction::IN;
  } else if (!ctx->leftArrowHead() && ctx->rightArrowHead()) {
    edge->direction_ = EdgeAtom::Direction::OUT;
  } else {
    // <-[]-> and -[]- is the same thing as far as we understand openCypher
    // grammar.
    edge->direction_ = EdgeAtom::Direction::BOTH;
  }
  return edge;
}

antlrcpp::Any CypherMainVisitor::visitRelationshipDetail(
    CypherParser::RelationshipDetailContext *) {
  debug_assert(false, "Should never be called. See documentation in hpp.");
  return 0;
}

antlrcpp::Any CypherMainVisitor::visitBfsDetail(
    CypherParser::BfsDetailContext *) {
  debug_assert(false, "Should never be called. See documentation in hpp.");
  return 0;
}

antlrcpp::Any CypherMainVisitor::visitRelationshipTypes(
    CypherParser::RelationshipTypesContext *ctx) {
  std::vector<GraphDbTypes::EdgeType> types;
  for (auto *edge_type : ctx->relTypeName()) {
    types.push_back(ctx_.db_accessor_.EdgeType(edge_type->accept(this)));
  }
  return types;
}

antlrcpp::Any CypherMainVisitor::visitRangeLiteral(
    CypherParser::RangeLiteralContext *ctx) {
  debug_assert(ctx->expression().size() <= 2U,
               "Expected 0, 1 or 2 bounds in range literal.");
  if (ctx->expression().size() == 0U) {
    // Case -[*]-
    return std::pair<Expression *, Expression *>(nullptr, nullptr);
  } else if (ctx->expression().size() == 1U) {
    auto dots_tokens = ctx->getTokens(kDotsTokenId);
    Expression *bound = ctx->expression()[0]->accept(this);
    if (!dots_tokens.size()) {
      // Case -[*bound]-
      return std::pair<Expression *, Expression *>(bound, bound);
    }
    if (dots_tokens[0]->getSourceInterval().startsAfter(
            ctx->expression()[0]->getSourceInterval())) {
      // Case -[*bound..]-
      return std::pair<Expression *, Expression *>(bound, nullptr);
    } else {
      // Case -[*..bound]-
      return std::pair<Expression *, Expression *>(nullptr, bound);
    }
  } else {
    // Case -[*lbound..rbound]-
    Expression *lbound = ctx->expression()[0]->accept(this);
    Expression *rbound = ctx->expression()[1]->accept(this);
    return std::pair<Expression *, Expression *>(lbound, rbound);
  }
}

antlrcpp::Any CypherMainVisitor::visitExpression(
    CypherParser::ExpressionContext *ctx) {
  return static_cast<Expression *>(ctx->expression12()->accept(this));
}

// OR.
antlrcpp::Any CypherMainVisitor::visitExpression12(
    CypherParser::Expression12Context *ctx) {
  return LeftAssociativeOperatorExpression(ctx->expression11(), ctx->children,
                                           {CypherParser::OR});
}

// XOR.
antlrcpp::Any CypherMainVisitor::visitExpression11(
    CypherParser::Expression11Context *ctx) {
  return LeftAssociativeOperatorExpression(ctx->expression10(), ctx->children,
                                           {CypherParser::XOR});
}

// AND.
antlrcpp::Any CypherMainVisitor::visitExpression10(
    CypherParser::Expression10Context *ctx) {
  return LeftAssociativeOperatorExpression(ctx->expression9(), ctx->children,
                                           {CypherParser::AND});
}

// NOT.
antlrcpp::Any CypherMainVisitor::visitExpression9(
    CypherParser::Expression9Context *ctx) {
  return PrefixUnaryOperator(ctx->expression8(), ctx->children,
                             {CypherParser::NOT});
}

// Comparisons.
// Expresion 1 < 2 < 3 is converted to 1 < 2 && 2 < 3 and then binary operator
// ast node is constructed for each operator.
antlrcpp::Any CypherMainVisitor::visitExpression8(
    CypherParser::Expression8Context *ctx) {
  if (!ctx->partialComparisonExpression().size()) {
    // There is no comparison operators. We generate expression7.
    return ctx->expression7()->accept(this);
  }

  // There is at least one comparison. We need to generate code for each of
  // them. We don't call visitPartialComparisonExpression but do everything in
  // this function and call expression7-s directly. Since every expression7
  // can be generated twice (because it can appear in two comparisons) code
  // generated by whole subtree of expression7 must not have any sideeffects.
  // We handle chained comparisons as defined by mathematics, neo4j handles
  // them in a very interesting, illogical and incomprehensible way. For
  // example in neo4j:
  //  1 < 2 < 3 -> true,
  //  1 < 2 < 3 < 4 -> false,
  //  5 > 3 < 5 > 3 -> true,
  //  4 <= 5 < 7 > 6 -> false
  //  All of those comparisons evaluate to true in memgraph.
  std::vector<Expression *> children;
  children.push_back(ctx->expression7()->accept(this));
  std::vector<size_t> operators;
  auto partial_comparison_expressions = ctx->partialComparisonExpression();
  for (auto *child : partial_comparison_expressions) {
    children.push_back(child->expression7()->accept(this));
  }
  // First production is comparison operator.
  for (auto *child : partial_comparison_expressions) {
    operators.push_back(
        dynamic_cast<antlr4::tree::TerminalNode *>(child->children[0])
            ->getSymbol()
            ->getType());
  }

  // Make all comparisons.
  Expression *first_operand = children[0];
  std::vector<Expression *> comparisons;
  for (int i = 0; i < (int)operators.size(); ++i) {
    auto *expr = children[i + 1];
    // TODO: first_operand should only do lookup if it is only calculated and
    // not recalculated whole subexpression once again. SymbolGenerator should
    // generate symbol for every expresion and then lookup would be possible.
    comparisons.push_back(
        CreateBinaryOperatorByToken(operators[i], first_operand, expr));
    first_operand = expr;
  }

  first_operand = comparisons[0];
  // Calculate logical and of results of comparisons.
  for (int i = 1; i < (int)comparisons.size(); ++i) {
    first_operand = storage_.Create<AndOperator>(first_operand, comparisons[i]);
  }
  return first_operand;
}

antlrcpp::Any CypherMainVisitor::visitPartialComparisonExpression(
    CypherParser::PartialComparisonExpressionContext *) {
  debug_assert(false, "Should never be called. See documentation in hpp.");
  return 0;
}

// Addition and subtraction.
antlrcpp::Any CypherMainVisitor::visitExpression7(
    CypherParser::Expression7Context *ctx) {
  return LeftAssociativeOperatorExpression(ctx->expression6(), ctx->children,
                                           {kPlusTokenId, kMinusTokenId});
}

// Multiplication, division, modding.
antlrcpp::Any CypherMainVisitor::visitExpression6(
    CypherParser::Expression6Context *ctx) {
  return LeftAssociativeOperatorExpression(
      ctx->expression5(), ctx->children,
      {kMultTokenId, kDivTokenId, kModTokenId});
}

// Power.
antlrcpp::Any CypherMainVisitor::visitExpression5(
    CypherParser::Expression5Context *ctx) {
  if (ctx->expression4().size() > 1U) {
    // TODO: implement power operator. In neo4j power is left associative and
    // int^int -> float.
    throw utils::NotYetImplemented("power (^) operator");
  }
  return visitChildren(ctx);
}

// Unary minus and plus.
antlrcpp::Any CypherMainVisitor::visitExpression4(
    CypherParser::Expression4Context *ctx) {
  return PrefixUnaryOperator(ctx->expression3a(), ctx->children,
                             {kUnaryPlusTokenId, kUnaryMinusTokenId});
}

// IS NULL, IS NOT NULL, STARTS WITH, ..
antlrcpp::Any CypherMainVisitor::visitExpression3a(
    CypherParser::Expression3aContext *ctx) {
  Expression *expression = ctx->expression3b()->accept(this);

  for (auto *op : ctx->stringAndNullOperators()) {
    if (op->IS() && op->NOT() && op->CYPHERNULL()) {
      expression = static_cast<Expression *>(storage_.Create<NotOperator>(
          storage_.Create<IsNullOperator>(expression)));
    } else if (op->IS() && op->CYPHERNULL()) {
      expression = static_cast<Expression *>(
          storage_.Create<IsNullOperator>(expression));
    } else if (op->IN()) {
      expression = static_cast<Expression *>(storage_.Create<InListOperator>(
          expression, op->expression3b()->accept(this)));
    } else {
      std::function<TypedValue(const std::vector<TypedValue> &,
                               GraphDbAccessor &)>
          f;
      if (op->STARTS() && op->WITH()) {
        f = NameToFunction(kStartsWith);
      } else if (op->ENDS() && op->WITH()) {
        f = NameToFunction(kEndsWith);
      } else if (op->CONTAINS()) {
        f = NameToFunction(kContains);
      } else {
        throw utils::NotYetImplemented("function '{}'", op->getText());
      }
      auto expression2 = op->expression3b()->accept(this);
      std::vector<Expression *> args = {expression, expression2};
      expression =
          static_cast<Expression *>(storage_.Create<Function>(f, args));
    }
  }
  return expression;
}

antlrcpp::Any CypherMainVisitor::visitStringAndNullOperators(
    CypherParser::StringAndNullOperatorsContext *) {
  debug_assert(false, "Should never be called. See documentation in hpp.");
  return 0;
}

antlrcpp::Any CypherMainVisitor::visitExpression3b(
    CypherParser::Expression3bContext *ctx) {
  Expression *expression = ctx->expression2a()->accept(this);
  for (auto *list_op : ctx->listIndexingOrSlicing()) {
    if (list_op->getTokens(kDotsTokenId).size() == 0U) {
      // If there is no '..' then we need to create list indexing operator.
      expression = storage_.Create<ListIndexingOperator>(
          expression, list_op->expression()[0]->accept(this));
    } else if (!list_op->lower_bound && !list_op->upper_bound) {
      throw SemanticException(
          "List slicing operator requires at least one bound.");
    } else {
      Expression *lower_bound_ast =
          list_op->lower_bound
              ? static_cast<Expression *>(list_op->lower_bound->accept(this))
              : nullptr;
      Expression *upper_bound_ast =
          list_op->upper_bound
              ? static_cast<Expression *>(list_op->upper_bound->accept(this))
              : nullptr;
      expression = storage_.Create<ListSlicingOperator>(
          expression, lower_bound_ast, upper_bound_ast);
    }
  }
  return expression;
}

antlrcpp::Any CypherMainVisitor::visitListIndexingOrSlicing(
    CypherParser::ListIndexingOrSlicingContext *) {
  debug_assert(false, "Should never be called. See documentation in hpp.");
  return 0;
}

antlrcpp::Any CypherMainVisitor::visitExpression2a(
    CypherParser::Expression2aContext *ctx) {
  Expression *expression = ctx->expression2b()->accept(this);
  if (ctx->nodeLabels()) {
    auto labels =
        ctx->nodeLabels()->accept(this).as<std::vector<GraphDbTypes::Label>>();
    expression = storage_.Create<LabelsTest>(expression, labels);
  }
  return expression;
}

antlrcpp::Any CypherMainVisitor::visitExpression2b(
    CypherParser::Expression2bContext *ctx) {
  Expression *expression = ctx->atom()->accept(this);
  for (auto *lookup : ctx->propertyLookup()) {
    std::pair<std::string, GraphDbTypes::Property> key = lookup->accept(this);
    auto property_lookup =
        storage_.Create<PropertyLookup>(expression, key.first, key.second);
    expression = property_lookup;
  }
  return expression;
}

antlrcpp::Any CypherMainVisitor::visitAtom(CypherParser::AtomContext *ctx) {
  if (ctx->literal()) {
    return static_cast<Expression *>(
        ctx->literal()->accept(this).as<BaseLiteral *>());
  } else if (ctx->parameter()) {
    return static_cast<Expression *>(
        ctx->parameter()->accept(this).as<PrimitiveLiteral *>());
  } else if (ctx->parenthesizedExpression()) {
    return static_cast<Expression *>(
        ctx->parenthesizedExpression()->accept(this));
  } else if (ctx->variable()) {
    std::string variable = ctx->variable()->accept(this);
    users_identifiers.insert(variable);
    return static_cast<Expression *>(storage_.Create<Identifier>(variable));
  } else if (ctx->functionInvocation()) {
    return static_cast<Expression *>(ctx->functionInvocation()->accept(this));
  } else if (ctx->COUNT()) {
    // Here we handle COUNT(*). COUNT(expression) is handled in
    // visitFunctionInvocation with other aggregations. This is visible in
    // functionInvocation and atom producions in opencypher grammar.
    return static_cast<Expression *>(
        storage_.Create<Aggregation>(nullptr, Aggregation::Op::COUNT));
  } else if (ctx->ALL()) {
    auto *ident = storage_.Create<Identifier>(ctx->filterExpression()
                                                  ->idInColl()
                                                  ->variable()
                                                  ->accept(this)
                                                  .as<std::string>());
    Expression *list_expr =
        ctx->filterExpression()->idInColl()->expression()->accept(this);
    Where *where = ctx->filterExpression()->where()->accept(this);
    return static_cast<Expression *>(
        storage_.Create<All>(ident, list_expr, where));
  } else if (ctx->caseExpression()) {
    return static_cast<Expression *>(ctx->caseExpression()->accept(this));
  }
  // TODO: Implement this. We don't support comprehensions, filtering... at
  // the moment.
  throw utils::NotYetImplemented("atom expression '{}'", ctx->getText());
}

antlrcpp::Any CypherMainVisitor::visitParameter(
    CypherParser::ParameterContext *ctx) {
  return storage_.Create<PrimitiveLiteral>(
      ctx->getText(),  // Not really important since we do parameter
                       // substitution by token position not by its name.
                       // Lookup by name is already done in stage before.
      ctx->getStart()->getTokenIndex());
}

antlrcpp::Any CypherMainVisitor::visitLiteral(
    CypherParser::LiteralContext *ctx) {
  int token_position = ctx->getStart()->getTokenIndex();
  if (ctx->CYPHERNULL()) {
    return static_cast<BaseLiteral *>(
        storage_.Create<PrimitiveLiteral>(TypedValue::Null, token_position));
  } else if (ctx->StringLiteral()) {
    return static_cast<BaseLiteral *>(storage_.Create<PrimitiveLiteral>(
        visitStringLiteral(ctx->StringLiteral()->getText()).as<std::string>(),
        token_position));
  } else if (ctx->booleanLiteral()) {
    return static_cast<BaseLiteral *>(storage_.Create<PrimitiveLiteral>(
        ctx->booleanLiteral()->accept(this).as<bool>(), token_position));
  } else if (ctx->numberLiteral()) {
    return static_cast<BaseLiteral *>(storage_.Create<PrimitiveLiteral>(
        ctx->numberLiteral()->accept(this).as<TypedValue>(), token_position));
  } else if (ctx->listLiteral()) {
    return static_cast<BaseLiteral *>(storage_.Create<ListLiteral>(
        ctx->listLiteral()->accept(this).as<std::vector<Expression *>>()));
  } else {
    return static_cast<BaseLiteral *>(storage_.Create<MapLiteral>(
        ctx->mapLiteral()
            ->accept(this)
            .as<std::map<std::pair<std::string, GraphDbTypes::Property>,
                         Expression *>>()));
  }
  return visitChildren(ctx);
}

antlrcpp::Any CypherMainVisitor::visitParenthesizedExpression(
    CypherParser::ParenthesizedExpressionContext *ctx) {
  return static_cast<Expression *>(ctx->expression()->accept(this));
}

antlrcpp::Any CypherMainVisitor::visitNumberLiteral(
    CypherParser::NumberLiteralContext *ctx) {
  if (ctx->integerLiteral()) {
    return TypedValue(ctx->integerLiteral()->accept(this).as<int64_t>());
  } else if (ctx->doubleLiteral()) {
    return TypedValue(ctx->doubleLiteral()->accept(this).as<double>());
  } else {
    // This should never happen, except grammar changes and we don't notice
    // change in this production.
    debug_assert(false, "can't happen");
    throw std::exception();
  }
}

antlrcpp::Any CypherMainVisitor::visitFunctionInvocation(
    CypherParser::FunctionInvocationContext *ctx) {
  if (ctx->DISTINCT()) {
    throw utils::NotYetImplemented("DISTINCT function call");
  }
  std::string function_name = ctx->functionName()->accept(this);
  std::vector<Expression *> expressions;
  for (auto *expression : ctx->expression()) {
    expressions.push_back(expression->accept(this));
  }
  if (expressions.size() == 1U) {
    if (function_name == Aggregation::kCount) {
      return static_cast<Expression *>(
          storage_.Create<Aggregation>(expressions[0], Aggregation::Op::COUNT));
    }
    if (function_name == Aggregation::kMin) {
      return static_cast<Expression *>(
          storage_.Create<Aggregation>(expressions[0], Aggregation::Op::MIN));
    }
    if (function_name == Aggregation::kMax) {
      return static_cast<Expression *>(
          storage_.Create<Aggregation>(expressions[0], Aggregation::Op::MAX));
    }
    if (function_name == Aggregation::kSum) {
      return static_cast<Expression *>(
          storage_.Create<Aggregation>(expressions[0], Aggregation::Op::SUM));
    }
    if (function_name == Aggregation::kAvg) {
      return static_cast<Expression *>(
          storage_.Create<Aggregation>(expressions[0], Aggregation::Op::AVG));
    }
    if (function_name == Aggregation::kCollect) {
      return static_cast<Expression *>(storage_.Create<Aggregation>(
          expressions[0], Aggregation::Op::COLLECT));
    }
  }
  auto function = NameToFunction(function_name);
  if (!function)
    throw SemanticException("Function '{}' doesn't exist.", function_name);
  return static_cast<Expression *>(
      storage_.Create<Function>(function, expressions));
}

antlrcpp::Any CypherMainVisitor::visitFunctionName(
    CypherParser::FunctionNameContext *ctx) {
  return utils::ToUpperCase(ctx->getText());
}

antlrcpp::Any CypherMainVisitor::visitDoubleLiteral(
    CypherParser::DoubleLiteralContext *ctx) {
  return ParseDoubleLiteral(ctx->getText());
}

antlrcpp::Any CypherMainVisitor::visitIntegerLiteral(
    CypherParser::IntegerLiteralContext *ctx) {
  return ParseIntegerLiteral(ctx->getText());
}

antlrcpp::Any CypherMainVisitor::visitStringLiteral(
    const std::string &escaped) {
  return ParseStringLiteral(escaped);
}

antlrcpp::Any CypherMainVisitor::visitBooleanLiteral(
    CypherParser::BooleanLiteralContext *ctx) {
  if (ctx->getTokens(CypherParser::TRUE).size()) {
    return true;
  }
  if (ctx->getTokens(CypherParser::FALSE).size()) {
    return false;
  }
  debug_assert(false, "Shouldn't happend");
  throw std::exception();
}

antlrcpp::Any CypherMainVisitor::visitCypherDelete(
    CypherParser::CypherDeleteContext *ctx) {
  auto *del = storage_.Create<Delete>();
  if (ctx->DETACH()) {
    del->detach_ = true;
  }
  for (auto *expression : ctx->expression()) {
    del->expressions_.push_back(expression->accept(this));
  }
  return del;
}

antlrcpp::Any CypherMainVisitor::visitWhere(CypherParser::WhereContext *ctx) {
  auto *where = storage_.Create<Where>();
  where->expression_ = ctx->expression()->accept(this);
  return where;
}

antlrcpp::Any CypherMainVisitor::visitSet(CypherParser::SetContext *ctx) {
  std::vector<Clause *> set_items;
  for (auto *set_item : ctx->setItem()) {
    set_items.push_back(set_item->accept(this));
  }
  return set_items;
}

antlrcpp::Any CypherMainVisitor::visitSetItem(
    CypherParser::SetItemContext *ctx) {
  // SetProperty
  if (ctx->propertyExpression()) {
    auto *set_property = storage_.Create<SetProperty>();
    set_property->property_lookup_ = ctx->propertyExpression()->accept(this);
    set_property->expression_ = ctx->expression()->accept(this);
    return static_cast<Clause *>(set_property);
  }

  // SetProperties either assignment or update
  if (ctx->getTokens(kPropertyAssignmentTokenId).size() ||
      ctx->getTokens(kPropertyUpdateTokenId).size()) {
    auto *set_properties = storage_.Create<SetProperties>();
    set_properties->identifier_ = storage_.Create<Identifier>(
        ctx->variable()->accept(this).as<std::string>());
    set_properties->expression_ = ctx->expression()->accept(this);
    if (ctx->getTokens(kPropertyUpdateTokenId).size()) {
      set_properties->update_ = true;
    }
    return static_cast<Clause *>(set_properties);
  }

  // SetLabels
  auto *set_labels = storage_.Create<SetLabels>();
  set_labels->identifier_ = storage_.Create<Identifier>(
      ctx->variable()->accept(this).as<std::string>());
  set_labels->labels_ =
      ctx->nodeLabels()->accept(this).as<std::vector<GraphDbTypes::Label>>();
  return static_cast<Clause *>(set_labels);
}

antlrcpp::Any CypherMainVisitor::visitRemove(CypherParser::RemoveContext *ctx) {
  std::vector<Clause *> remove_items;
  for (auto *remove_item : ctx->removeItem()) {
    remove_items.push_back(remove_item->accept(this));
  }
  return remove_items;
}

antlrcpp::Any CypherMainVisitor::visitRemoveItem(
    CypherParser::RemoveItemContext *ctx) {
  // RemoveProperty
  if (ctx->propertyExpression()) {
    auto *remove_property = storage_.Create<RemoveProperty>();
    remove_property->property_lookup_ = ctx->propertyExpression()->accept(this);
    return static_cast<Clause *>(remove_property);
  }

  // RemoveLabels
  auto *remove_labels = storage_.Create<RemoveLabels>();
  remove_labels->identifier_ = storage_.Create<Identifier>(
      ctx->variable()->accept(this).as<std::string>());
  remove_labels->labels_ =
      ctx->nodeLabels()->accept(this).as<std::vector<GraphDbTypes::Label>>();
  return static_cast<Clause *>(remove_labels);
}

antlrcpp::Any CypherMainVisitor::visitPropertyExpression(
    CypherParser::PropertyExpressionContext *ctx) {
  Expression *expression = ctx->atom()->accept(this);
  for (auto *lookup : ctx->propertyLookup()) {
    std::pair<std::string, GraphDbTypes::Property> key = lookup->accept(this);
    auto property_lookup =
        storage_.Create<PropertyLookup>(expression, key.first, key.second);
    expression = property_lookup;
  }
  // It is guaranteed by grammar that there is at least one propertyLookup.
  return dynamic_cast<PropertyLookup *>(expression);
}

antlrcpp::Any CypherMainVisitor::visitCaseExpression(
    CypherParser::CaseExpressionContext *ctx) {
  Expression *test_expression =
      ctx->test ? ctx->test->accept(this).as<Expression *>() : nullptr;
  auto alternatives = ctx->caseAlternatives();
  // Reverse alternatives so that tree of IfOperators can be built bottom-up.
  std::reverse(alternatives.begin(), alternatives.end());
  Expression *else_expression =
      ctx->else_expression
          ? ctx->else_expression->accept(this).as<Expression *>()
          : storage_.Create<PrimitiveLiteral>(TypedValue::Null);
  for (auto *alternative : alternatives) {
    Expression *condition =
        test_expression
            ? storage_.Create<EqualOperator>(
                  test_expression, alternative->when_expression->accept(this))
            : alternative->when_expression->accept(this).as<Expression *>();
    Expression *then_expression = alternative->then_expression->accept(this);
    else_expression = storage_.Create<IfOperator>(condition, then_expression,
                                                  else_expression);
  }
  return else_expression;
}

antlrcpp::Any CypherMainVisitor::visitCaseAlternatives(
    CypherParser::CaseAlternativesContext *) {
  debug_fail("Should never be called. See documentation in hpp.");
  return 0;
}

antlrcpp::Any CypherMainVisitor::visitWith(CypherParser::WithContext *ctx) {
  auto *with = storage_.Create<With>();
  in_with_ = true;
  with->body_ = ctx->returnBody()->accept(this);
  in_with_ = false;
  if (ctx->DISTINCT()) {
    with->body_.distinct = true;
  }
  if (ctx->where()) {
    with->where_ = ctx->where()->accept(this);
  }
  return with;
}

antlrcpp::Any CypherMainVisitor::visitMerge(CypherParser::MergeContext *ctx) {
  auto *merge = storage_.Create<Merge>();
  merge->pattern_ = ctx->patternPart()->accept(this);
  for (auto &merge_action : ctx->mergeAction()) {
    auto set = merge_action->set()->accept(this).as<std::vector<Clause *>>();
    if (merge_action->MATCH()) {
      merge->on_match_.insert(merge->on_match_.end(), set.begin(), set.end());
    } else {
      debug_assert(merge_action->CREATE(), "Expected ON MATCH or ON CREATE");
      merge->on_create_.insert(merge->on_create_.end(), set.begin(), set.end());
    }
  }
  return merge;
}

antlrcpp::Any CypherMainVisitor::visitUnwind(CypherParser::UnwindContext *ctx) {
  auto *named_expr = storage_.Create<NamedExpression>();
  named_expr->expression_ = ctx->expression()->accept(this);
  named_expr->name_ =
      std::string(ctx->variable()->accept(this).as<std::string>());
  return storage_.Create<Unwind>(named_expr);
}

antlrcpp::Any CypherMainVisitor::visitFilterExpression(
    CypherParser::FilterExpressionContext *) {
  debug_fail("Should never be called. See documentation in hpp.");
  return 0;
}

}  // namespace query::frontend
