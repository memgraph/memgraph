#include "query/frontend/ast/cypher_main_visitor.hpp"

#include <climits>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "database/graph_db.hpp"
#include "query/frontend/ast/named_antlr_tokens.hpp"
#include "utils/assert.hpp"

namespace query {
namespace frontend {

namespace {
// Map children tokens of antlr node to Function enum.
// std::vector<Function> MapTokensToOperators(
//    antlr4::ParserRuleContext *node,
//    const std::unordered_map<size_t, Function> token_to_operator) {
//  std::vector<antlr4::tree::TerminalNode *> tokens;
//  for (const auto &x : token_to_operator) {
//    tokens.insert(tokens.end(), node->getTokens(x.first).begin(),
//                  node->getTokens(x.first).end());
//  }
//  sort(tokens.begin(), tokens.end(), [](antlr4::tree::TerminalNode *a,
//                                        antlr4::tree::TerminalNode *b) {
//    return
//    a->getSourceInterval().startsBeforeDisjoint(b->getSourceInterval());
//  });
//  std::vector<Function> ops;
//  for (auto *token : tokens) {
//    auto it = token_to_operator.find(token->getSymbol()->getType());
//    debug_assert(it != token_to_operator.end(),
//                 "Wrong mapping sent to function.");
//    ops.push_back(it->second);
//  }
//  return ops;
//}
}

const std::string CypherMainVisitor::kAnonPrefix = "anon";

antlrcpp::Any
CypherMainVisitor::visitSingleQuery(CypherParser::SingleQueryContext *ctx) {
  query_ = storage_.query();
  for (auto *child : ctx->clause()) {
    query_->clauses_.push_back(child->accept(this));
  }
  // Construct unique names for anonymous identifiers;
  int id = 1;
  for (auto **identifier : anonymous_identifiers) {
    while (true) {
      std::string id_name = kAnonPrefix + std::to_string(id++);
      if (users_identifiers.find(id_name) == users_identifiers.end()) {
        *identifier = storage_.Create<Identifier>(id_name);
        break;
      }
    }
  }
  return query_;
}

antlrcpp::Any CypherMainVisitor::visitClause(CypherParser::ClauseContext *ctx) {
  if (ctx->cypherReturn()) {
    return (Clause *)ctx->cypherReturn()->accept(this).as<Return *>();
  }
  if (ctx->cypherMatch()) {
    return (Clause *)ctx->cypherMatch()->accept(this).as<Match *>();
  }
  throw std::exception();
  return visitChildren(ctx);
}

antlrcpp::Any
CypherMainVisitor::visitCypherMatch(CypherParser::CypherMatchContext *ctx) {
  auto *match = storage_.Create<Match>();
  if (ctx->OPTIONAL() || ctx->where()) {
    throw std::exception();
  }
  match->patterns_ = ctx->pattern()->accept(this).as<std::vector<Pattern *>>();
  return match;
}

antlrcpp::Any
CypherMainVisitor::visitCypherReturn(CypherParser::CypherReturnContext *ctx) {
  if (ctx->DISTINCT()) {
    throw std::exception();
  }
  return visitChildren(ctx);
}

antlrcpp::Any
CypherMainVisitor::visitReturnBody(CypherParser::ReturnBodyContext *ctx) {
  if (ctx->order() || ctx->skip() || ctx->limit()) {
    throw std::exception();
  }
  return ctx->returnItems()->accept(this);
}

antlrcpp::Any
CypherMainVisitor::visitReturnItems(CypherParser::ReturnItemsContext *ctx) {
  auto *return_clause = storage_.Create<Return>();
  if (ctx->getTokens(kReturnAllTokenId).size()) {
    throw std::exception();
  }
  for (auto *item : ctx->returnItem()) {
    return_clause->named_expressions_.push_back(item->accept(this));
  }
  return return_clause;
}

antlrcpp::Any
CypherMainVisitor::visitReturnItem(CypherParser::ReturnItemContext *ctx) {
  auto *named_expr = storage_.Create<NamedExpression>();
  if (ctx->variable()) {
    named_expr->name_ =
        std::string(ctx->variable()->accept(this).as<std::string>());
  } else {
    // TODO: Should we get this by text or some escaping is needed?
    named_expr->name_ = std::string(ctx->getText());
  }
  named_expr->expression_ = ctx->expression()->accept(this);
  return named_expr;
}

antlrcpp::Any
CypherMainVisitor::visitNodePattern(CypherParser::NodePatternContext *ctx) {
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
        ctx->nodeLabels()->accept(this).as<std::vector<GraphDb::Label>>();
  }
  if (ctx->properties()) {
    throw std::exception();
    //    node.properties = ctx->properties()
    //                          ->accept(this)
    //                          .as<std::unordered_map<std::string,
    //                          std::string>>();
  }
  return node;
}

antlrcpp::Any
CypherMainVisitor::visitNodeLabels(CypherParser::NodeLabelsContext *ctx) {
  std::vector<GraphDb::Label> labels;
  for (auto *node_label : ctx->nodeLabel()) {
    labels.push_back(ctx_.db_accessor_.label(node_label->accept(this)));
  }
  return labels;
}

antlrcpp::Any
CypherMainVisitor::visitProperties(CypherParser::PropertiesContext *ctx) {
  if (!ctx->mapLiteral()) {
    // If child is not mapLiteral that means child is params. At the moment
    // memgraph doesn't support params.
    throw std::exception();
  }
  return ctx->mapLiteral()->accept(this);
}

antlrcpp::Any
CypherMainVisitor::visitMapLiteral(CypherParser::MapLiteralContext *ctx) {
  throw std::exception();
  (void)ctx;
  return 0;
  //  std::unordered_map<std::string, std::string> map;
  //  for (int i = 0; i < (int)ctx->propertyKeyName().size(); ++i) {
  //    map[ctx->propertyKeyName()[i]->accept(this).as<std::string>()] =
  //        ctx->expression()[i]->accept(this).as<std::string>();
  //  }
  //  return map;
}

antlrcpp::Any
CypherMainVisitor::visitSymbolicName(CypherParser::SymbolicNameContext *ctx) {
  if (ctx->EscapedSymbolicName()) {
    // We don't allow at this point for variable to be EscapedSymbolicName
    // because we would have t ofigure out how escaping works since same
    // variable can be referenced in two ways: escaped and unescaped.
    throw std::exception();
  }
  // TODO: We should probably escape string.
  return std::string(ctx->getText());
}

antlrcpp::Any
CypherMainVisitor::visitPattern(CypherParser::PatternContext *ctx) {
  std::vector<Pattern *> patterns;
  for (auto *pattern_part : ctx->patternPart()) {
    patterns.push_back(pattern_part->accept(this));
  }
  return patterns;
}

antlrcpp::Any
CypherMainVisitor::visitPatternPart(CypherParser::PatternPartContext *ctx) {
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
  auto *edge = storage_.Create<EdgeAtom>();
  if (ctx->relationshipDetail()) {
    if (ctx->relationshipDetail()->variable()) {
      std::string variable =
          ctx->relationshipDetail()->variable()->accept(this);
      edge->identifier_ = storage_.Create<Identifier>(variable);
      users_identifiers.insert(variable);
    }
    if (ctx->relationshipDetail()->relationshipTypes()) {
      edge->types_ = ctx->relationshipDetail()
                         ->relationshipTypes()
                         ->accept(this)
                         .as<std::vector<GraphDb::EdgeType>>();
    }
    if (ctx->relationshipDetail()->properties()) {
      throw std::exception();
    }
    if (ctx->relationshipDetail()->rangeLiteral()) {
      throw std::exception();
    }
  }
  //    relationship.has_range = true;
  //    auto range = ctx->relationshipDetail()
  //                     ->rangeLiteral()
  //                     ->accept(this)
  //                     .as<std::pair<int64_t, int64_t>>();
  //    relationship.lower_bound = range.first;
  //    relationship.upper_bound = range.second;
  if (!edge->identifier_) {
    anonymous_identifiers.push_back(&edge->identifier_);
  }

  if (ctx->leftArrowHead() && !ctx->rightArrowHead()) {
    edge->direction_ = EdgeAtom::Direction::LEFT;
  } else if (!ctx->leftArrowHead() && ctx->rightArrowHead()) {
    edge->direction_ = EdgeAtom::Direction::RIGHT;
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

antlrcpp::Any CypherMainVisitor::visitRelationshipTypes(
    CypherParser::RelationshipTypesContext *ctx) {
  std::vector<GraphDb::EdgeType> types;
  for (auto *edge_type : ctx->relTypeName()) {
    types.push_back(ctx_.db_accessor_.edge_type(edge_type->accept(this)));
  }
  return types;
}

antlrcpp::Any
CypherMainVisitor::visitRangeLiteral(CypherParser::RangeLiteralContext *ctx) {
  if (ctx->integerLiteral().size() == 0U) {
    // -[*]-
    return std::pair<int64_t, int64_t>(1LL, LLONG_MAX);
  } else if (ctx->integerLiteral().size() == 1U) {
    auto dots_tokens = ctx->getTokens(kDotsTokenId);
    int64_t bound = ctx->integerLiteral()[0]->accept(this).as<int64_t>();
    if (!dots_tokens.size()) {
      // -[*2]-
      return std::pair<int64_t, int64_t>(bound, bound);
    }
    if (dots_tokens[0]->getSourceInterval().startsAfter(
            ctx->integerLiteral()[0]->getSourceInterval())) {
      // -[*2..]-
      return std::pair<int64_t, int64_t>(bound, LLONG_MAX);
    } else {
      // -[*..2]-
      return std::pair<int64_t, int64_t>(1LL, bound);
    }
  } else {
    int64_t lbound = ctx->integerLiteral()[0]->accept(this).as<int64_t>();
    int64_t rbound = ctx->integerLiteral()[1]->accept(this).as<int64_t>();
    // -[*2..5]-
    return std::pair<int64_t, int64_t>(lbound, rbound);
  }
}

antlrcpp::Any
CypherMainVisitor::visitExpression(CypherParser::ExpressionContext *ctx) {
  return visitChildren(ctx);
}

//// OR.
// antlrcpp::Any
// CypherMainVisitor::visitExpression12(CypherParser::Expression12Context *ctx)
// {
//  return LeftAssociativeOperatorExpression(ctx->expression11(),
//                                           Function::LOGICAL_OR);
//}
//
//// XOR.
// antlrcpp::Any
// CypherMainVisitor::visitExpression11(CypherParser::Expression11Context *ctx)
// {
//  return LeftAssociativeOperatorExpression(ctx->expression10(),
//                                           Function::LOGICAL_XOR);
//}
//
//// AND.
// antlrcpp::Any
// CypherMainVisitor::visitExpression10(CypherParser::Expression10Context *ctx)
// {
//  return LeftAssociativeOperatorExpression(ctx->expression9(),
//                                           Function::LOGICAL_AND);
//}
//
//// NOT.
// antlrcpp::Any
// CypherMainVisitor::visitExpression9(CypherParser::Expression9Context *ctx) {
//  // TODO: make template similar to LeftAssociativeOperatorExpression for
//  unary
//  // expresssions.
//  auto operand = ctx->expression8()->accept(this).as<std::string>();
//  for (int i = 0; i < (int)ctx->NOT().size(); ++i) {
//    auto lhs_id = new_id();
//    symbol_table_[lhs_id] = SimpleExpression{Function::LOGICAL_NOT,
//    {operand}};
//    operand = lhs_id;
//  }
//  return operand;
//}
//
//// Comparisons.
// antlrcpp::Any
// CypherMainVisitor::visitExpression8(CypherParser::Expression8Context *ctx) {
//  if (!ctx->partialComparisonExpression().size()) {
//    // There is no comparison operators. We generate expression7.
//    return ctx->expression7()->accept(this);
//  }
//
//  // There is at least one comparison. We need to generate code for each of
//  // them. We don't call visitPartialComparisonExpression but do everything in
//  // this function and call expression7-s directly. Since every expression7
//  // can be generated twice (because it can appear in two comparisons) code
//  // generated by whole subtree of expression7 must not have any sideeffects.
//  // We handle chained comparisons as defined by mathematics, neo4j handles
//  // them in a very interesting, illogical and incomprehensible way. For
//  // example in neo4j:
//  //  1 < 2 < 3 -> true,
//  //  1 < 2 < 3 < 4 -> false,
//  //  5 > 3 < 5 > 3 -> true,
//  //  4 <= 5 < 7 > 6 -> false
//  //  All of those comparisons evaluate to true in memgraph.
//  std::vector<std::string> children_ids;
//  children_ids.push_back(ctx->expression7()->accept(this).as<std::string>());
//  auto partial_comparison_expressions = ctx->partialComparisonExpression();
//  for (auto *child : partial_comparison_expressions) {
//    children_ids.push_back(child->accept(this).as<std::string>());
//  }
//
//  // Make all comparisons.
//  std::string first_operand = children_ids[0];
//  std::vector<std::string> comparison_ids;
//  for (int i = 0; i < (int)partial_comparison_expressions.size(); ++i) {
//    auto *expr = partial_comparison_expressions[i];
//    auto op = [](CypherParser::PartialComparisonExpressionContext *expr) {
//      if (expr->getToken(kEqTokenId, 0)) {
//        return Function::EQ;
//      } else if (expr->getToken(kNeTokenId1, 0) ||
//                 expr->getToken(kNeTokenId2, 0)) {
//        return Function::NE;
//      } else if (expr->getToken(kLtTokenId, 0)) {
//        return Function::LT;
//      } else if (expr->getToken(kGtTokenId, 0)) {
//        return Function::GT;
//      } else if (expr->getToken(kLeTokenId, 0)) {
//        return Function::LE;
//      } else if (expr->getToken(kGeTokenId, 0)) {
//        return Function::GE;
//      }
//      assert(false);
//      return Function::GE;
//    }(expr);
//    auto lhs_id = new_id();
//    symbol_table_[lhs_id] =
//        SimpleExpression{op, {first_operand, children_ids[i + 1]}};
//    first_operand = lhs_id;
//    comparison_ids.push_back(lhs_id);
//  }
//
//  first_operand = comparison_ids[0];
//  // Calculate logical and of results of comparisons.
//  for (int i = 1; i < (int)comparison_ids.size(); ++i) {
//    auto lhs_id = new_id();
//    symbol_table_[lhs_id] = SimpleExpression{
//        Function::LOGICAL_AND, {first_operand, comparison_ids[i]}};
//    first_operand = lhs_id;
//  }
//  return first_operand;
//}
//
// antlrcpp::Any CypherMainVisitor::visitPartialComparisonExpression(
//    CypherParser::PartialComparisonExpressionContext *) {
//  debug_assert(false, "Should never be called. See documentation in hpp.");
//  return 0;
//}
//
//// Addition and subtraction.
// antlrcpp::Any
// CypherMainVisitor::visitExpression7(CypherParser::Expression7Context *ctx) {
//  return LeftAssociativeOperatorExpression(
//      ctx->expression6(),
//      MapTokensToOperators(ctx, {{kPlusTokenId, Function::ADDITION},
//                                 {kMinusTokenId, Function::SUBTRACTION}}));
//}
//
//// Multiplication, division, modding.
// antlrcpp::Any
// CypherMainVisitor::visitExpression6(CypherParser::Expression6Context *ctx) {
//  return LeftAssociativeOperatorExpression(
//      ctx->expression5(),
//      MapTokensToOperators(ctx, {{kMultTokenId, Function::MULTIPLICATION},
//                                 {kDivTokenId, Function::DIVISION},
//                                 {kModTokenId, Function::MODULO}}));
//}
//
//// Power.
// antlrcpp::Any
// CypherMainVisitor::visitExpression5(CypherParser::Expression5Context *ctx) {
//  if (ctx->expression4().size() > 1u) {
//    // TODO: implement power operator. In neo4j power is right associative and
//    // int^int -> float.
//    throw SemanticException();
//  }
//  return visitChildren(ctx);
//}
//
//// Unary minus and plus.
// antlrcpp::Any
// CypherMainVisitor::visitExpression4(CypherParser::Expression4Context *ctx) {
//  auto ops =
//      MapTokensToOperators(ctx, {{kUnaryPlusTokenId, Function::UNARY_PLUS},
//                                 {kUnaryMinusTokenId,
//                                 Function::UNARY_MINUS}});
//  auto operand = ctx->expression3()->accept(this).as<std::string>();
//  for (int i = 0; i < (int)ops.size(); ++i) {
//    auto lhs_id = new_id();
//    symbol_table_[lhs_id] = SimpleExpression{ops[i], {operand}};
//    operand = lhs_id;
//  }
//  return operand;
//}
//
// antlrcpp::Any
// CypherMainVisitor::visitExpression3(CypherParser::Expression3Context *ctx) {
//  // If there is only one child we don't need to generate any code in this
//  since
//  // that child is expression2. Other operations are not implemented at the
//  // moment.
//  // TODO: implement this.
//  if (ctx->children.size() > 1u) {
//    throw SemanticException();
//  }
//  return visitChildren(ctx);
//}
//
// antlrcpp::Any
// CypherMainVisitor::visitExpression2(CypherParser::Expression2Context *ctx) {
//  if (ctx->nodeLabels().size()) {
//    // TODO: Implement this. We don't currently support label checking in
//    // expresssion.
//    throw SemanticException();
//  }
//  auto operand = ctx->atom()->accept(this).as<std::string>();
//  for (int i = 0; i < (int)ctx->propertyLookup().size(); ++i) {
//    auto lhs_id = new_id();
//    symbol_table_[lhs_id] =
//        SimpleExpression{Function::PROPERTY_GETTER, {operand}};
//    operand = lhs_id;
//  }
//  return operand;
//}

antlrcpp::Any CypherMainVisitor::visitAtom(CypherParser::AtomContext *ctx) {
  if (ctx->literal()) {
    // This is not very nice since we didn't parse text given in query, but we
    // left that job to the code generator. Correct approach would be to parse
    // it and store it in a structure of appropriate type, int, string... And
    // then code generator would generate its own text based on structure. This
    // is also a security risk if code generator doesn't parse and escape
    // text appropriately. At the moment we don;t care much since literal will
    // appear only in tests and real queries will be stripped.
    // TODO: Either parse it correctly or raise exception. If exception is
    // raised it tests should also use params instead of literals.
    //    auto text = ctx->literal()->getText();
    //    auto lhs_id = new_id();
    //    symbol_table_[lhs_id] = SimpleExpression{Function::LITERAL, {text}};
    //    return lhs_id;
    throw std::exception();
  } else if (ctx->parameter()) {
    // This is once again potential security risk. We shouldn't output text
    // given in user query as parameter name directly to the code. Stripper
    // should either replace user's parameter name with generic one or we should
    // allow only parameters with numeric names. At the moment this is not a
    // problem since we don't accept user's parameters but only ours.
    // TODO: revise this.
    //    auto text = ctx->literal()->getText();
    //    auto lhs_id = new_id();
    //    symbol_table_[lhs_id] = SimpleExpression{Function::PARAMETER, {text}};
    throw std::exception();
    //    return lhs_id;
  } else if (ctx->parenthesizedExpression()) {
    return ctx->parenthesizedExpression()->accept(this);
  } else if (ctx->variable()) {
    std::string variable = ctx->variable()->accept(this);
    users_identifiers.insert(variable);
    return storage_.Create<Identifier>(variable);
  }
  // TODO: Implement this. We don't support comprehensions, functions,
  // filtering... at the moment.
  throw std::exception();
}

antlrcpp::Any CypherMainVisitor::visitIntegerLiteral(
    CypherParser::IntegerLiteralContext *ctx) {
  int64_t t = 0LL;
  try {
    t = std::stoll(ctx->getText(), 0, 0);
  } catch (std::out_of_range) {
    throw std::exception();
  }
  return t;
}
}
}
