#include "query/backend/cpp/cypher_main_visitor.hpp"

#include <climits>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "query/backend/cpp/compiler_structures.hpp"
#include "utils/assert.hpp"

namespace {
// List of unnamed tokens visitor needs to use. This should be reviewed on every
// grammar change since even changes in ordering of rules will cause antlr to
// generate different constants for unnamed tokens.
const auto kDotsTokenId = CypherParser::T__12;  // ..
}

antlrcpp::Any CypherMainVisitor::visitNodePattern(
    CypherParser::NodePatternContext *ctx) {
  Node node;
  node.output_identifier = new_identifier();
  if (ctx->variable()) {
    identifiers_map_[ctx->variable()->accept(this).as<std::string>()] =
        node.output_identifier;
  }
  if (ctx->nodeLabels()) {
    node.labels =
        ctx->nodeLabels()->accept(this).as<std::vector<std::string>>();
  }
  if (ctx->properties()) {
    node.properties =
        ctx->properties()
            ->accept(this)
            .as<std::unordered_map<std::string,
                                   CypherParser::ExpressionContext *>>();
  }
  symbol_table_[node.output_identifier] = node;
  return node;
}

antlrcpp::Any CypherMainVisitor::visitNodeLabels(
    CypherParser::NodeLabelsContext *ctx) {
  std::vector<std::string> labels;
  for (auto *node_label : ctx->nodeLabel()) {
    labels.push_back(node_label->accept(this).as<std::string>());
  }
  return labels;
}

antlrcpp::Any CypherMainVisitor::visitProperties(
    CypherParser::PropertiesContext *ctx) {
  if (!ctx->mapLiteral()) {
    // If child is not mapLiteral that means child is params. At the moment
    // memgraph doesn't support params.
    throw SemanticException();
  }
  return ctx->mapLiteral()->accept(this);
}

antlrcpp::Any CypherMainVisitor::visitMapLiteral(
    CypherParser::MapLiteralContext *ctx) {
  std::unordered_map<std::string, CypherParser::ExpressionContext *> map;
  for (int i = 0; i < (int)ctx->propertyKeyName().size(); ++i) {
    map[ctx->propertyKeyName()[i]->accept(this).as<std::string>()] =
        ctx->expression()[i];
  }
  return map;
}

antlrcpp::Any CypherMainVisitor::visitSymbolicName(
    CypherParser::SymbolicNameContext *ctx) {
  if (!ctx->UnescapedSymbolicName()) {
    // SymbolicName can only be UnescapedSymbolicName. At this moment we want to
    // avoid openCypher crazyness that allows variables to be named as keywords
    // and escaped sequences. To allow all possible variable names allowed by
    // openCypher grammar we need to figure out escaping rules so we can
    // reference same variable as unescaped and escaped string.
    throw SemanticException();
  }
  return ctx->getText();
}

antlrcpp::Any CypherMainVisitor::visitPattern(
    CypherParser::PatternContext *ctx) {
  std::vector<PatternPart> pattern;
  for (auto *pattern_part : ctx->patternPart()) {
    pattern.push_back(pattern_part->accept(this).as<PatternPart>());
  }
  return pattern;
}

antlrcpp::Any CypherMainVisitor::visitPatternPart(
    CypherParser::PatternPartContext *ctx) {
  PatternPart pattern_part =
      ctx->anonymousPatternPart()->accept(this).as<PatternPart>();
  if (ctx->variable()) {
    identifiers_map_[ctx->variable()->accept(this).as<std::string>()] =
        pattern_part.output_identifier;
  }
  symbol_table_[pattern_part.output_identifier] = pattern_part;
  return pattern_part;
}

antlrcpp::Any CypherMainVisitor::visitPatternElement(
    CypherParser::PatternElementContext *ctx) {
  if (ctx->patternElement()) {
    return ctx->patternElement()->accept(this);
  }
  PatternPart pattern_part;
  pattern_part.output_identifier = new_identifier();
  pattern_part.nodes.push_back(ctx->nodePattern()->accept(this).as<Node>());
  for (auto *pattern_element_chain : ctx->patternElementChain()) {
    auto element =
        pattern_element_chain->accept(this).as<std::pair<Relationship, Node>>();
    pattern_part.relationships.push_back(element.first);
    pattern_part.nodes.push_back(element.second);
  }
  return pattern_part;
}

antlrcpp::Any CypherMainVisitor::visitPatternElementChain(
    CypherParser::PatternElementChainContext *ctx) {
  return std::pair<Relationship, Node>(
      ctx->relationshipPattern()->accept(this).as<Relationship>(),
      ctx->nodePattern()->accept(this).as<Node>());
}

antlrcpp::Any CypherMainVisitor::visitRelationshipPattern(
    CypherParser::RelationshipPatternContext *ctx) {
  Relationship relationship;
  relationship.output_identifier = new_identifier();
  if (ctx->relationshipDetail()) {
    VisitRelationshipDetail(ctx->relationshipDetail(), relationship);
  }
  if (ctx->leftArrowHead() && !ctx->rightArrowHead()) {
    relationship.direction = Relationship::Direction::LEFT;
  } else if (!ctx->leftArrowHead() && ctx->rightArrowHead()) {
    relationship.direction = Relationship::Direction::RIGHT;
  } else {
    // <-[]-> and -[]- is the same thing as far as we understand openCypher
    // grammar.
    relationship.direction = Relationship::Direction::BOTH;
  }
  symbol_table_[relationship.output_identifier] = relationship;
  return relationship;
}

antlrcpp::Any CypherMainVisitor::visitRelationshipDetail(
    CypherParser::RelationshipDetailContext *) {
  debug_assert(false, "Unimplemented.");
  return 0;
}

void CypherMainVisitor::VisitRelationshipDetail(
    CypherParser::RelationshipDetailContext *ctx, Relationship &relationship) {
  if (ctx->variable()) {
    identifiers_map_[ctx->variable()->accept(this).as<std::string>()] =
        relationship.output_identifier;
  }
  if (ctx->relationshipTypes()) {
    relationship.types =
        ctx->relationshipTypes()->accept(this).as<std::vector<std::string>>();
  }
  if (ctx->properties()) {
    relationship.properties =
        ctx->properties()
            ->accept(this)
            .as<std::unordered_map<std::string,
                                   CypherParser::ExpressionContext *>>();
  }
  if (ctx->rangeLiteral()) {
    relationship.has_range = true;
    auto range =
        ctx->rangeLiteral()->accept(this).as<std::pair<int64_t, int64_t>>();
    relationship.lower_bound = range.first;
    relationship.upper_bound = range.second;
  }
}

antlrcpp::Any CypherMainVisitor::visitRelationshipTypes(
    CypherParser::RelationshipTypesContext *ctx) {
  std::vector<std::string> types;
  for (auto *label : ctx->relTypeName()) {
    types.push_back(label->accept(this).as<std::string>());
  }
  return types;
}

antlrcpp::Any CypherMainVisitor::visitRangeLiteral(
    CypherParser::RangeLiteralContext *ctx) {
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

antlrcpp::Any CypherMainVisitor::visitIntegerLiteral(
    CypherParser::IntegerLiteralContext *ctx) {
  int64_t t = 0LL;
  try {
    t = std::stoll(ctx->getText(), 0, 0);
  } catch (std::out_of_range) {
    throw SemanticException();
  }
  return t;
}
