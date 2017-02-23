#pragma once

#include <string>
#include "query/frontend/opencypher/generated/CypherBaseVisitor.h"
#include "antlr4-runtime.h"
#include "query/backend/cpp/compiler_structures.hpp"

using antlropencypher::CypherParser;

class CypherMainVisitor : public antlropencypher::CypherBaseVisitor {
  /**
   * Creates Node and stores it in symbol_table_. If variable is defined it is
   * stored in identifiers_map_.
   *
   * @return Node.
   */
  antlrcpp::Any visitNodePattern(
      CypherParser::NodePatternContext *ctx) override;

  /**
   * @return vector<string> labels.
   */
  antlrcpp::Any visitNodeLabels(CypherParser::NodeLabelsContext *ctx) override;

  /**
   * @return unordered_map<string, ExpressionContext*> properties.
   */
  antlrcpp::Any visitProperties(CypherParser::PropertiesContext *ctx) override;

  /**
   * @return unordered_map<string, ExpressionContext*> map.
   */
  antlrcpp::Any visitMapLiteral(CypherParser::MapLiteralContext *ctx) override;

  /**
   * @return string.
   */
  antlrcpp::Any visitSymbolicName(
      CypherParser::SymbolicNameContext *ctx) override;

  /**
   * @return vector<PatternPart> pattern.
   */
  antlrcpp::Any visitPattern(CypherParser::PatternContext *ctx) override;

  /**
   * Stores PatternPart in symbol_table_. If variable is defined it is stored in
   * identifiers_map_.
   *
   * @return PatternPart.
   */
  antlrcpp::Any visitPatternPart(
      CypherParser::PatternPartContext *ctx) override;

  /**
   * Creates PatternPart.
   *
   * @return PatternPart.
   */
  antlrcpp::Any visitPatternElement(
      CypherParser::PatternElementContext *ctx) override;

  /**
   * @return pair<Relationship, Node>
   */
  antlrcpp::Any visitPatternElementChain(
      CypherParser::PatternElementChainContext *ctx) override;

  /**
   * Creates Relationship and stores it in symbol_table_.
   *
   */
  antlrcpp::Any visitRelationshipPattern(
      CypherParser::RelationshipPatternContext *ctx) override;

  /**
   * This should never be called. Call VisitRelationshipDetail with already
   * created Relationship instead.
   */
  antlrcpp::Any visitRelationshipDetail(
      CypherParser::RelationshipDetailContext *ctx) override;

  /**
   * If variable is defined it is stored in symbol_table_. Relationship is
   * filled with properties, types and range if provided.
   * Use this instead of antlr generated visitRelationshipDetail with already
   * created Relationship. If we should have used visitRelationshipDetail
   * (relationshipDetail is optional production in relationshipPattern) then we
   * would have needed to return not completely initialised Relationship.
   */
  void VisitRelationshipDetail(CypherParser::RelationshipDetailContext *ctx,
                               Relationship &relationship);

  /**
   * @return vector<string>.
   */
  antlrcpp::Any visitRelationshipTypes(
      CypherParser::RelationshipTypesContext *ctx) override;

  /**
   * @return int64_t.
   */
  antlrcpp::Any visitIntegerLiteral(
      CypherParser::IntegerLiteralContext *ctx) override;

  /**
   * @return pair<int64_t, int64_t>.
   */
  antlrcpp::Any visitRangeLiteral(
      CypherParser::RangeLiteralContext *ctx) override;

 public:
  // TODO: These temporary getters should eventually be replaced with something
  // else once we figure out where and how those strctures will be used.
  // Currently there are needed for testing. cypher_main_visitor test should be
  // refactored once these getters are deleted.
  const std::unordered_map<std::string, std::string> &identifiers_map() const {
    return identifiers_map_;
  }
  const std::unordered_map<std::string, antlrcpp::Any> &symbol_table() const {
    return symbol_table_;
  }

 private:
  // Return new output code identifier.
  // TODO: Should we generate identifiers with more readable names: node_1,
  // relationship_5, ...?
  std::string new_identifier() const {
    static int next_identifier = 0;
    return "id" + std::to_string(next_identifier++);
  }

  // Mapping of identifiers (nodes, relationships, values, lists ...) from query
  // code to identifier that is used in generated code;
  std::unordered_map<std::string, std::string> identifiers_map_;

  // Mapping of output (generated) code identifiers to appropriate parser
  // structure.
  std::unordered_map<std::string, antlrcpp::Any> symbol_table_;
};
