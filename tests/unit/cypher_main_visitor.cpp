#include <algorithm>
#include <climits>
#include <string>
#include <unordered_map>
#include <vector>

#include "antlr4-runtime.h"
#include "dbms/dbms.hpp"
#include "gmock/gmock.h"
#include "query/context.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "gtest/gtest.h"

namespace {

using namespace query;
using namespace query::frontend;
using testing::UnorderedElementsAre;

class AstGenerator {
public:
  AstGenerator(const std::string &query)
      : dbms_(), db_accessor_(dbms_.active()),
        context_(Config{}, *db_accessor_), query_string_(query), parser_(query),
        visitor_(context_), query_([&]() {
          visitor_.visit(parser_.tree());
          return visitor_.query();
        }()) {}

  Dbms dbms_;
  std::unique_ptr<GraphDbAccessor> db_accessor_;
  Context context_;
  std::string query_string_;
  ::frontend::opencypher::Parser parser_;
  CypherMainVisitor visitor_;
  Query *query_;
};

TEST(CompilerStructuresTest, SyntaxException) {
  ASSERT_THROW(AstGenerator("CREATE ()-[*1...2]-()"), std::exception);
}

TEST(CompilerStructuresTest, NodePattern) {
  AstGenerator ast_generator("MATCH (:label1:label2:label3)");
  auto *query = ast_generator.query_;
  ASSERT_EQ(query->clauses_.size(), 1U);
  auto *match = dynamic_cast<Match *>(query->clauses_[0]);
  ASSERT_TRUE(match);
  ASSERT_EQ(match->patterns_.size(), 1U);
  ASSERT_TRUE(match->patterns_[0]);
  ASSERT_EQ(match->patterns_[0]->atoms_.size(), 1U);
  auto node = dynamic_cast<NodeAtom *>(match->patterns_[0]->atoms_[0]);
  ASSERT_TRUE(node);
  ASSERT_TRUE(node->identifier_);
  ASSERT_EQ(node->identifier_->name_,
            CypherMainVisitor::kAnonPrefix + std::to_string(1));
  ASSERT_THAT(node->labels_, UnorderedElementsAre(
                                 ast_generator.db_accessor_->label("label1"),
                                 ast_generator.db_accessor_->label("label2"),
                                 ast_generator.db_accessor_->label("label3")));
  // TODO: add test for properties.
}

TEST(CompilerStructuresTest, NodePatternIdentifier) {
  AstGenerator ast_generator("MATCH (var)");
  auto *query = ast_generator.query_;
  auto *match = dynamic_cast<Match *>(query->clauses_[0]);
  auto node = dynamic_cast<NodeAtom *>(match->patterns_[0]->atoms_[0]);
  ASSERT_TRUE(node->identifier_);
  ASSERT_EQ(node->identifier_->name_, "var");
  ASSERT_THAT(node->labels_, UnorderedElementsAre());
  // TODO: add test for properties.
}

TEST(CompilerStructuresTest, RelationshipPatternNoDetails) {
  AstGenerator ast_generator("MATCH ()--()");
  auto *query = ast_generator.query_;
  auto *match = dynamic_cast<Match *>(query->clauses_[0]);
  ASSERT_EQ(match->patterns_.size(), 1U);
  ASSERT_TRUE(match->patterns_[0]);
  ASSERT_EQ(match->patterns_[0]->atoms_.size(), 3U);
  auto *node1 = dynamic_cast<NodeAtom *>(match->patterns_[0]->atoms_[0]);
  ASSERT_TRUE(node1);
  auto *edge = dynamic_cast<EdgeAtom *>(match->patterns_[0]->atoms_[1]);
  ASSERT_TRUE(edge);
  auto *node2 = dynamic_cast<NodeAtom *>(match->patterns_[0]->atoms_[2]);
  ASSERT_TRUE(node2);
  ASSERT_EQ(edge->direction_, EdgeAtom::Direction::BOTH);
  ASSERT_TRUE(edge->identifier_);
  ASSERT_THAT(edge->identifier_->name_,
              CypherMainVisitor::kAnonPrefix + std::to_string(2));
}

TEST(CompilerStructuresTest, RelationshipPatternDetails) {
  AstGenerator ast_generator("MATCH ()<-[:type1|type2]-()");
  auto *query = ast_generator.query_;
  auto *match = dynamic_cast<Match *>(query->clauses_[0]);
  auto *edge = dynamic_cast<EdgeAtom *>(match->patterns_[0]->atoms_[1]);
  ASSERT_EQ(edge->direction_, EdgeAtom::Direction::LEFT);
  ASSERT_THAT(
      edge->types_,
      UnorderedElementsAre(ast_generator.db_accessor_->edge_type("type1"),
                           ast_generator.db_accessor_->edge_type("type2")));
  // TODO: test properties
}

TEST(CompilerStructuresTest, RelationshipPatternVariable) {
  AstGenerator ast_generator("MATCH ()-[var]->()");
  auto *query = ast_generator.query_;
  auto *match = dynamic_cast<Match *>(query->clauses_[0]);
  auto *edge = dynamic_cast<EdgeAtom *>(match->patterns_[0]->atoms_[1]);
  ASSERT_EQ(edge->direction_, EdgeAtom::Direction::RIGHT);
  ASSERT_TRUE(edge->identifier_);
  ASSERT_THAT(edge->identifier_->name_, "var");
}

// // Relationship with unbounded variable range.
// TEST(CompilerStructuresTest, RelationshipPatternUnbounded) {
//   ParserTables parser("CREATE ()-[*]-()");
//   ASSERT_EQ(parser.identifiers_map_.size(), 0U);
//   ASSERT_EQ(parser.relationships_.size(), 1U);
//   CompareRelationships(*parser.relationships_.begin(),
//                        Relationship::Direction::BOTH, {}, {}, true, 1,
//                        LLONG_MAX);
// }
//
// // Relationship with lower bounded variable range.
// TEST(CompilerStructuresTest, RelationshipPatternLowerBounded) {
//   ParserTables parser("CREATE ()-[*5..]-()");
//   ASSERT_EQ(parser.identifiers_map_.size(), 0U);
//   ASSERT_EQ(parser.relationships_.size(), 1U);
//   CompareRelationships(*parser.relationships_.begin(),
//                        Relationship::Direction::BOTH, {}, {}, true, 5,
//                        LLONG_MAX);
// }
//
// // Relationship with upper bounded variable range.
// TEST(CompilerStructuresTest, RelationshipPatternUpperBounded) {
//   ParserTables parser("CREATE ()-[*..10]-()");
//   ASSERT_EQ(parser.identifiers_map_.size(), 0U);
//   ASSERT_EQ(parser.relationships_.size(), 1U);
//   CompareRelationships(*parser.relationships_.begin(),
//                        Relationship::Direction::BOTH, {}, {}, true, 1, 10);
// }
//
// // Relationship with lower and upper bounded variable range.
// TEST(CompilerStructuresTest, RelationshipPatternLowerUpperBounded) {
//   ParserTables parser("CREATE ()-[*5..10]-()");
//   ASSERT_EQ(parser.identifiers_map_.size(), 0U);
//   ASSERT_EQ(parser.relationships_.size(), 1U);
//   CompareRelationships(*parser.relationships_.begin(),
//                        Relationship::Direction::BOTH, {}, {}, true, 5, 10);
// }
//
// // Relationship with fixed number of edges.
// TEST(CompilerStructuresTest, RelationshipPatternFixedRange) {
//   ParserTables parser("CREATE ()-[*10]-()");
//   ASSERT_EQ(parser.identifiers_map_.size(), 0U);
//   ASSERT_EQ(parser.relationships_.size(), 1U);
//   CompareRelationships(*parser.relationships_.begin(),
//                        Relationship::Direction::BOTH, {}, {}, true, 10, 10);
// }
//
// // Relationship with invalid bound (larger than long long).
// TEST(CompilerStructuresTest, RelationshipPatternInvalidBound) {
//   ASSERT_THROW(
//       ParserTables parser("CREATE ()-[*100000000000000000000000000]-()"),
//       SemanticException);
// }
//
//
// // PatternPart.
// TEST(CompilerStructuresTest, PatternPart) {
//   ParserTables parser("CREATE ()--()");
//   ASSERT_EQ(parser.identifiers_map_.size(), 0U);
//   ASSERT_EQ(parser.pattern_parts_.size(), 1U);
//   ASSERT_EQ(parser.relationships_.size(), 1U);
//   ASSERT_EQ(parser.nodes_.size(), 2U);
//   ASSERT_EQ(parser.pattern_parts_.begin()->second.nodes.size(), 2U);
//   ASSERT_EQ(parser.pattern_parts_.begin()->second.relationships.size(), 1U);
// }
//
// // PatternPart in braces.
// TEST(CompilerStructuresTest, PatternPartBraces) {
//   ParserTables parser("CREATE ((()--()))");
//   ASSERT_EQ(parser.identifiers_map_.size(), 0U);
//   ASSERT_EQ(parser.pattern_parts_.size(), 1U);
//   ASSERT_EQ(parser.relationships_.size(), 1U);
//   ASSERT_EQ(parser.nodes_.size(), 2U);
//   ASSERT_EQ(parser.pattern_parts_.begin()->second.nodes.size(), 2U);
//   ASSERT_EQ(parser.pattern_parts_.begin()->second.relationships.size(), 1U);
// }
//
// // PatternPart with variable.
// TEST(CompilerStructuresTest, PatternPartVariable) {
//   ParserTables parser("CREATE var=()--()");
//   ASSERT_EQ(parser.identifiers_map_.size(), 1U);
//   ASSERT_EQ(parser.pattern_parts_.size(), 1U);
//   ASSERT_EQ(parser.relationships_.size(), 1U);
//   ASSERT_EQ(parser.nodes_.size(), 2U);
//   ASSERT_EQ(parser.pattern_parts_.begin()->second.nodes.size(), 2U);
//   ASSERT_EQ(parser.pattern_parts_.begin()->second.relationships.size(), 1U);
//   ASSERT_NE(parser.identifiers_map_.find("var"),
//   parser.identifiers_map_.end());
//   auto output_identifier = parser.identifiers_map_["var"];
//   ASSERT_NE(parser.pattern_parts_.find(output_identifier),
//             parser.pattern_parts_.end());
// }
//
// // Multiple nodes with same variable and properties.
// TEST(CompilerStructuresTest, MultipleNodesWithVariableAndProperties) {
//   ASSERT_THROW(ParserTables parser("CREATE (a {b: 5})-[]-(a {c: 5})"),
//                SemanticException);
// }
}
