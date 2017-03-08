#include <climits>
#include <string>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include "antlr4-runtime.h"
#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "query/backend/cpp/cypher_main_visitor.cpp"
#include "query/frontend/opencypher/parser.hpp"

using namespace ::testing;

namespace {

using namespace backend::cpp;

class ParserTables {
  template <typename T>
  auto FilterAnies(std::unordered_map<std::string, antlrcpp::Any> map) {
    std::unordered_map<std::string, T> filtered;
    for (auto x : map) {
      if (x.second.is<T>()) {
        filtered[x.first] = x.second.as<T>();
      }
    }
    return filtered;
  }

 public:
  ParserTables(const std::string &query) {
    frontend::opencypher::Parser parser(query);
    auto *tree = parser.tree();
    CypherMainVisitor visitor;
    visitor.visit(tree);
    identifiers_map_ = visitor.ids_map().back();
    symbol_table_ = visitor.symbol_table();
    pattern_parts_ = FilterAnies<PatternPart>(symbol_table_);
    nodes_ = FilterAnies<Node>(symbol_table_);
    relationships_ = FilterAnies<Relationship>(symbol_table_);
  }

  std::unordered_map<std::string, std::string> identifiers_map_;
  std::unordered_map<std::string, antlrcpp::Any> symbol_table_;
  std::unordered_map<std::string, PatternPart> pattern_parts_;
  std::unordered_map<std::string, Node> nodes_;
  std::unordered_map<std::string, Relationship> relationships_;
};

// TODO: Once expression evaluation is implemented, we should also test if
// property values are equal.
void CompareNodes(std::pair<std::string, Node> node_entry,
                  std::vector<std::string> labels,
                  std::vector<std::string> property_keys) {
  auto node = node_entry.second;
  ASSERT_EQ(node_entry.first, node.output_id);
  ASSERT_THAT(node.labels,
              UnorderedElementsAreArray(labels.begin(), labels.end()));
  std::vector<std::string> node_property_keys;
  for (auto x : node.properties) {
    node_property_keys.push_back(x.first);
  }
  ASSERT_THAT(
      node_property_keys,
      UnorderedElementsAreArray(property_keys.begin(), property_keys.end()));
}

// If has_range is false, lower and upper bound values are ignored.
// TODO: Once expression evaluation is implemented, we should also test if
// property values are equal.
void CompareRelationships(
    std::pair<std::string, Relationship> relationship_entry,
    Relationship::Direction direction, std::vector<std::string> types,
    std::vector<std::string> property_keys, bool has_range,
    int64_t lower_bound = 1LL, int64_t upper_bound = LLONG_MAX) {
  auto relationship = relationship_entry.second;
  ASSERT_EQ(relationship_entry.first, relationship.output_id);
  ASSERT_EQ(relationship.direction, direction);
  ASSERT_THAT(relationship.types,
              UnorderedElementsAreArray(types.begin(), types.end()));
  std::vector<std::string> relationship_property_keys;
  for (auto x : relationship.properties) {
    relationship_property_keys.push_back(x.first);
  }
  ASSERT_THAT(
      relationship_property_keys,
      UnorderedElementsAreArray(property_keys.begin(), property_keys.end()));
  ASSERT_EQ(relationship.has_range, has_range);
  if (!has_range) return;
  ASSERT_EQ(relationship.lower_bound, lower_bound);
  ASSERT_EQ(relationship.upper_bound, upper_bound);
}

// SyntaxException on incorrect syntax.
TEST(CompilerStructuresTest, SyntaxException) {
  ASSERT_THROW(ParserTables("CREATE ()-[*1...2]-()"),
               frontend::opencypher::SyntaxException);
}

// Empty node.
TEST(CompilerStructuresTest, NodePatternEmpty) {
  ParserTables parser("CREATE ()");
  ASSERT_EQ(parser.identifiers_map_.size(), 0U);
  ASSERT_EQ(parser.nodes_.size(), 1U);
  CompareNodes(*parser.nodes_.begin(), {}, {});
}

// Node with variable.
TEST(CompilerStructuresTest, NodePatternVariable) {
  ParserTables parser("CREATE (var)");
  ASSERT_EQ(parser.identifiers_map_.size(), 1U);
  ASSERT_NE(parser.identifiers_map_.find("var"), parser.identifiers_map_.end());
  ASSERT_EQ(parser.nodes_.size(), 1U);
  auto output_identifier = parser.identifiers_map_["var"];
  ASSERT_NE(parser.nodes_.find(output_identifier), parser.nodes_.end());
  CompareNodes(*parser.nodes_.begin(), {}, {});
}

// Node with labels.
TEST(CompilerStructuresTest, NodePatternLabels) {
  ParserTables parser("CREATE (:label1:label2:label3)");
  ASSERT_EQ(parser.identifiers_map_.size(), 0U);
  ASSERT_EQ(parser.nodes_.size(), 1U);
  CompareNodes(*parser.nodes_.begin(), {"label1", "label2", "label3"}, {});
}

// Node with properties.
TEST(CompilerStructuresTest, NodePatternProperties) {
  ParserTables parser("CREATE ({age: 5, name: \"John\", surname: \"Smith\"})");
  ASSERT_EQ(parser.identifiers_map_.size(), 0U);
  ASSERT_EQ(parser.nodes_.size(), 1U);
  CompareNodes(*parser.nodes_.begin(), {}, {"age", "name", "surname"});
}

// Relationship without relationship details.
TEST(CompilerStructuresTest, RelationshipPatternNoDetails) {
  ParserTables parser("CREATE ()--()");
  ASSERT_EQ(parser.identifiers_map_.size(), 0U);
  ASSERT_EQ(parser.relationships_.size(), 1U);
  CompareRelationships(*parser.relationships_.begin(),
                       Relationship::Direction::BOTH, {}, {}, false);
}

// Relationship with empty relationship details.
TEST(CompilerStructuresTest, RelationshipPatternEmptyDetails) {
  ParserTables parser("CREATE ()-[]-()");
  ASSERT_EQ(parser.identifiers_map_.size(), 0U);
  ASSERT_EQ(parser.relationships_.size(), 1U);
  CompareRelationships(*parser.relationships_.begin(),
                       Relationship::Direction::BOTH, {}, {}, false);
}

// Relationship with left direction.
TEST(CompilerStructuresTest, RelationshipPatternLeftDirection) {
  ParserTables parser("CREATE ()<--()");
  ASSERT_EQ(parser.identifiers_map_.size(), 0U);
  ASSERT_EQ(parser.relationships_.size(), 1U);
  CompareRelationships(*parser.relationships_.begin(),
                       Relationship::Direction::LEFT, {}, {}, false);
}

// Relationship with right direction.
TEST(CompilerStructuresTest, RelationshipPatternRightDirection) {
  ParserTables parser("CREATE ()-[]->()");
  ASSERT_EQ(parser.identifiers_map_.size(), 0U);
  ASSERT_EQ(parser.relationships_.size(), 1U);
  CompareRelationships(*parser.relationships_.begin(),
                       Relationship::Direction::RIGHT, {}, {}, false);
}

// Relationship with both directions.
TEST(CompilerStructuresTest, RelationshipPatternBothDirection) {
  ParserTables parser("CREATE ()<-[]->()");
  ASSERT_EQ(parser.identifiers_map_.size(), 0U);
  ASSERT_EQ(parser.relationships_.size(), 1U);
  CompareRelationships(*parser.relationships_.begin(),
                       Relationship::Direction::BOTH, {}, {}, false);
}

// Relationship with unbounded variable range.
TEST(CompilerStructuresTest, RelationshipPatternUnbounded) {
  ParserTables parser("CREATE ()-[*]-()");
  ASSERT_EQ(parser.identifiers_map_.size(), 0U);
  ASSERT_EQ(parser.relationships_.size(), 1U);
  CompareRelationships(*parser.relationships_.begin(),
                       Relationship::Direction::BOTH, {}, {}, true, 1,
                       LLONG_MAX);
}

// Relationship with lower bounded variable range.
TEST(CompilerStructuresTest, RelationshipPatternLowerBounded) {
  ParserTables parser("CREATE ()-[*5..]-()");
  ASSERT_EQ(parser.identifiers_map_.size(), 0U);
  ASSERT_EQ(parser.relationships_.size(), 1U);
  CompareRelationships(*parser.relationships_.begin(),
                       Relationship::Direction::BOTH, {}, {}, true, 5,
                       LLONG_MAX);
}

// Relationship with upper bounded variable range.
TEST(CompilerStructuresTest, RelationshipPatternUpperBounded) {
  ParserTables parser("CREATE ()-[*..10]-()");
  ASSERT_EQ(parser.identifiers_map_.size(), 0U);
  ASSERT_EQ(parser.relationships_.size(), 1U);
  CompareRelationships(*parser.relationships_.begin(),
                       Relationship::Direction::BOTH, {}, {}, true, 1, 10);
}

// Relationship with lower and upper bounded variable range.
TEST(CompilerStructuresTest, RelationshipPatternLowerUpperBounded) {
  ParserTables parser("CREATE ()-[*5..10]-()");
  ASSERT_EQ(parser.identifiers_map_.size(), 0U);
  ASSERT_EQ(parser.relationships_.size(), 1U);
  CompareRelationships(*parser.relationships_.begin(),
                       Relationship::Direction::BOTH, {}, {}, true, 5, 10);
}

// Relationship with fixed number of edges.
TEST(CompilerStructuresTest, RelationshipPatternFixedRange) {
  ParserTables parser("CREATE ()-[*10]-()");
  ASSERT_EQ(parser.identifiers_map_.size(), 0U);
  ASSERT_EQ(parser.relationships_.size(), 1U);
  CompareRelationships(*parser.relationships_.begin(),
                       Relationship::Direction::BOTH, {}, {}, true, 10, 10);
}

// Relationship with invalid bound (larger than long long).
TEST(CompilerStructuresTest, RelationshipPatternInvalidBound) {
  ASSERT_THROW(
      ParserTables parser("CREATE ()-[*100000000000000000000000000]-()"),
      SemanticException);
}

// Relationship with variable
TEST(CompilerStructuresTest, RelationshipPatternVariable) {
  ParserTables parser("CREATE ()-[var]-()");
  ASSERT_EQ(parser.identifiers_map_.size(), 1U);
  ASSERT_NE(parser.identifiers_map_.find("var"), parser.identifiers_map_.end());
  ASSERT_EQ(parser.relationships_.size(), 1U);
  auto output_identifier = parser.identifiers_map_["var"];
  ASSERT_NE(parser.relationships_.find(output_identifier),
            parser.relationships_.end());
  CompareRelationships(*parser.relationships_.begin(),
                       Relationship::Direction::BOTH, {}, {}, false);
}

// Relationship with labels.
TEST(CompilerStructuresTest, RelationshipPatternLabels) {
  ParserTables parser("CREATE ()-[:label1|label2|:label3]-()");
  ASSERT_EQ(parser.identifiers_map_.size(), 0U);
  ASSERT_EQ(parser.relationships_.size(), 1U);
  CompareRelationships(*parser.relationships_.begin(),
                       Relationship::Direction::BOTH,
                       {"label1", "label2", "label3"}, {}, false);
}

// Relationship with properties.
TEST(CompilerStructuresTest, RelationshipPatternProperties) {
  ParserTables parser(
      "CREATE ()-[{age: 5, name: \"John\", surname: \"Smith\"}]-()");
  ASSERT_EQ(parser.identifiers_map_.size(), 0U);
  ASSERT_EQ(parser.relationships_.size(), 1U);
  CompareRelationships(*parser.relationships_.begin(),
                       Relationship::Direction::BOTH, {},
                       {"age", "name", "surname"}, false);
}

// PatternPart.
TEST(CompilerStructuresTest, PatternPart) {
  ParserTables parser("CREATE ()--()");
  ASSERT_EQ(parser.identifiers_map_.size(), 0U);
  ASSERT_EQ(parser.pattern_parts_.size(), 1U);
  ASSERT_EQ(parser.relationships_.size(), 1U);
  ASSERT_EQ(parser.nodes_.size(), 2U);
  ASSERT_EQ(parser.pattern_parts_.begin()->second.nodes.size(), 2U);
  ASSERT_EQ(parser.pattern_parts_.begin()->second.relationships.size(), 1U);
}

// PatternPart in braces.
TEST(CompilerStructuresTest, PatternPartBraces) {
  ParserTables parser("CREATE ((()--()))");
  ASSERT_EQ(parser.identifiers_map_.size(), 0U);
  ASSERT_EQ(parser.pattern_parts_.size(), 1U);
  ASSERT_EQ(parser.relationships_.size(), 1U);
  ASSERT_EQ(parser.nodes_.size(), 2U);
  ASSERT_EQ(parser.pattern_parts_.begin()->second.nodes.size(), 2U);
  ASSERT_EQ(parser.pattern_parts_.begin()->second.relationships.size(), 1U);
}

// PatternPart with variable.
TEST(CompilerStructuresTest, PatternPartVariable) {
  ParserTables parser("CREATE var=()--()");
  ASSERT_EQ(parser.identifiers_map_.size(), 1U);
  ASSERT_EQ(parser.pattern_parts_.size(), 1U);
  ASSERT_EQ(parser.relationships_.size(), 1U);
  ASSERT_EQ(parser.nodes_.size(), 2U);
  ASSERT_EQ(parser.pattern_parts_.begin()->second.nodes.size(), 2U);
  ASSERT_EQ(parser.pattern_parts_.begin()->second.relationships.size(), 1U);
  ASSERT_NE(parser.identifiers_map_.find("var"), parser.identifiers_map_.end());
  auto output_identifier = parser.identifiers_map_["var"];
  ASSERT_NE(parser.pattern_parts_.find(output_identifier),
            parser.pattern_parts_.end());
}

// Multiple nodes with same variable and properties.
TEST(CompilerStructuresTest, MultipleNodesWithVariableAndProperties) {
  ASSERT_THROW(ParserTables parser("CREATE (a {b: 5})-[]-(a {c: 5})"),
               SemanticException);
}

// Multiple nodes with same variable name.
TEST(CompilerStructuresTest, MultipleNodesWithVariable) {
  ParserTables parser("CREATE (a {b: 5, c: 5})-[]-(a)");
  ASSERT_EQ(parser.identifiers_map_.size(), 1U);
  ASSERT_EQ(parser.pattern_parts_.size(), 1U);
  ASSERT_EQ(parser.relationships_.size(), 1U);
  ASSERT_EQ(parser.nodes_.size(), 1U);
  auto pattern_part = parser.pattern_parts_.begin()->second;
  ASSERT_EQ(pattern_part.nodes.size(), 2U);
  ASSERT_EQ(pattern_part.relationships.size(), 1U);
  ASSERT_EQ(pattern_part.nodes[0], pattern_part.nodes[1]);
}

// Multiple relationships with same variable name and properties.
TEST(CompilerStructuresTest, MultipleRelationshipsWithVariableAndProperties) {
  ASSERT_THROW(ParserTables parser("CREATE ()-[e {a: 5}]-()-[e {c: 5}]-()"),
               SemanticException);
}

// Multiple relationships with same variable name.
TEST(CompilerStructuresTest, MultipleRelationshipsWithVariable) {
  ParserTables parser("CREATE ()-[a {a: 5}]-()-[a]-()");
  ASSERT_EQ(parser.identifiers_map_.size(), 1U);
  ASSERT_EQ(parser.pattern_parts_.size(), 1U);
  ASSERT_EQ(parser.relationships_.size(), 1U);
  ASSERT_EQ(parser.nodes_.size(), 3U);
  auto pattern_part = parser.pattern_parts_.begin()->second;
  ASSERT_EQ(pattern_part.nodes.size(), 3U);
  ASSERT_EQ(pattern_part.relationships.size(), 2U);
  ASSERT_NE(pattern_part.nodes[0], pattern_part.nodes[1]);
  ASSERT_NE(pattern_part.nodes[1], pattern_part.nodes[2]);
  ASSERT_NE(pattern_part.nodes[0], pattern_part.nodes[2]);
  ASSERT_EQ(pattern_part.relationships[0], pattern_part.relationships[1]);
}

// Different structures (nodes, realtionships, patterns) with same variable
// name.
TEST(CompilerStructuresTest, DifferentTypesWithVariable) {
  ASSERT_THROW(ParserTables parser("CREATE a=(a)"), SemanticException);
  ASSERT_THROW(ParserTables parser("CREATE (a)-[a]-()"), SemanticException);
  ASSERT_THROW(ParserTables parser("CREATE a=()-[a]-()"), SemanticException);
}
}

int main(int argc, char **argv) {
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
